package qa.autotest.framework.infrastructure.kafka.adapter;

import lombok.extern.slf4j.Slf4j;
import org.apache.kafka.clients.consumer.*;
import org.apache.kafka.common.TopicPartition;
import org.apache.kafka.common.header.Header;
import org.apache.kafka.common.serialization.StringDeserializer;
import qa.autotest.framework.config.KafkaConfig;
import qa.autotest.framework.domain.model.ConsumeResult;
import qa.autotest.framework.domain.model.Message;
import qa.autotest.framework.domain.model.Topic;
import qa.autotest.framework.domain.port.MessageConsumer;
import qa.autotest.framework.infrastructure.KafkaPropertiesBuilder;

import java.lang.ref.WeakReference;
import java.nio.charset.StandardCharsets;
import java.time.Duration;
import java.time.Instant;
import java.util.*;
import java.util.stream.Collectors;

/**
 * Infrastructure Adapter: KafkaConsumerAdapter
 * <p>
 * Implements the MessageConsumer port using Apache Kafka.
 * Translates domain operations to Kafka-specific calls.
 */
@Slf4j
public class KafkaConsumerAdapter implements MessageConsumer {

    /**
     * Number of consecutive empty polls required to conclude that no more
     * messages are available on the broker.
     * <p>
     * A single empty {@code poll()} is NOT a reliable end-of-stream signal:
     * the broker may temporarily return nothing due to batch-assembly delay
     * (controlled by {@code fetch.max.wait.ms}), transient network jitter, or
     * a partition rebalance that is still in progress mid-poll.  Waiting for
     * {@value} empty polls in a row gives the broker enough time to recover
     * from any of those conditions before we give up.
     * <p>
     * Value reasoning: with the default {@code fetch.max.wait.ms=500} and a
     * poll timeout cap of 1 000 ms per iteration, three consecutive misses
     * represent at minimum ~1.5 s of broker silence — sufficient to distinguish
     * "temporarily busy" from "genuinely empty" under normal load.  Increase
     * this constant in environments with high GC pause or very slow brokers.
     */
    private static final int CONSECUTIVE_EMPTY_POLLS_THRESHOLD = 3;

    private final KafkaConfig config;
    private final String groupId;
    private final ThreadLocal<KafkaConsumer<String, String>> consumerThreadLocal;

    /**
     * Track all created consumers across all threads
     */
    private final Set<WeakReference<KafkaConsumer<String, String>>> allConsumers =
            Collections.synchronizedSet(new HashSet<>());

    public KafkaConsumerAdapter(KafkaConfig config, String groupId) {
        this.config = config;
        this.groupId = groupId;
        this.consumerThreadLocal = ThreadLocal.withInitial(this::createConsumer);
        log.debug("KafkaConsumerAdapter initialized: groupId={}", groupId);
    }

    /**
     * Creates a new Kafka consumer
     */
    private KafkaConsumer<String, String> createConsumer() {
        log.debug("Creating Kafka consumer: groupId={}, thread={}",
                groupId, Thread.currentThread().getName());

        Properties props = KafkaPropertiesBuilder.buildBaseProperties(config);
        props.put(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class.getName());
        props.put(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class.getName());
        props.put(ConsumerConfig.GROUP_ID_CONFIG, groupId);
        props.put(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG, config.consumerAutoOffsetReset());
        props.put(ConsumerConfig.ENABLE_AUTO_COMMIT_CONFIG, config.consumerEnableAutoCommit());
        props.put(ConsumerConfig.MAX_POLL_RECORDS_CONFIG, config.consumerMaxPollRecords());
        props.put(ConsumerConfig.SESSION_TIMEOUT_MS_CONFIG, config.consumerSessionTimeoutMs());
        props.put(ConsumerConfig.HEARTBEAT_INTERVAL_MS_CONFIG, 3000);

        KafkaPropertiesBuilder.configureSecurity(props, config);

        KafkaConsumer<String, String> consumer = new KafkaConsumer<>(props);

        // Register for tracking
        allConsumers.add(new WeakReference<>(consumer));
        log.debug("Registered consumer for tracking. Total tracked: {}", getTrackedConsumerCount());

        return consumer;
    }

    /**
     * Gets the consumer for the current thread
     */
    private KafkaConsumer<String, String> getConsumer() {
        return consumerThreadLocal.get();
    }

    @Override
    public void subscribe(Set<Topic> topics) {
        List<String> topicNames = topics.stream()
                .map(Topic::getName)
                .collect(Collectors.toList());

        log.debug("Subscribing to topics: {}", topicNames);
        getConsumer().subscribe(topicNames);
    }

    @Override
    public void subscribe(Topic topic) {
        subscribe(Set.of(topic));
    }

    @Override
    public ConsumeResult poll(Duration timeout) {
        try {
            ConsumerRecords<String, String> records = getConsumer().poll(timeout);

            if (records.isEmpty()) {
                return ConsumeResult.empty();
            }

            List<Message> messages = new ArrayList<>();
            for (ConsumerRecord<String, String> record : records) {
                messages.add(toDomainMessage(record));
            }

            log.debug("Polled {} messages", messages.size());
            return ConsumeResult.success(messages);

        } catch (Exception e) {
            log.error("Failed to poll messages: {}", e.getMessage(), e);
            return ConsumeResult.failureFrom(e.getMessage(), e);
        }
    }

    @Override
    public ConsumeResult pollMessages(int expectedCount, Duration timeout) {
        long endTime = System.currentTimeMillis() + timeout.toMillis();
        List<Message> allMessages = new ArrayList<>();

        try {
            while (allMessages.size() < expectedCount && System.currentTimeMillis() < endTime) {
                long remainingTime = endTime - System.currentTimeMillis();
                if (remainingTime <= 0) {
                    break;
                }

                ConsumerRecords<String, String> records = getConsumer().poll(Duration.ofMillis(remainingTime));

                for (ConsumerRecord<String, String> record : records) {
                    allMessages.add(toDomainMessage(record));

                    if (allMessages.size() >= expectedCount) {
                        break;
                    }
                }
            }

            log.debug("Polled {}/{} expected messages", allMessages.size(), expectedCount);
            return ConsumeResult.success(allMessages);

        } catch (Exception e) {
            log.error("Failed to poll expected messages: {}", e.getMessage(), e);
            return ConsumeResult.failureFrom(e.getMessage(), e);
        }
    }

    /**
     * Consumes up to {@code maxMessages} messages within {@code timeout}, stopping
     * early only after {@link #CONSECUTIVE_EMPTY_POLLS_THRESHOLD} consecutive empty
     * poll responses — not on the first empty poll.
     *
     * <h3>Why not break on the first empty poll?</h3>
     * A single empty {@code poll()} is NOT a reliable end-of-stream signal.
     * The broker may return nothing temporarily due to:
     * <ul>
     *   <li><b>Batch assembly delay</b> — {@code fetch.max.wait.ms} (default 500 ms)
     *       controls how long the broker waits to fill a batch before responding.
     *       If the batch isn't full yet, the response arrives slightly after the
     *       poll timeout cap, making the next poll appear empty.</li>
     *   <li><b>Network jitter</b> — a brief TCP stall can cause a poll to return
     *       before the broker response arrives.</li>
     *   <li><b>Rebalance mid-poll</b> — a partition reassignment during the poll
     *       causes the broker to return an empty response while the new assignment
     *       is being negotiated.</li>
     * </ul>
     * Breaking on the first empty poll under any of these conditions causes
     * {@code consumeAll(100, 30s)} to return 5 messages instead of 100 — a
     * source of flaky tests that is extremely difficult to reproduce locally.
     *
     * <h3>Termination conditions (in priority order)</h3>
     * <ol>
     *   <li>{@code allMessages.size() >= maxMessages} — collected enough, stop.</li>
     *   <li>Wall-clock deadline exceeded — timeout, stop.</li>
     *   <li>{@link #CONSECUTIVE_EMPTY_POLLS_THRESHOLD} empty polls in a row —
     *       broker is genuinely empty, stop.</li>
     * </ol>
     * Any non-empty poll resets the consecutive counter to 0.
     *
     * @param maxMessages upper bound on collected messages
     * @param timeout     wall-clock deadline for the entire operation
     * @return {@link ConsumeResult} containing all collected messages on success,
     *         or a failure result if an unrecoverable exception is thrown
     */
    @Override
    public ConsumeResult consumeAll(int maxMessages, Duration timeout) {
        long endTime = System.currentTimeMillis() + timeout.toMillis();
        List<Message> allMessages = new ArrayList<>();
        int consecutiveEmptyPolls = 0;

        try {
            while (allMessages.size() < maxMessages && System.currentTimeMillis() < endTime) {
                long remainingTime = endTime - System.currentTimeMillis();
                if (remainingTime <= 0) {
                    break;
                }

                ConsumerRecords<String, String> records =
                        getConsumer().poll(Duration.ofMillis(Math.min(remainingTime, 1000)));

                if (records.isEmpty()) {
                    consecutiveEmptyPolls++;
                    log.trace("Empty poll #{} (consecutive), collected so far: {}",
                            consecutiveEmptyPolls, allMessages.size());

                    if (consecutiveEmptyPolls >= CONSECUTIVE_EMPTY_POLLS_THRESHOLD) {
                        log.debug("Stopping consumeAll after {} consecutive empty polls — "
                                        + "broker has no more messages. Collected: {}/{}",
                                consecutiveEmptyPolls, allMessages.size(), maxMessages);
                        break;
                    }
                    continue;  // try again — do NOT break on the first empty poll
                }

                // Non-empty poll: reset the consecutive counter
                consecutiveEmptyPolls = 0;

                for (ConsumerRecord<String, String> record : records) {
                    allMessages.add(toDomainMessage(record));

                    if (allMessages.size() >= maxMessages) {
                        break;
                    }
                }
            }

            log.debug("consumeAll complete: collected={}, max={}, consecutiveEmptyAtEnd={}",
                    allMessages.size(), maxMessages, consecutiveEmptyPolls);
            return ConsumeResult.success(allMessages);

        } catch (Exception e) {
            if (Thread.interrupted()) {
                Thread.currentThread().interrupt();
            }
            log.error("Failed to consume all messages: {}", e.getMessage(), e);
            return ConsumeResult.failureFrom(e.getMessage(), e);
        }
    }

    @Override
    public void seek(Topic topic, int partition, long offset) {
        TopicPartition tp = new TopicPartition(topic.getName(), partition);
        getConsumer().seek(tp, offset);
        log.debug("Seeked to offset: topic={}, partition={}, offset={}",
                topic.getName(), partition, offset);
    }

    @Override
    public void seekToBeginning() {
        Set<TopicPartition> assignment = getConsumer().assignment();
        if (!assignment.isEmpty()) {
            getConsumer().seekToBeginning(assignment);
            log.debug("Seeked to beginning of {} partitions", assignment.size());
        }
    }

    @Override
    public void seekToEnd() {
        Set<TopicPartition> assignment = getConsumer().assignment();
        if (!assignment.isEmpty()) {
            getConsumer().seekToEnd(assignment);
            log.debug("Seeked to end of {} partitions", assignment.size());
        }
    }

    /**
     * Returns {@code true} if this consumer currently has at least one
     * partition assigned.
     * <p>
     * Reads {@code KafkaConsumer.assignment()} — a local, in-memory set,
     * no network round-trip.  Safe to call on the main test thread inside
     * an Awaitility {@code pollInSameThread()} loop triggered immediately
     * after {@code subscribe()}.  The set becomes non-empty once the
     * group coordinator completes the rebalance and the next {@code poll()}
     * drives the JoinGroup/SyncGroup protocol to completion.
     *
     * @return {@code true} when at least one {@link TopicPartition} is assigned
     */
    @Override
    public boolean isAssigned() {
        boolean assigned = !getConsumer().assignment().isEmpty();
        log.trace("isAssigned={}, partitions={}", assigned, getConsumer().assignment());
        return assigned;
    }

    @Override
    public void commitSync() {
        getConsumer().commitSync();
        log.debug("Committed offsets synchronously");
    }

    /**
     * Commits a specific offset for a single partition using
     * {@code KafkaConsumer.commitSync(Map)} — the correct Kafka API for
     * explicit offset commits without a preceding {@code poll()}.
     * <p>
     * The committed position is {@code offset + 1}: Kafka interprets the
     * committed offset as "the next record to fetch", so we must always
     * store lastConsumedOffset + 1.
     *
     * <h3>Why not seek() + commitSync()?</h3>
     * {@code seek()} repositions the in-memory fetch position only.
     * {@code commitSync()} (no-arg) then persists <em>that</em> fetch
     * position, which can silently overwrite legitimate progress on other
     * partitions assigned to the same consumer.  Using the {@code Map}
     * overload targets exactly one {@code TopicPartition} and leaves all
     * other committed offsets untouched.
     *
     * @param topic     topic the offset belongs to
     * @param partition partition number (0-based)
     * @param offset    offset of the last consumed record;
     *                  committed position will be {@code offset + 1}
     */
    @Override
    public void commitSync(Topic topic, int partition, long offset) {
        TopicPartition tp = new TopicPartition(topic.getName(), partition);
        // committed offset = lastConsumed + 1 (Kafka convention)
        OffsetAndMetadata meta = new OffsetAndMetadata(offset + 1);
        Map<TopicPartition, OffsetAndMetadata> offsets = Map.of(tp, meta);
        getConsumer().commitSync(offsets);
        log.debug("Committed explicit offset: topic={}, partition={}, committedPosition={}",
                topic.getName(), partition, offset + 1);
    }

    @Override
    public void commitAsync() {
        getConsumer().commitAsync();
        log.debug("Committed offsets asynchronously");
    }

    @Override
    public void close() {
        log.debug("Closing current thread consumer");
        KafkaConsumer<String, String> consumer = consumerThreadLocal.get();
        if (consumer != null) {
            consumer.close(Duration.ofSeconds(5));
        }
        consumerThreadLocal.remove();
    }

    /**
     * CRITICAL: Closes ALL consumers from ALL threads
     */
    public void closeAll() {
        log.info("Closing ALL consumers from ALL threads. Total tracked: {}", getTrackedConsumerCount());

        int closed = 0;
        int failed = 0;

        synchronized (allConsumers) {
            for (WeakReference<KafkaConsumer<String, String>> ref : allConsumers) {
                KafkaConsumer<String, String> consumer = ref.get();
                if (consumer != null) {
                    try {
                        consumer.close(Duration.ofSeconds(5));
                        closed++;
                    } catch (Exception e) {
                        failed++;
                        log.error("Failed to close consumer: {}", e.getMessage());
                    }
                }
            }
            allConsumers.clear();
        }

        consumerThreadLocal.remove();

        log.info("Consumer cleanup complete. Closed: {}, Failed: {}", closed, failed);
    }

    /**
     * Gets the number of tracked consumers
     */
    public int getTrackedConsumerCount() {
        return (int) allConsumers.stream()
                .map(WeakReference::get)
                .filter(Objects::nonNull)
                .count();
    }

    /**
     * Converts Kafka ConsumerRecord to domain Message
     */
    private Message toDomainMessage(ConsumerRecord<String, String> record) {
        // Extract headers
        Map<String, String> headers = new HashMap<>();
        String correlationId = null;
        String eventType = null;
        String messageId = null;

        for (Header header : record.headers()) {
            String key = header.key();
            String value = new String(header.value(), StandardCharsets.UTF_8);

            if ("correlation-id".equals(key)) {
                correlationId = value;
            } else if ("event-type".equals(key)) {
                eventType = value;
            } else if ("message-id".equals(key)) {
                messageId = value;
            } else {
                headers.put(key, value);
            }
        }

        Topic topic = Topic.builder()
                .name(record.topic())
                .partitionCount(1) // We don't have full topic info here
                .build();

        return Message.builder()
                .messageId(messageId != null ? messageId : UUID.randomUUID().toString())
                .key(record.key())
                .content(record.value())
                .topic(topic)
                .partition(record.partition())
                .headers(headers)
                .timestamp(Instant.ofEpochMilli(record.timestamp()))
                .correlationId(correlationId)
                .eventType(eventType)
                .build();
    }
}
