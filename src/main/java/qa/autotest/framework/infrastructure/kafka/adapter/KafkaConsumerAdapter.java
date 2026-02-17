package qa.autotest.framework.infrastructure.kafka.adapter;

import lombok.extern.slf4j.Slf4j;
import org.apache.kafka.clients.consumer.*;
import org.apache.kafka.common.TopicPartition;
import org.apache.kafka.common.header.Header;
import org.apache.kafka.common.serialization.StringDeserializer;
import qa.autotest.framework.config.KafkaConfig;
import qa.autotest.framework.domain.model.ConsumeResult;
import qa.autotest.framework.domain.model.ConsumerGroup;
import qa.autotest.framework.domain.model.KafkaErrorCategory;
import qa.autotest.framework.domain.model.Message;
import qa.autotest.framework.domain.model.Topic;
import qa.autotest.framework.domain.port.MessageConsumer;
import qa.autotest.framework.kafka.KafkaPropertiesBuilder;

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

    @Override
    public ConsumeResult consumeAll(int maxMessages, Duration timeout) {
        long endTime = System.currentTimeMillis() + timeout.toMillis();
        List<Message> allMessages = new ArrayList<>();

        try {
            while (allMessages.size() < maxMessages && System.currentTimeMillis() < endTime) {
                long remainingTime = endTime - System.currentTimeMillis();
                if (remainingTime <= 0) {
                    break;
                }

                ConsumerRecords<String, String> records = getConsumer().poll(Duration.ofMillis(Math.min(remainingTime, 1000)));

                if (records.isEmpty()) {
                    break; // No more messages available
                }

                for (ConsumerRecord<String, String> record : records) {
                    allMessages.add(toDomainMessage(record));

                    if (allMessages.size() >= maxMessages) {
                        break;
                    }
                }
            }

            log.debug("Consumed {} messages (max: {})", allMessages.size(), maxMessages);
            return ConsumeResult.success(allMessages);

        } catch (Exception e) {
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

    @Override
    public void commitSync() {
        getConsumer().commitSync();
        log.debug("Committed offsets synchronously");
    }

    @Override
    public void commitAsync() {
        getConsumer().commitAsync();
        log.debug("Committed offsets asynchronously");
    }

    @Override
    public ConsumerGroup getConsumerGroup() {
        return ConsumerGroup.builder()
                .groupId(groupId)
                .state(ConsumerGroup.GroupState.STABLE)
                .memberCount(1)
                .build();
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
