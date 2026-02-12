package qa.autotest.framework.kafka;

import io.qameta.allure.Step;
import lombok.extern.slf4j.Slf4j;
import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.clients.consumer.OffsetAndMetadata;
import org.apache.kafka.common.TopicPartition;
import org.apache.kafka.common.header.Header;
import org.apache.kafka.common.serialization.StringDeserializer;
import qa.autotest.app.dto.ConsumerRecordDto;
import qa.autotest.framework.config.KafkaConfig;

import java.nio.charset.StandardCharsets;
import java.time.Duration;
import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Properties;
import java.util.UUID;

/**
 * Thread-safe Kafka Consumer Manager
 * Manages Kafka consumer instances and message consumption operations
 *
 * <p><b>Thread Safety:</b> This class uses ThreadLocal to ensure each thread
 * gets its own KafkaConsumer instance. Consumers are lazily initialized when
 * {@link #initConsumer(String)} is called for the first time on each thread.</p>
 *
 * <p><b>Usage Pattern:</b>
 * <pre>
 * ConsumerManager manager = new ConsumerManager(config);
 * manager.initConsumer("my-topic");        // Must be called first
 * List&lt;Record&gt; records = manager.poll(5); // Then use
 * manager.close();                          // Finally cleanup
 * </pre>
 * </p>
 */
@Slf4j
public class KafkaConsumerManager implements AutoCloseable {

    private final KafkaConfig config;

    /**
     * Thread-local consumer instance. Each thread gets its own consumer.
     * Initialized lazily via {@link #initConsumer(String)}.
     *
     * <p><b>WARNING:</b> Calling {@link #getConsumer()} before initialization
     * will throw {@link IllegalStateException}.</p>
     */
    private final ThreadLocal<KafkaConsumer<String, String>> consumerThreadLocal;

    /**
     * Thread-local group ID. Tracks which consumer group ID is used by
     * the consumer on this thread.
     */
    private final ThreadLocal<String> groupIdThreadLocal;

    public KafkaConsumerManager(KafkaConfig config) {
        this.config = config;
        // ThreadLocal initialized without initial value - lazy initialization pattern
        // Values are set explicitly in initConsumer() to ensure proper setup
        this.consumerThreadLocal = new ThreadLocal<>();
        this.groupIdThreadLocal = new ThreadLocal<>();
    }

    /**
     * Creates a new Kafka consumer with unique group ID
     */
    private KafkaConsumer<String, String> createConsumer(String groupId) {
        log.debug("Creating Kafka consumer on thread: {} with group ID: {}",
                Thread.currentThread().getName(), groupId);

        Properties props = new Properties();
        props.put(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, config.kafkaBootstrapServers());
        props.put(ConsumerConfig.GROUP_ID_CONFIG, groupId);
        props.put(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class.getName());
        props.put(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class.getName());
        props.put(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG, config.consumerAutoOffsetReset());
        props.put(ConsumerConfig.ENABLE_AUTO_COMMIT_CONFIG, config.consumerEnableAutoCommit());
        props.put(ConsumerConfig.AUTO_COMMIT_INTERVAL_MS_CONFIG, config.consumerAutoCommitIntervalMs());
        props.put(ConsumerConfig.SESSION_TIMEOUT_MS_CONFIG, config.consumerSessionTimeoutMs());
        props.put(ConsumerConfig.MAX_POLL_INTERVAL_MS_CONFIG, config.consumerMaxPollIntervalMs());
        props.put(ConsumerConfig.MAX_POLL_RECORDS_CONFIG, config.consumerMaxPollRecords());
        props.put(ConsumerConfig.FETCH_MIN_BYTES_CONFIG, config.consumerFetchMinBytes());
        props.put(ConsumerConfig.FETCH_MAX_WAIT_MS_CONFIG, config.consumerFetchMaxWaitMs());

        // SSL configuration
        if ("SSL".equals(config.securityProtocol()) || "SASL_SSL".equals(config.securityProtocol())) {
            props.put("security.protocol", config.securityProtocol());
            props.put("ssl.truststore.location", config.sslTruststoreLocation());
            props.put("ssl.truststore.password", config.sslTruststorePassword());
            props.put("ssl.truststore.type", config.sslTruststoreType());
            props.put("ssl.keystore.location", config.sslKeystoreLocation());
            props.put("ssl.keystore.password", config.sslKeyPassword());
            props.put("ssl.keystore.type", config.sslKeystoreType());

            if (config.sslKeyPassword() != null) {
                props.put("ssl.key.password", config.sslKeyPassword());
            }
        }

        return new KafkaConsumer<>(props);
    }

    /**
     * Initializes consumer for the current thread with unique group ID
     *
     * <p><b>IMPORTANT:</b> This method MUST be called before any other consumer
     * operations (poll, commit, etc.) on each thread.</p>
     *
     * @param topic Topic to subscribe to
     * @return Unique group ID assigned to this consumer
     * @throws IllegalStateException if consumer is already initialized on this thread
     */
    @Step("Initialize consumer for topic: {topic}")
    public String initConsumer(String topic) {
        String groupId = config.consumerGroupIdBase() + "-" + UUID.randomUUID();
        return initConsumer(topic, groupId);
    }

    /**
     * Initializes consumer for the current thread with specified group ID
     *
     * <p><b>IMPORTANT:</b> This method MUST be called before any other consumer
     * operations (poll, commit, etc.) on each thread.</p>
     *
     * @param topic Topic to subscribe to
     * @param groupId Consumer group ID
     * @return Group ID
     * @throws IllegalStateException if consumer is already initialized on this thread
     */
    @Step("Initialize consumer for topic: {topic} with group ID: {groupId}")
    public String initConsumer(String topic, String groupId) {
        // Check if consumer already exists on this thread
        if (consumerThreadLocal.get() != null) {
            log.warn("Consumer already initialized on thread {}. Closing existing consumer.",
                    Thread.currentThread().getName());
            close(); // Clean up existing consumer first
        }

        KafkaConsumer<String, String> consumer = createConsumer(groupId);
        consumer.subscribe(Collections.singletonList(topic));
        consumerThreadLocal.set(consumer);
        groupIdThreadLocal.set(groupId);

        log.info("Consumer initialized for topic '{}' with group ID '{}'", topic, groupId);
        return groupId;
    }

    /**
     * Subscribes consumer to multiple topics
     *
     * @param topics List of topics to subscribe to
     * @return Group ID assigned to this consumer
     */
    @Step("Subscribe to topics: {topics}")
    public String subscribeToTopics(List<String> topics) {
        String groupId = config.consumerGroupIdBase() + "-" + UUID.randomUUID();

        // Check if consumer already exists on this thread
        if (consumerThreadLocal.get() != null) {
            log.warn("Consumer already initialized on thread {}. Closing existing consumer.",
                    Thread.currentThread().getName());
            close();
        }

        KafkaConsumer<String, String> consumer = createConsumer(groupId);
        consumer.subscribe(topics);
        consumerThreadLocal.set(consumer);
        groupIdThreadLocal.set(groupId);

        log.info("Consumer subscribed to topics {} with group ID '{}'", topics, groupId);
        return groupId;
    }

    /**
     * Gets the consumer for the current thread
     *
     * @return KafkaConsumer instance for this thread
     * @throws IllegalStateException if consumer not initialized (call initConsumer first)
     */
    private KafkaConsumer<String, String> getConsumer() {
        KafkaConsumer<String, String> consumer = consumerThreadLocal.get();
        if (consumer == null) {
            throw new IllegalStateException(
                    "Consumer not initialized for thread: " + Thread.currentThread().getName() +
                            ". Call initConsumer() first before using consumer operations.");
        }
        return consumer;
    }

    /**
     * Polls messages from Kafka
     *
     * @param timeoutSeconds Poll timeout in seconds
     * @return List of consumed records
     * @throws IllegalStateException if consumer not initialized
     */
    @Step("Poll messages with timeout: {timeoutSeconds}s")
    public List<ConsumerRecordDto> poll(int timeoutSeconds) {
        ConsumerRecords<String, String> records = getConsumer().poll(Duration.ofSeconds(timeoutSeconds));

        List<ConsumerRecordDto> dtoList = new ArrayList<>();
        for (ConsumerRecord<String, String> record : records) {
            dtoList.add(mapToDto(record));
        }

        log.info("Polled {} messages", dtoList.size());
        return dtoList;
    }

    /**
     * Polls all available messages until no more messages
     *
     * @param maxAttempts Maximum poll attempts
     * @return List of all consumed records
     */
    @Step("Poll all messages (max attempts: {maxAttempts})")
    public List<ConsumerRecordDto> pollAll(int maxAttempts) {
        List<ConsumerRecordDto> allRecords = new ArrayList<>();
        int attempts = 0;

        while (attempts < maxAttempts) {
            List<ConsumerRecordDto> records = poll(1);
            if (records.isEmpty()) {
                break;
            }
            allRecords.addAll(records);
            attempts++;
        }

        log.info("Polled total {} messages in {} attempts", allRecords.size(), attempts);
        return allRecords;
    }

    /**
     * Commits offsets synchronously
     *
     * @throws IllegalStateException if consumer not initialized
     */
    @Step("Commit offsets sync")
    public void commitSync() {
        log.debug("Committing offsets synchronously");
        getConsumer().commitSync();
    }

    /**
     * Commits offsets asynchronously
     *
     * @throws IllegalStateException if consumer not initialized
     */
    @Step("Commit offsets async")
    public void commitAsync() {
        log.debug("Committing offsets asynchronously");
        getConsumer().commitAsync((offsets, exception) -> {
            if (exception != null) {
                log.error("Failed to commit offsets: {}", exception.getMessage(), exception);
            } else {
                log.debug("Offsets committed successfully: {}", offsets);
            }
        });
    }

    /**
     * Commits specific offset for a partition
     *
     * @param topic Topic name
     * @param partition Partition number
     * @param offset Offset to commit
     * @throws IllegalStateException if consumer not initialized
     */
    @Step("Commit offset for partition")
    public void commitOffset(String topic, int partition, long offset) {
        TopicPartition topicPartition = new TopicPartition(topic, partition);
        Map<TopicPartition, OffsetAndMetadata> offsetMap = new HashMap<>();
        offsetMap.put(topicPartition, new OffsetAndMetadata(offset + 1)); // +1 because commit is next offset

        log.debug("Committing offset {} for partition {}", offset, partition);
        getConsumer().commitSync(offsetMap);
    }

    /**
     * Seeks to beginning of partitions
     *
     * @param topic Topic name (unused but kept for API compatibility)
     * @throws IllegalStateException if consumer not initialized
     */
    @Step("Seek to beginning")
    public void seekToBeginning(String topic) {
        getConsumer().seekToBeginning(getConsumer().assignment());
        log.info("Sought to beginning for topic: {}", topic);
    }

    /**
     * Seeks to end of partitions
     *
     * @param topic Topic name (unused but kept for API compatibility)
     * @throws IllegalStateException if consumer not initialized
     */
    @Step("Seek to end")
    public void seekToEnd(String topic) {
        getConsumer().seekToEnd(getConsumer().assignment());
        log.info("Sought to end for topic: {}", topic);
    }

    /**
     * Gets current position (offset) for a partition
     *
     * @param topic Topic name
     * @param partition Partition number
     * @return Current offset position
     * @throws IllegalStateException if consumer not initialized
     */
    public long getPosition(String topic, int partition) {
        TopicPartition topicPartition = new TopicPartition(topic, partition);
        return getConsumer().position(topicPartition);
    }

    /**
     * Maps ConsumerRecord to ConsumerRecordDto
     */
    private ConsumerRecordDto mapToDto(ConsumerRecord<String, String> record) {
        Map<String, String> headers = new HashMap<>();
        for (Header header : record.headers()) {
            headers.put(header.key(), new String(header.value(), StandardCharsets.UTF_8));
        }

        return ConsumerRecordDto.builder()
                .topic(record.topic())
                .partition(record.partition())
                .offset(record.offset())
                .key(record.key())
                .value(record.value())
                .headers(headers)
                .timestamp(record.timestamp())
                .timestampType(record.timestampType().name())
                .build();
    }

    /**
     * Closes the consumer for the current thread and removes ThreadLocal references.
     *
     * <p><b>Thread Safety:</b> This method only affects the consumer on the calling thread.
     * Other threads' consumers are not affected.</p>
     *
     * <p><b>Cleanup:</b> Always calls {@link ThreadLocal#remove()} to prevent memory leaks
     * in thread pool scenarios.</p>
     */
    @Override
    public void close() {
        KafkaConsumer<String, String> consumer = consumerThreadLocal.get();
        try {
            if (consumer != null) {
                log.debug("Closing consumer on thread: {}", Thread.currentThread().getName());
                consumer.close();
            }
        } catch (Exception e) {
            log.error("Error closing consumer on thread {}: {}",
                    Thread.currentThread().getName(), e.getMessage(), e);
        } finally {
            // CRITICAL: Always remove ThreadLocal to prevent memory leaks
            consumerThreadLocal.remove();
            groupIdThreadLocal.remove();
        }
    }

    /**
     * Gets the group ID for the current thread
     *
     * @return Consumer group ID, or null if not initialized
     */
    public String getGroupId() {
        return groupIdThreadLocal.get();
    }
}
