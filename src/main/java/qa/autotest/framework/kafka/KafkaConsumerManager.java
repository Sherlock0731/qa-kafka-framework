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
 */
@Slf4j
public class KafkaConsumerManager implements AutoCloseable {

    private final KafkaConfig config;
    private final ThreadLocal<KafkaConsumer<String, String>> consumerThreadLocal;
    private final ThreadLocal<String> groupIdThreadLocal;

    public KafkaConsumerManager(KafkaConfig config) {
        this.config = config;
        this.consumerThreadLocal = new ThreadLocal<>();
        this.groupIdThreadLocal = new ThreadLocal<>();
    }

    /**
     * Creates a new Kafka consumer with unique group ID
     */
    private KafkaConsumer<String, String> createConsumer(String groupId) {
        log.debug("Creating Kafka consumer on thread: {} with group ID: {}",
                Thread.currentThread().getName(), groupId);

        // Base properties
        Properties props = KafkaPropertiesBuilder.buildBaseProperties(config);
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

        // Security configuration (SSL/TLS)
        KafkaPropertiesBuilder.configureSecurity(props, config);

        return new KafkaConsumer<>(props);
    }

    /**
     * Initializes consumer for the current thread with unique group ID
     *
     * @param topic Topic to subscribe to
     * @return Unique group ID
     */
    @Step("Initialize consumer for topic: {topic}")
    public String initConsumer(String topic) {
        String groupId = config.consumerGroupIdBase() + "-" + UUID.randomUUID();
        return initConsumer(topic, groupId);
    }

    /**
     * Initializes consumer for the current thread with specified group ID
     *
     * @param topic Topic to subscribe to
     * @param groupId Consumer group ID
     * @return Group ID
     */
    @Step("Initialize consumer for topic: {topic} with group ID: {groupId}")
    public String initConsumer(String topic, String groupId) {
        KafkaConsumer<String, String> consumer = createConsumer(groupId);
        consumer.subscribe(Collections.singletonList(topic));
        consumerThreadLocal.set(consumer);
        groupIdThreadLocal.set(groupId);

        log.info("Consumer initialized for topic '{}' with group ID '{}'", topic, groupId);
        return groupId;
    }

    /**
     * Subscribes consumer to multiple topics
     */
    @Step("Subscribe to topics: {topics}")
    public String subscribeToTopics(List<String> topics) {
        String groupId = config.consumerGroupIdBase() + "-" + UUID.randomUUID();
        KafkaConsumer<String, String> consumer = createConsumer(groupId);
        consumer.subscribe(topics);
        consumerThreadLocal.set(consumer);
        groupIdThreadLocal.set(groupId);

        log.info("Consumer subscribed to topics {} with group ID '{}'", topics, groupId);
        return groupId;
    }

    /**
     * Gets the consumer for the current thread
     */
    private KafkaConsumer<String, String> getConsumer() {
        KafkaConsumer<String, String> consumer = consumerThreadLocal.get();
        if (consumer == null) {
            String threadName = Thread.currentThread().getName();
            log.error("Consumer not initialized for thread: {}", threadName);
            throw qa.autotest.framework.exceptions.KafkaConsumerException.notInitialized(threadName);
        }
        return consumer;
    }

    /**
     * Polls messages from Kafka
     *
     * @param timeoutSeconds Poll timeout in seconds
     * @return List of consumed records
     */
    @Step("Poll messages with timeout: {timeoutSeconds}s")
    public List<ConsumerRecordDto> poll(int timeoutSeconds) {
        long startTime = System.currentTimeMillis();
        
        try {
            ConsumerRecords<String, String> records = getConsumer().poll(Duration.ofSeconds(timeoutSeconds));

            List<ConsumerRecordDto> dtoList = new ArrayList<>();
            for (ConsumerRecord<String, String> record : records) {
                dtoList.add(mapToDto(record));
            }

            long duration = System.currentTimeMillis() - startTime;
            log.info("Polled {} messages in {} ms", dtoList.size(), duration);
            
            // Record metrics
            qa.autotest.framework.metrics.TestMetricsCollector.recordDuration("consumer_poll", duration);
            
            return dtoList;
        } catch (org.apache.kafka.common.errors.TimeoutException e) {
            long duration = System.currentTimeMillis() - startTime;
            log.warn("Poll timeout after {} ms", duration);
            throw new qa.autotest.framework.exceptions.KafkaTimeoutException(
                "consumer_poll", 
                timeoutSeconds * 1000L, 
                e
            );
        } catch (Exception e) {
            long duration = System.currentTimeMillis() - startTime;
            log.error("Poll failed after {} ms: {}", duration, e.getMessage(), e);
            throw new qa.autotest.framework.exceptions.KafkaConsumerException(
                "Failed to poll messages",
                e,
                qa.autotest.framework.exceptions.KafkaTestException.ErrorType.UNKNOWN
            ).addContext("timeout_seconds", String.valueOf(timeoutSeconds))
             .addContext("duration_ms", String.valueOf(duration));
        }
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
     */
    @Step("Commit offsets sync")
    public void commitSync() {
        log.debug("Committing offsets synchronously");
        getConsumer().commitSync();
    }

    /**
     * Commits offsets asynchronously
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
     */
    @Step("Seek to beginning")
    public void seekToBeginning(String topic) {
        getConsumer().seekToBeginning(getConsumer().assignment());
        log.info("Sought to beginning for topic: {}", topic);
    }

    /**
     * Seeks to end of partitions
     */
    @Step("Seek to end")
    public void seekToEnd(String topic) {
        getConsumer().seekToEnd(getConsumer().assignment());
        log.info("Sought to end for topic: {}", topic);
    }

    /**
     * Gets current position (offset) for a partition
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
     * Closes the consumer for the current thread
     */
    @Override
    public void close() {
        KafkaConsumer<String, String> consumer = consumerThreadLocal.get();
        try {
            if (consumer != null) {
                log.debug("Closing consumer on thread: {}", Thread.currentThread().getName());
                consumer.close();
            }
        } finally {
            consumerThreadLocal.remove();
            groupIdThreadLocal.remove();
        }
    }

    /**
     * Gets the group ID for the current thread
     */
    public String getGroupId() {
        return groupIdThreadLocal.get();
    }
}
