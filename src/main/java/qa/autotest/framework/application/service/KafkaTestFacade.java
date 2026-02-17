package qa.autotest.framework.application.service;

import lombok.Getter;
import lombok.extern.slf4j.Slf4j;
import qa.autotest.framework.config.KafkaConfig;
import qa.autotest.framework.domain.model.*;
import qa.autotest.framework.infrastructure.kafka.adapter.KafkaAdminAdapter;
import qa.autotest.framework.infrastructure.kafka.adapter.KafkaConsumerAdapter;
import qa.autotest.framework.infrastructure.kafka.adapter.KafkaProducerAdapter;

import java.time.Duration;
import java.util.List;
import java.util.Set;
import java.util.UUID;

/**
 * Application Facade: KafkaTestFacade
 * <p>
 * Provides simplified API for test classes.
 * Hides complexity of Hexagonal Architecture from tests.
 * <p>
 * This is the primary entry point for tests - follows Facade pattern.
 */
@Slf4j
@Getter
public class KafkaTestFacade implements AutoCloseable {

    // Infrastructure adapters
    private final KafkaProducerAdapter producerAdapter;
    private final KafkaConsumerAdapter consumerAdapter;
    private final KafkaAdminAdapter adminAdapter;

    // Application services
    private final MessagePublishingService publishingService;
    private final MessageConsumptionService consumptionService;
    private final TopicManagementService topicManagementService;

    private final KafkaConfig config;
    private final String consumerGroupId;

    /**
     * Constructor: Initializes complete Hexagonal Architecture
     * <p>
     * Creates:
     * 1. Infrastructure adapters (outer layer)
     * 2. Application services (middle layer)
     * 3. Wires them together via ports (interfaces)
     */
    public KafkaTestFacade(KafkaConfig config) {
        this.config = config;
        this.consumerGroupId = "qa-test-group-" + UUID.randomUUID();

        log.debug("Initializing KafkaTestFacade with Hexagonal Architecture");

        // Create infrastructure adapters
        this.producerAdapter = new KafkaProducerAdapter(config);
        this.consumerAdapter = new KafkaConsumerAdapter(config, consumerGroupId);
        this.adminAdapter = new KafkaAdminAdapter(config);

        // Create application services (depend on ports)
        this.publishingService = new MessagePublishingService(producerAdapter);
        this.consumptionService = new MessageConsumptionService(consumerAdapter);
        this.topicManagementService = new TopicManagementService(adminAdapter);

        log.info("KafkaTestFacade initialized: groupId={}", consumerGroupId);
    }

    // ==================== Simplified Publishing API ====================

    /**
     * Publishes a message (simplified API)
     */
    public PublishResult publish(String topicName, String key, String content) {
        Topic topic = Topic.builder().name(topicName).build();
        Message message = Message.builder()
                .topic(topic)
                .key(key)
                .content(content)
                .build();

        return publishingService.publishMessage(message);
    }

    /**
     * Publishes a message with headers
     */
    public PublishResult publish(String topicName, String key, String content, java.util.Map<String, String> headers) {
        Topic topic = Topic.builder().name(topicName).build();
        Message message = Message.builder()
                .topic(topic)
                .key(key)
                .content(content)
                .headers(headers)
                .build();

        return publishingService.publishMessage(message);
    }

    /**
     * Publishes a domain Message
     */
    public PublishResult publish(Message message) {
        return publishingService.publishMessage(message);
    }

    /**
     * Publishes with retry
     */
    public PublishResult publishWithRetry(Message message, int maxRetries) {
        return publishingService.publishWithRetry(message, maxRetries);
    }

    // ==================== Simplified Consumption API ====================

    /**
     * Subscribes to topic (simplified API)
     */
    public void subscribe(String topicName) {
        Topic topic = Topic.builder().name(topicName).build();
        consumptionService.subscribeToTopic(topic);
    }

    /**
     * Subscribes to multiple topics
     */
    public void subscribe(String... topicNames) {
        Set<Topic> topics = Set.of(topicNames).stream()
                .map(name -> Topic.builder().name(name).build())
                .collect(java.util.stream.Collectors.toSet());

        consumptionService.subscribeToTopics(topics);
    }

    /**
     * Polls for messages with timeout
     */
    public ConsumeResult poll(Duration timeout) {
        return consumptionService.consumeMessages(timeout);
    }

    /**
     * Polls for expected number of messages
     */
    public ConsumeResult pollMessages(int expectedCount, Duration timeout) {
        return consumptionService.consumeExpectedMessages(expectedCount, timeout);
    }

    /**
     * Consumes all available messages
     */
    public ConsumeResult consumeAll(int maxMessages, Duration timeout) {
        return consumptionService.consumeAllMessages(maxMessages, timeout);
    }

    /**
     * Seeks to beginning
     */
    public void seekToBeginning() {
        consumptionService.seekToBeginning();
    }

    /**
     * Seeks to end
     */
    public void seekToEnd() {
        consumptionService.seekToEnd();
    }

    /**
     * Commits current consumer offsets synchronously
     */
    public void commitSync() {
        log.debug("Committing offsets synchronously");
        consumptionService.commitOffsets();
    }

    /**
     * Commits a specific offset for a partition (seeks to offset+1 then commits)
     */
    public void commitOffset(String topicName, int partition, long offset) {
        log.debug("Committing offset: topic={}, partition={}, offset={}", topicName, partition, offset);
        Topic topic = Topic.builder().name(topicName).build();
        consumptionService.seekToOffset(topic, partition, offset + 1);
        consumptionService.commitOffsets();
    }

    // ==================== Simplified Topic Management API ====================

    /**
     * Creates a topic (simplified API)
     */
    public boolean createTopic(String topicName) {
        Topic topic = Topic.builder().name(topicName).build();
        return topicManagementService.createTopic(topic);
    }

    /**
     * Creates a topic with partitions
     */
    public boolean createTopic(String topicName, int partitions) {
        Topic topic = Topic.builder()
                .name(topicName)
                .partitionCount(partitions)
                .build();
        return topicManagementService.createTopic(topic);
    }

    /**
     * Creates a topic with full configuration
     */
    public boolean createTopic(Topic topic) {
        return topicManagementService.createTopic(topic);
    }

    /**
     * Creates topic with DLQ
     */
    public boolean createTopicWithDlq(String topicName) {
        Topic topic = Topic.builder().name(topicName).build();
        return topicManagementService.createTopicWithDlq(topic);
    }

    /**
     * Deletes a topic
     */
    public boolean deleteTopic(String topicName) {
        Topic topic = Topic.builder().name(topicName).build();
        return topicManagementService.deleteTopic(topic);
    }

    /**
     * Deletes all test topics
     */
    public int deleteTestTopics() {
        return topicManagementService.deleteTestTopics();
    }

    /**
     * Checks if topic exists
     */
    public boolean topicExists(String topicName) {
        Topic topic = Topic.builder().name(topicName).build();
        return topicManagementService.topicExists(topic);
    }

    /**
     * Waits for topic to be available
     */
    public boolean waitForTopic(String topicName, int timeoutSeconds) {
        Topic topic = Topic.builder().name(topicName).build();
        return topicManagementService.waitForTopic(topic, timeoutSeconds);
    }

    // ==================== Resource Management ====================

    /**
     * Publishes a batch of messages and returns all results
     */
    public List<PublishResult> publishBatch(List<Message> messages) {
        return publishingService.publishBatch(messages);
    }

    /**
     * Flushes the producer - ensures all buffered messages are sent to Kafka
     */
    public void flush() {
        log.debug("Flushing producer to ensure all messages are sent");
        producerAdapter.flush();
    }

    /**
     * Closes current thread resources
     */
    @Override
    public void close() {
        log.debug("Closing KafkaTestFacade for current thread");
        publishingService.close();
        consumptionService.close();
        topicManagementService.close();
    }

    /**
     * CRITICAL: Closes ALL resources from ALL threads
     * Must be called in @AfterAll to prevent memory leaks
     */
    public void closeAll() {
        log.info("Closing ALL KafkaTestFacade resources from ALL threads");

        // Close all producers from all threads
        producerAdapter.closeAll();

        // Close all consumers from all threads
        consumerAdapter.closeAll();

        // Close admin client
        adminAdapter.close();

        log.info("All KafkaTestFacade resources closed");
    }

    // ==================== Metrics ====================

    /**
     * Gets tracking metrics
     */
    public String getMetrics() {
        return String.format("Producers tracked: %d, Consumers tracked: %d",
                producerAdapter.getTrackedProducerCount(),
                consumerAdapter.getTrackedConsumerCount());
    }
}
