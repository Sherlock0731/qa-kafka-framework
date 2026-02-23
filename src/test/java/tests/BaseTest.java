package tests;

import io.qameta.allure.Step;
import lombok.extern.slf4j.Slf4j;
import org.junit.jupiter.api.*;
import org.junit.jupiter.api.extension.ExtendWith;
import qa.autotest.framework.application.service.KafkaTestFacade;
import qa.autotest.framework.config.ConfigFactory;
import qa.autotest.framework.config.KafkaConfig;
import qa.autotest.framework.domain.model.*;
import tests.listeners.AllureKafkaListener;
import tests.listeners.KafkaTestExecutionListener;
import qa.autotest.framework.metrics.TestMetricsExtension;

import java.time.Duration;
import java.util.*;

/**
 * Base Test Class with Hexagonal Architecture
 * <p>
 * This class provides a clean API for Kafka testing using the Hexagonal Architecture pattern.
 * <p>
 * Architecture Benefits:
 * - Domain logic is independent of Kafka
 * - Easy to test (can mock ports)
 * - Easy to switch messaging systems
 * - Clear separation of concerns
 * <p>
 * Memory Leak Prevention:
 * - Uses WeakHashMap-backed set: GC может собрать facade после закрытия теста,
 *   не дожидаясь @AfterAll — устраняет накопление ссылок при большом числе тестов
 * - Global cleanup в @AfterAll для явного закрытия ресурсов
 * - Thread-safe для параллельного выполнения
 *
 * @author QA Automation Team
 * @version 2.0.1 - Hexagonal Architecture + WeakReference tracking
 */
@Slf4j
@ExtendWith({TestMetricsExtension.class, AllureKafkaListener.class, KafkaTestExecutionListener.class})
public abstract class BaseTest {

    protected static final KafkaConfig CONFIG = ConfigFactory.getConfig();

    /**
     * WeakHashMap-backed set для отслеживания фасадов.
     * <p>
     * Использование WeakReference: если тест завершился и kafka-поле обнулилось,
     * GC может собрать объект до вызова @AfterAll, не допуская накопления
     * мёртвых ссылок при большом числе тестов (memory pressure prevention).
     * <p>
     * @AfterAll всё равно вызывает closeAll() на оставшихся живых фасадах —
     * это страховочный слой для тех, чей жизненный цикл выходит за рамки одного теста
     * (например, facade из createNewFacade()).
     */
    private static final Set<KafkaTestFacade> ALL_FACADES =
            Collections.synchronizedSet(Collections.newSetFromMap(new WeakHashMap<>()));

    /**
     * Test instance facade - provides simplified API
     * This is the main entry point for test operations
     */
    protected KafkaTestFacade kafka;

    /**
     * Track created topics for cleanup
     */
    protected final List<String> createdTopics = new ArrayList<>();

    /**
     * Test execution tracking
     */
    protected long testStartTime;

    @BeforeAll
    static void initFramework() {
        log.info("=".repeat(80));
        log.info("Kafka Test Framework - Hexagonal Architecture");
        log.info("=".repeat(80));
        log.info("Configuration:");
        log.info("  - Kafka Bootstrap: {}", CONFIG.kafkaBootstrapServers());
        log.info("  - Security Protocol: {}", CONFIG.securityProtocol());
        log.info("  - Architecture: Hexagonal (Domain + Application + Infrastructure)");
        log.info("  - Memory Leak Prevention: ENABLED");
        log.info("=".repeat(80));
    }

    @BeforeEach
    void setUp(TestInfo testInfo) {
        testStartTime = System.currentTimeMillis();

        log.info("=".repeat(80));
        log.info("Starting test: {}", testInfo.getDisplayName());
        log.info("Thread: {}", Thread.currentThread().getName());
        log.info("=".repeat(80));

        // Initialize Hexagonal Architecture facade
        kafka = new KafkaTestFacade(CONFIG);

        // Register for global cleanup
        ALL_FACADES.add(kafka);

        log.debug("Test facade initialized");
    }

    @AfterEach
    void tearDown(TestInfo testInfo) {
        long duration = System.currentTimeMillis() - testStartTime;

        log.info("=".repeat(80));
        log.info("Finishing test: {}", testInfo.getDisplayName());
        log.info("Duration: {}ms", duration);
        log.info("=".repeat(80));

        try {
            // Cleanup created topics
            cleanupTopics();

            // Close current thread resources
            kafka.close();

            log.debug("Test cleanup completed successfully");

        } catch (Exception e) {
            log.error("Error during test cleanup", e);
        }
    }

    /**
     * CRITICAL: Global cleanup for ALL threads
     * <p>
     * This method prevents memory leaks by closing ALL Kafka clients
     * from ALL threads, not just the current thread.
     * <p>
     * Architecture note:
     * - Calls closeAll() on each facade
     * - Each facade calls closeAll() on its adapters
     * - Adapters use WeakReference tracking to find all instances
     */
    @AfterAll
    static void globalCleanup() {
        log.info("=".repeat(80));
        log.info("GLOBAL CLEANUP - Closing ALL resources from ALL threads");
        log.info("=".repeat(80));

        int facadeClosed = 0;
        int facadeFailed = 0;

        synchronized (ALL_FACADES) {
            for (KafkaTestFacade facade : ALL_FACADES) {
                try {
                    facade.closeAll();
                    facadeClosed++;
                } catch (Exception e) {
                    facadeFailed++;
                    log.error("Failed to close facade", e);
                }
            }
            ALL_FACADES.clear();
        }

        log.info("=".repeat(80));
        log.info("Global cleanup completed");
        log.info("  - Facades closed: {}", facadeClosed);
        log.info("  - Facades failed: {}", facadeFailed);
        log.info("=".repeat(80));
    }

    // ==================== Helper Methods (Domain-Oriented) ====================

    /**
     * Creates a test topic with auto-generated name.
     * Returns topic name for use in tests.
     */
    @Step("Create test topic with {partitions} partitions")
    protected String createTestTopic(int partitions) {
        return createTestTopic(generateTopicName("auto"), partitions);
    }

    @Step("Create test topic (auto name)")
    protected String createTestTopic() {
        return createTestTopic(generateTopicName("auto"), 2);
    }

    /**
     * Creates a test topic with given name and default 2 partitions (Aiven limit).
     * Returns topic name for use in tests.
     */
    @Step("Create test topic: {topicName}")
    protected String createTestTopic(String topicName) {
        return createTestTopic(topicName, 2);
    }

    /**
     * Creates a test topic with given partitions.
     * replicationFactor берётся из конфигурации (kafka.test.topic.replication.factor).
     */
    @Step("Create test topic: {topicName} with {partitions} partitions")
    protected String createTestTopic(String topicName, int partitions) {
        Topic topic = Topic.builder()
                .name(topicName)
                .partitionCount(partitions)
                .replicationFactor(CONFIG.testTopicReplicationFactor())
                .build();

        kafka.createTopic(topic);
        createdTopics.add(topicName);
        kafka.waitForTopic(topicName, 10);

        return topicName;
    }

    /**
     * Creates a topic with DLQ
     */
    @Step("Create topic with DLQ: {topicName}")
    protected boolean createTopicWithDlq(String topicName) {
        boolean created = kafka.createTopicWithDlq(topicName);

        if (created) {
            createdTopics.add(topicName);
            createdTopics.add(topicName + "-dlq");
        }

        return created;
    }

    /**
     * Creates a fresh KafkaTestFacade (useful for rebalance tests).
     * The new facade is tracked for cleanup in @AfterAll.
     */
    protected KafkaTestFacade createNewFacade() {
        KafkaTestFacade fresh = new KafkaTestFacade(CONFIG);
        ALL_FACADES.add(fresh);
        return fresh;
    }

    /**
     * Publishes a list of domain messages via the facade.
     */
    @Step("Publish batch of {messages.size} messages")
    protected List<PublishResult> publishBatch(List<Message> messages) {
        return kafka.publishBatch(messages);
    }

    /**
     * Publishes a message (domain method)
     */
    @Step("Publish message to {topicName}")
    protected PublishResult publishMessage(String topicName, String key, String value) {
        return kafka.publish(topicName, key, value);
    }

    /**
     * Publishes a domain message
     */
    @Step("Publish domain message")
    protected PublishResult publishMessage(Message message) {
        return kafka.publish(message);
    }

    /**
     * Consumes messages (domain method)
     */
    @Step("Consume messages from {topicName}")
    protected List<Message> consumeMessages(String topicName, int expectedCount, Duration timeout) {
        kafka.subscribe(topicName);
        kafka.seekToBeginning();

        ConsumeResult result = kafka.pollMessages(expectedCount, timeout);

        return result.isSuccess() ? result.getMessages() : List.of();
    }

    /**
     * Consumes all available messages
     */
    @Step("Consume all messages from {topicName}")
    protected List<Message> consumeAllMessages(String topicName, int maxMessages) {
        kafka.subscribe(topicName);
        kafka.seekToBeginning();

        ConsumeResult result = kafka.consumeAll(maxMessages, Duration.ofSeconds(10));

        return result.isSuccess() ? result.getMessages() : List.of();
    }

    /**
     * Waits for messages to be available
     */
    @Step("Wait for messages in {topicName}")
    protected boolean waitForMessages(String topicName, int expectedCount, Duration timeout) {
        kafka.subscribe(topicName);
        kafka.seekToBeginning();

        ConsumeResult result = kafka.pollMessages(expectedCount, timeout);

        return result.isSuccess() && result.getMessageCount() >= expectedCount;
    }

    /**
     * Generates a unique topic name for tests
     */
    protected String generateTopicName(String prefix) {
        return String.format("qa-test-%s-%s", prefix, UUID.randomUUID());
    }

    /**
     * Cleanup created topics
     */
    private void cleanupTopics() {
        if (createdTopics.isEmpty()) {
            return;
        }

        log.debug("Cleaning up {} created topics", createdTopics.size());

        for (String topicName : createdTopics) {
            try {
                kafka.deleteTopic(topicName);
            } catch (Exception e) {
                log.warn("Failed to delete topic: {}", topicName);
            }
        }

        createdTopics.clear();
    }

    // ==================== Domain Assertions ====================

    /**
     * Asserts that publish was successful
     */
    protected void assertPublishSuccess(PublishResult result) {
        Assertions.assertTrue(result.isSuccess(),
                "Publish failed: " + result.getErrorMessage());
    }

    /**
     * Asserts that consume was successful
     */
    protected void assertConsumeSuccess(ConsumeResult result) {
        Assertions.assertTrue(result.isSuccess(),
                "Consume failed: " + result.getErrorMessage());
    }

    /**
     * Asserts message count
     */
    protected void assertMessageCount(ConsumeResult result, int expectedCount) {
        Assertions.assertEquals(expectedCount, result.getMessageCount(),
                "Expected " + expectedCount + " messages, got " + result.getMessageCount());
    }

    /**
     * Asserts message content
     */
    protected void assertMessageContent(Message message, String expectedContent) {
        Assertions.assertEquals(expectedContent, message.getContent(),
                "Message content mismatch");
    }

    // ==================== Metrics ====================

    /**
     * Logs current metrics
     */
    protected void logMetrics() {
        log.info("Test Metrics: {}", kafka.getMetrics());
    }
}
