package tests;

import io.qameta.allure.Step;
import lombok.extern.slf4j.Slf4j;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.extension.ExtendWith;
import qa.autotest.framework.config.ConfigFactory;
import qa.autotest.framework.utils.AsyncTestHelper;
import qa.autotest.framework.config.KafkaConfig;
import qa.autotest.framework.kafka.KafkaConsumerManager;
import qa.autotest.framework.kafka.KafkaProducerManager;
import qa.autotest.framework.kafka.KafkaTopicManager;
import tests.listeners.AllureKafkaListener;
import tests.listeners.KafkaTestExecutionListener;

import java.util.ArrayList;
import java.util.List;

/**
 * Base test class with common setup and teardown for Kafka tests
 * Supports parallel execution with thread-safe resources
 */
@Slf4j
@ExtendWith({AllureKafkaListener.class, KafkaTestExecutionListener.class})
public abstract class BaseTest {

    protected static final KafkaConfig CONFIG = ConfigFactory.getConfig();
    
    protected KafkaProducerManager producerManager;
    protected KafkaConsumerManager consumerManager;
    protected KafkaTopicManager topicManager;
    
    // Track created topics for cleanup
    protected List<String> createdTopics = new ArrayList<>();

    @BeforeAll
    static void setUpAll() {
        log.info("=== Kafka Test Framework Initialized ===");
        log.info("Bootstrap Servers: {}", CONFIG.kafkaBootstrapServers());
        log.info("Security Protocol: {}", CONFIG.securityProtocol());
    }

    @BeforeEach
    @Step("Setup test environment")
    void setUp() {
        log.info("=== Test Started: {} ===", this.getClass().getSimpleName());
        log.info("Thread: {}", Thread.currentThread().getName());
        
        // Initialize managers for current thread
        producerManager = new KafkaProducerManager(CONFIG);
        consumerManager = new KafkaConsumerManager(CONFIG);
        topicManager = new KafkaTopicManager(CONFIG);
        
        createdTopics = new ArrayList<>();
    }

    @AfterEach
    @Step("Cleanup test environment")
    void tearDown() {
        log.info("=== Test Cleanup Started ===");
        
        try {
            // Close consumer first
            if (consumerManager != null) {
                try {
                    consumerManager.close();
                    log.debug("Consumer closed successfully");
                } catch (Exception e) {
                    log.warn("Error closing consumer: {}", e.getMessage());
                }
            }
            
            // Close producer
            if (producerManager != null) {
                try {
                    producerManager.close();
                    log.debug("Producer closed successfully");
                } catch (Exception e) {
                    log.warn("Error closing producer: {}", e.getMessage());
                }
            }
            
            // STRICT CLEANUP: Always delete topics regardless of config
            if (topicManager != null && !createdTopics.isEmpty()) {
                log.info("Cleaning up {} test topics", createdTopics.size());
                
                for (String topic : createdTopics) {
                    deleteTopicWithRetry(topic, 3);
                }
                
                log.info("All test topics cleaned up successfully");
            }
            
        } finally {
            // Close topic manager in finally block to ensure it's always closed
            if (topicManager != null) {
                try {
                    topicManager.close();
                    log.debug("Topic manager closed successfully");
                } catch (Exception e) {
                    log.warn("Error closing topic manager: {}", e.getMessage());
                }
            }
        }
        
        log.info("=== Test Finished: {} ===", this.getClass().getSimpleName());
    }
    
    /**
     * Delete topic with retry logic for reliability
     * 
     * @param topic Topic name to delete
     * @param maxRetries Maximum number of retry attempts
     */
    private void deleteTopicWithRetry(String topic, int maxRetries) {
        for (int attempt = 1; attempt <= maxRetries; attempt++) {
            try {
                topicManager.deleteTopic(topic);
                log.debug("✓ Deleted test topic: {} (attempt {})", topic, attempt);
                return; // Success - exit method
                
            } catch (Exception e) {
                if (attempt < maxRetries) {
                    log.warn("Failed to delete topic {} (attempt {}), retrying: {}", 
                        topic, attempt, e.getMessage());
                    
                    // Wait before retry with exponential backoff: 1s, 2s, 3s
                    AsyncTestHelper.waitFor(attempt);
                } else {
                    // Final attempt failed
                    log.error("✗ Failed to delete topic {} after {} attempts: {}", 
                        topic, maxRetries, e.getMessage());
                    
                    // Log but don't fail the test - topic will be cleaned up eventually
                }
            }
        }
    }
    
    /**
     * Helper method to create and track topic
     */
    @Step("Create test topic")
    protected String createTestTopic() {
        String topic = topicManager.createUniqueTopic();
        createdTopics.add(topic);
        return topic;
    }
    
    /**
     * Helper method to create topic with specific partitions
     */
    @Step("Create test topic with {partitions} partitions")
    protected String createTestTopic(int partitions) {
        String topic = topicManager.createTopicWithPartitions(partitions);
        createdTopics.add(topic);
        return topic;
    }
    
    /**
     * Helper method to create and track any topic by name
     * Use this for DLQ topics, retry topics, etc.
     * 
     * @param topicName Name of the topic to create
     * @param partitions Number of partitions
     * @param replicationFactor Replication factor
     * @return Created topic name
     */
    @Step("Create and track topic: {topicName}")
    protected String createAndTrackTopic(String topicName, int partitions, short replicationFactor) {
        topicManager.createTopic(topicName, partitions, replicationFactor);
        createdTopics.add(topicName);
        log.debug("Created and tracking topic: {}", topicName);
        return topicName;
    }
    
    /**
     * Manual tracking of externally created topics
     * Use when topic is created outside of helper methods
     * 
     * @param topicName Topic name to track for cleanup
     */
    protected void trackTopicForCleanup(String topicName) {
        if (!createdTopics.contains(topicName)) {
            createdTopics.add(topicName);
            log.debug("Manually tracking topic for cleanup: {}", topicName);
        }
    }
}
