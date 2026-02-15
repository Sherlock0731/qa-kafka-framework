package tests;

import io.qameta.allure.Step;
import lombok.extern.slf4j.Slf4j;
import org.junit.jupiter.api.*;
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
import java.util.Collections;
import java.util.HashSet;
import java.util.List;
import java.util.Set;

/**
 * Base test class with common setup and teardown for Kafka tests
 * Supports parallel execution with thread-safe resources
 * 
 * MEMORY LEAK PREVENTION:
 * This class implements explicit global cleanup to prevent memory leaks
 * in ForkJoinPool and parallel execution scenarios. The globalCleanup()
 * method ensures all Kafka clients across all threads are properly closed.
 */
@Slf4j
@ExtendWith({AllureKafkaListener.class, KafkaTestExecutionListener.class})
public abstract class BaseTest {

    protected static final KafkaConfig CONFIG = ConfigFactory.getConfig();
    
    // Static thread-safe collections for tracking all managers across all threads
    private static final Set<KafkaProducerManager> ALL_PRODUCER_MANAGERS = 
        Collections.synchronizedSet(new HashSet<>());
    private static final Set<KafkaConsumerManager> ALL_CONSUMER_MANAGERS = 
        Collections.synchronizedSet(new HashSet<>());
    private static final Set<KafkaTopicManager> ALL_TOPIC_MANAGERS = 
        Collections.synchronizedSet(new HashSet<>());
    
    // Instance-level managers for per-test resources
    protected KafkaProducerManager producerManager;
    protected KafkaConsumerManager consumerManager;
    protected KafkaTopicManager topicManager;
    
    // Track created topics for cleanup
    protected List<String> createdTopics = new ArrayList<>();

    @BeforeAll
    static void setUpAll() {
        log.info("=== Kafka Test Framework Initialized ===");
        log.info("Kafka SSL enabled");
        log.info("Security Protocol: {}", CONFIG.securityProtocol());
        log.info("Memory leak prevention: ENABLED (explicit thread tracking)");
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
        
        // Track managers for global cleanup to prevent memory leaks
        ALL_PRODUCER_MANAGERS.add(producerManager);
        ALL_CONSUMER_MANAGERS.add(consumerManager);
        ALL_TOPIC_MANAGERS.add(topicManager);
        
        createdTopics = new ArrayList<>();
        
        log.debug("Managers initialized. Total tracked - Producers: {}, Consumers: {}, Topics: {}",
                 ALL_PRODUCER_MANAGERS.size(), ALL_CONSUMER_MANAGERS.size(), ALL_TOPIC_MANAGERS.size());
    }

    @AfterEach
    @Step("Cleanup test environment")
    void tearDown() {
        log.info("=== Test Cleanup Started ===");

        // Delete topics first
        safeRun("delete topics", () -> {
            if (topicManager != null && !createdTopics.isEmpty()) {
                log.info("Cleaning up {} test topics", createdTopics.size());
                createdTopics.forEach(t -> deleteTopicWithRetry(t, 5));
            }
        });
        
        // Close current thread's resources (not all threads - that's done in globalCleanup)
        safeClose(producerManager, "producer");
        safeClose(consumerManager, "consumer");
        safeClose(topicManager, "topic manager");

        log.info("=== Test Finished: {} ===", getClass().getSimpleName());
    }
    
    /**
     * CRITICAL MEMORY LEAK FIX: Global cleanup to close ALL producers/consumers from ALL threads
     * 
     * This method is ESSENTIAL for preventing memory leaks in parallel test execution.
     * It ensures that ALL Kafka clients are properly closed, even if they were created
     * by threads that no longer exist (e.g., in ForkJoinPool worker threads).
     * 
     * Without this cleanup:
     * - Producers and consumers from terminated threads remain open
     * - TCP connections to Kafka remain active
     * - Memory leaks occur during long test runs
     * - Eventually leads to connection pool exhaustion and OOM errors
     * 
     * The closeAll() methods use WeakReference tracking to find and close
     * all instances across all threads.
     */
    @AfterAll
    static void globalCleanup() {
        log.info("=== Global Cleanup Started ===");
        log.info("Total managers to cleanup - Producers: {}, Consumers: {}, Topics: {}",
                ALL_PRODUCER_MANAGERS.size(), ALL_CONSUMER_MANAGERS.size(), ALL_TOPIC_MANAGERS.size());
        
        int totalProducersClosed = 0;
        int totalConsumersClosed = 0;
        
        // Close all producers across all threads
        for (KafkaProducerManager manager : ALL_PRODUCER_MANAGERS) {
            try {
                if (manager != null) {
                    int trackedBefore = manager.getTrackedProducerCount();
                    manager.closeAll();  // Closes ALL producers from ALL threads
                    totalProducersClosed += trackedBefore;
                    log.debug("Closed producer manager (had {} tracked producers)", trackedBefore);
                }
            } catch (Exception e) {
                log.warn("Failed to close producer manager: {}", e.getMessage());
            }
        }
        ALL_PRODUCER_MANAGERS.clear();
        
        // Close all consumers across all threads
        for (KafkaConsumerManager manager : ALL_CONSUMER_MANAGERS) {
            try {
                if (manager != null) {
                    int trackedBefore = manager.getTrackedConsumerCount();
                    manager.closeAll();  // Closes ALL consumers from ALL threads
                    totalConsumersClosed += trackedBefore;
                    log.debug("Closed consumer manager (had {} tracked consumers)", trackedBefore);
                }
            } catch (Exception e) {
                log.warn("Failed to close consumer manager: {}", e.getMessage());
            }
        }
        ALL_CONSUMER_MANAGERS.clear();
        
        // Close all topic managers
        for (KafkaTopicManager manager : ALL_TOPIC_MANAGERS) {
            try {
                if (manager != null) {
                    manager.close();
                }
            } catch (Exception e) {
                log.warn("Failed to close topic manager: {}", e.getMessage());
            }
        }
        ALL_TOPIC_MANAGERS.clear();
        
        log.info("=== Global Cleanup Completed ===");
        log.info("Total resources closed - Producers: {}, Consumers: {}", 
                totalProducersClosed, totalConsumersClosed);
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
        String topic = createTopicWithRetry(() -> topicManager.createUniqueTopic());
        createdTopics.add(topic);
        return topic;
    }
    
    /**
     * Helper method to create topic with specific partitions
     */
    @Step("Create test topic with {partitions} partitions")
    protected String createTestTopic(int partitions) {
        String topic = createTopicWithRetry(() -> topicManager.createTopicWithPartitions(partitions));
        createdTopics.add(topic);
        return topic;
    }
    
    /**
     * Create topic with retry mechanism for better reliability
     */
    private String createTopicWithRetry(java.util.function.Supplier<String> topicCreator) {
        int maxRetries = 5;  // Увеличено с 3 до 5
        RuntimeException lastException = null;
        
        for (int attempt = 1; attempt <= maxRetries; attempt++) {
            long backoff = 3000L * attempt; // Exponential backoff: 3s, 6s, 9s, 12s, 15s
            qa.autotest.framework.utils.RetryContext ctx = 
                new qa.autotest.framework.utils.RetryContext(attempt, backoff);
            
            try {
                String topic = topicCreator.get();
                ctx.markSuccess();
                ctx.attachToAllure("Topic Creation");
                
                if (attempt > 1) {
                    log.info("✓ Topic created successfully on attempt {}", attempt);
                }
                return topic;
            } catch (RuntimeException e) {
                lastException = e;
                ctx.markFailure(e);
                ctx.attachToAllure("Topic Creation");
                
                if (attempt < maxRetries) {
                    log.warn("Failed to create topic (attempt {}), retrying in {} ms: {}", 
                        attempt, backoff, e.getMessage());
                    AsyncTestHelper.waitFor((int)(backoff / 1000)); // Convert to seconds
                } else {
                    log.error("✗ Failed to create topic after {} attempts: {}", maxRetries, e.getMessage());
                }
            }
        }
        
        throw lastException;
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

    private void safeClose(AutoCloseable c, String name) {
        if (c == null) return;

        try {
            c.close();
            log.debug("{} closed", name);
        } catch (Exception e) {
            log.warn("Failed to close {}: {}", name, e.getMessage());
        }
    }

    private void safeRun(String name, Runnable action) {
        try {
            action.run();
        } catch (Exception e) {
            log.warn("Cleanup step '{}' failed: {}", name, e.getMessage());
        }
    }
}
