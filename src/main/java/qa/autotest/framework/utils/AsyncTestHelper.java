package qa.autotest.framework.utils;

import lombok.extern.slf4j.Slf4j;
import org.awaitility.core.ConditionTimeoutException;
import qa.autotest.app.dto.ConsumerRecordDto;
import qa.autotest.framework.kafka.KafkaConsumerManager;

import java.time.Duration;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.Callable;
import java.util.concurrent.TimeUnit;

import static org.awaitility.Awaitility.await;

/**
 * Async Test Helper
 * Provides utilities for async testing without Thread.sleep()
 */
@Slf4j
public class AsyncTestHelper {
    
    /**
     * Poll messages from Kafka consumer with retry logic
     * This method keeps the consumer in the same thread to avoid ThreadLocal issues
     * 
     * @param consumer Kafka consumer manager
     * @param timeoutSeconds Maximum time to wait
     * @param expectedMinMessages Minimum expected messages
     * @return List of consumed records
     */
    public static List<ConsumerRecordDto> pollWithRetry(
            KafkaConsumerManager consumer, 
            int timeoutSeconds,
            int expectedMinMessages) {
        
        log.debug("Polling with retry: timeout={}s, expectedMin={}", timeoutSeconds, expectedMinMessages);
        
        List<ConsumerRecordDto> allRecords = new ArrayList<>();
        long startTime = System.currentTimeMillis();
        long timeoutMillis = timeoutSeconds * 1000L;
        
        // Дополнительное ожидание для remote Kafka (rebalancing, network latency)
        try {
            Thread.sleep(2000); // 2 секунды на rebalancing
        } catch (InterruptedException e) {
            Thread.currentThread().interrupt();
        }
        
        while (System.currentTimeMillis() - startTime < timeoutMillis) {
            List<ConsumerRecordDto> records = consumer.poll(5); // Увеличено с 2 до 5 секунд
            
            if (!records.isEmpty()) {
                allRecords.addAll(records);
                log.debug("Polled {} records, total: {}", records.size(), allRecords.size());
                
                if (allRecords.size() >= expectedMinMessages) {
                    log.info("Successfully polled {} records (expected min: {})", 
                            allRecords.size(), expectedMinMessages);
                    return allRecords;
                }
            }
            
            // Small sleep between polls - увеличено для remote Kafka
            try {
                Thread.sleep(1000); // Увеличено с 500ms до 1000ms
            } catch (InterruptedException e) {
                Thread.currentThread().interrupt();
                throw new RuntimeException("Interrupted while polling", e);
            }
        }
        
        log.warn("Timeout reached. Polled {} records, expected min: {}", 
                allRecords.size(), expectedMinMessages);
        return allRecords;
    }
    
    /**
     * Poll all available messages from Kafka consumer with retry logic
     * 
     * @param consumer Kafka consumer manager
     * @param timeoutSeconds Maximum time to wait
     * @return List of all consumed records
     */
    public static List<ConsumerRecordDto> pollAllWithRetry(
            KafkaConsumerManager consumer,
            int timeoutSeconds) {
        
        return pollWithRetry(consumer, timeoutSeconds, 1);
    }
    
    /**
     * Waits until condition is met
     */
    public static <T> T awaitCondition(Callable<T> condition, int timeoutSeconds) {
        try {
            return await()
                    .atMost(Duration.ofSeconds(timeoutSeconds))
                    .pollInterval(Duration.ofMillis(500))
                    .until(condition, result -> result != null);
        } catch (ConditionTimeoutException e) {
            log.error("Condition timeout after {} seconds", timeoutSeconds);
            throw e;
        }
    }
    
    /**
     * Waits until boolean condition is true
     */
    public static void awaitCondition(Callable<Boolean> condition, int timeoutSeconds, String description) {
        try {
            await()
                    .atMost(timeoutSeconds, TimeUnit.SECONDS)
                    .pollInterval(500, TimeUnit.MILLISECONDS)
                    .until(condition);
            log.debug("Condition met: {}", description);
        } catch (Exception e) {
            log.error("Condition not met after {} seconds: {}", timeoutSeconds, description);
            throw new RuntimeException("Condition not met: " + description, e);
        }
    }
}
