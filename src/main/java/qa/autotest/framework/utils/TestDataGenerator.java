package qa.autotest.framework.utils;

import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.datatype.jsr310.JavaTimeModule;
import lombok.extern.slf4j.Slf4j;
import qa.autotest.app.dto.KafkaMessageDto;

import java.time.Instant;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.UUID;

/**
 * Test Data Generator
 * Generates test data for Kafka messages with thread-safe parallel execution support
 */
@Slf4j
public class TestDataGenerator {
    
    private static final ObjectMapper OBJECT_MAPPER = new ObjectMapper()
            .registerModule(new JavaTimeModule());
    
    /**
     * Generates a unique message for testing
     * Thread-safe: uses per-thread isolation via thread name in headers
     */
    public static KafkaMessageDto generateMessage(String topic) {
        return KafkaMessageDto.builder()
                .messageId(UUID.randomUUID().toString())
                .topic(topic)
                .key("key-" + UUID.randomUUID())
                .value(generateJsonValue())
                .timestamp(Instant.now())
                .headers(generateHeaders())
                .build();
    }
    
    /**
     * Generates message with specific key
     * Thread-safe: key provided by caller ensures deterministic partitioning per test
     */
    public static KafkaMessageDto generateMessageWithKey(String topic, String key) {
        return KafkaMessageDto.builder()
                .messageId(UUID.randomUUID().toString())
                .topic(topic)
                .key(key)
                .value(generateJsonValue())
                .timestamp(Instant.now())
                .headers(generateHeaders())
                .build();
    }
    
    /**
     * Generates multiple messages
     * Thread-safe: each invocation produces isolated UUIDs
     */
    public static List<KafkaMessageDto> generateMessages(String topic, int count) {
        List<KafkaMessageDto> messages = new ArrayList<>();
        for (int i = 0; i < count; i++) {
            messages.add(generateMessage(topic));
        }
        return messages;
    }
    
    /**
     * Generates JSON value with thread context
     */
    private static String generateJsonValue() {
        Map<String, Object> data = new HashMap<>();
        data.put("id", UUID.randomUUID().toString());
        data.put("timestamp", Instant.now().toString());
        data.put("payload", "test-data-" + System.currentTimeMillis());
        data.put("thread", Thread.currentThread().getName()); // Parallel execution tracking
        
        try {
            return OBJECT_MAPPER.writeValueAsString(data);
        } catch (JsonProcessingException e) {
            log.error("Failed to generate JSON: {}", e.getMessage());
            return "{\\\"error\\\":\\\"failed to generate\\\"}";
        }
    }
    
    /**
     * Generates standard headers with thread isolation
     */
    private static Map<String, String> generateHeaders() {
        Map<String, String> headers = new HashMap<>();
        String traceId = UUID.randomUUID().toString();
        String threadName = Thread.currentThread().getName();
        
        // Add thread context for parallel test isolation and debugging
        headers.put("trace-id", traceId);
        headers.put("thread-id", threadName);
        headers.put("source", "qa-test-framework");
        
        return headers;
    }
}
