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
 * Utility class for generating test data for Kafka messages.
 * 
 * <p>Provides thread-safe methods for creating test messages with unique identifiers
 * and metadata. All methods are static and support parallel test execution.</p>
 * 
 * <p><b>Thread Safety:</b> All methods are thread-safe. Each invocation generates
 * unique UUIDs and captures current thread context.</p>
 * 
 * <p><b>Usage Example:</b></p>
 * <pre>{@code
 * // Single message
 * KafkaMessageDto message = TestDataGenerator.generateMessage("my-topic");
 * 
 * // Message with specific key
 * KafkaMessageDto keyedMessage = TestDataGenerator.generateMessageWithKey("topic", "user-123");
 * 
 * // Batch of messages
 * List<KafkaMessageDto> batch = TestDataGenerator.generateMessages("topic", 100);
 * }</pre>
 * 
 * @see KafkaMessageDto
 * @since 1.0
 */
@Slf4j
public class TestDataGenerator {

    /**
     * ObjectMapper for JSON serialization, configured with JavaTimeModule.
     * Thread-safe after configuration.
     */
    private static final ObjectMapper OBJECT_MAPPER = new ObjectMapper()
            .registerModule(new JavaTimeModule());

    /**
     * Private constructor to prevent instantiation of utility class.
     * 
     * @throws UnsupportedOperationException always
     */
    private TestDataGenerator() {
        throw new UnsupportedOperationException("Utility class");
    }

    /**
     * Generates a test message with random key.
     * 
     * <p>Creates a message with unique ID, random key, JSON payload, and standard headers.
     * Thread-safe for parallel execution.</p>
     * 
     * @param topic the target Kafka topic
     * @return a new message with unique identifiers
     * @throws NullPointerException if topic is null
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
     * Generates a test message with specific key for partition routing.
     * 
     * <p>Messages with the same key are routed to the same partition, maintaining order.
     * Use this for testing partition routing and message ordering.</p>
     * 
     * @param topic the target Kafka topic
     * @param key the message key for partition routing (can be null)
     * @return a new message with the specified key
     * @throws NullPointerException if topic is null
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
     * Generates multiple test messages in a batch.
     * 
     * <p>Each message has unique identifiers. All messages are loaded into memory.
     * For large batches (&gt;10,000), consider memory constraints.</p>
     * 
     * @param topic the target Kafka topic
     * @param count the number of messages to generate (must be &gt;= 0)
     * @return a list of unique messages (empty if count is 0)
     * @throws NullPointerException if topic is null
     * @throws IllegalArgumentException if count is negative
     */
    public static List<KafkaMessageDto> generateMessages(String topic, int count) {
        if (count < 0) {
            throw new IllegalArgumentException("Count must be non-negative, got: " + count);
        }

        List<KafkaMessageDto> messages = new ArrayList<>(count);
        for (int i = 0; i < count; i++) {
            messages.add(generateMessage(topic));
        }
        return messages;
    }

    /**
     * Generates JSON payload with test metadata.
     * 
     * <p>Creates JSON with id, timestamp, payload, and thread name.
     * Returns error JSON if serialization fails.</p>
     * 
     * @return JSON string with test data
     */
    private static String generateJsonValue() {
        Map<String, Object> data = new HashMap<>();
        data.put("id", UUID.randomUUID().toString());
        data.put("timestamp", Instant.now().toString());
        data.put("payload", "test-data-" + System.currentTimeMillis());
        data.put("thread", Thread.currentThread().getName());
        
        try {
            return OBJECT_MAPPER.writeValueAsString(data);
        } catch (JsonProcessingException e) {
            log.error("Failed to generate JSON: {}", e.getMessage());
            return "{\"error\":\"failed to generate\"}";
        }
    }

    /**
     * Generates standard headers with tracing metadata.
     * 
     * <p>Includes trace-id (UUID), thread-id (thread name), and source identifier.
     * Headers enable distributed tracing and parallel test debugging.</p>
     * 
     * @return map of standard headers
     */
    private static Map<String, String> generateHeaders() {
        Map<String, String> headers = new HashMap<>();
        String traceId = UUID.randomUUID().toString();
        String threadName = Thread.currentThread().getName();
        
        headers.put("trace-id", traceId);
        headers.put("thread-id", threadName);
        headers.put("source", "qa-test-framework");
        
        return headers;
    }
}
