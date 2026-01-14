package qa.autotest.app.dto;

import com.fasterxml.jackson.annotation.JsonInclude;
import lombok.AllArgsConstructor;
import lombok.Builder;
import lombok.Data;
import lombok.NoArgsConstructor;

import java.time.Instant;
import java.util.Map;
import java.util.UUID;

/**
 * Data Transfer Object for Kafka messages
 * Represents a message sent to or received from Kafka
 */
@Data
@Builder
@NoArgsConstructor
@AllArgsConstructor
@JsonInclude(JsonInclude.Include.NON_NULL)
public class KafkaMessageDto {
    
    /**
     * Unique message identifier (for idempotence testing)
     */
    @Builder.Default
    private String messageId = UUID.randomUUID().toString();
    
    /**
     * Message key for partitioning
     */
    private String key;
    
    /**
     * Message payload
     */
    private String value;
    
    /**
     * Message headers
     */
    private Map<String, String> headers;
    
    /**
     * Target topic
     */
    private String topic;
    
    /**
     * Target partition (optional, can be null for automatic partitioning)
     */
    private Integer partition;
    
    /**
     * Message timestamp
     */
    @Builder.Default
    private Instant timestamp = Instant.now();
    
    /**
     * Offset after message is sent (populated after send)
     */
    private Long offset;
    
    /**
     * Event type for event-driven testing
     */
    private String eventType;
    
    /**
     * Correlation ID for tracing
     */
    private String correlationId;
    
    /**
     * Retry count for error handling testing
     */
    @Builder.Default
    private Integer retryCount = 0;
    
    /**
     * Original topic (for DLQ testing)
     */
    private String originalTopic;
    
    /**
     * Error message (for DLQ testing)
     */
    private String errorMessage;

    /**
     * Add a single header to the message
     * Creates headers map if it doesn't exist
     */
    public void addHeader(String key, String value) {
        if (this.headers == null) {
            this.headers = new java.util.HashMap<>();
        }
        this.headers.put(key, value);
    }
}
