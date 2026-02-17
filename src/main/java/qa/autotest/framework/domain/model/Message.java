package qa.autotest.framework.domain.model;

import lombok.Builder;
import lombok.Value;
import lombok.With;

import java.time.Instant;
import java.util.Map;
import java.util.Objects;
import java.util.UUID;

/**
 * Domain Entity: Message
 * <p>
 * Pure business object representing a message in the messaging system.
 * No dependencies on infrastructure (Kafka, HTTP, etc.)
 * <p>
 * Following Domain-Driven Design principles:
 * - Immutable value object
 * - Rich domain model with business validation
 * - Self-contained business logic
 */
@Value
@Builder(toBuilder = true)
public class Message {

    /**
     * Unique identifier for this message
     */
    @Builder.Default
    String messageId = UUID.randomUUID().toString();

    /**
     * Business key for message partitioning
     * Can be null for round-robin partitioning
     */
    String key;

    /**
     * Message payload
     */
    String content;

    /**
     * Topic where message should be sent
     */
    Topic topic;

    /**
     * Optional explicit partition for routing
     */
    @With
    Integer partition;

    /**
     * Message metadata/headers
     */
    @Builder.Default
    Map<String, String> headers = Map.of();

    /**
     * Timestamp when message was created
     */
    @Builder.Default
    Instant timestamp = Instant.now();

    /**
     * Correlation ID for distributed tracing
     */
    String correlationId;

    /**
     * Event type for event-driven architectures
     */
    String eventType;

    /**
     * Domain validation: ensures message has required fields
     *
     * @throws IllegalArgumentException if validation fails
     */
    public void validate() {
        Objects.requireNonNull(messageId, "Message ID cannot be null");
        Objects.requireNonNull(topic, "Topic cannot be null");
        Objects.requireNonNull(content, "Content cannot be null");
        Objects.requireNonNull(timestamp, "Timestamp cannot be null");

        if (content.isEmpty()) {
            throw new IllegalArgumentException("Content cannot be empty");
        }

        topic.validate();
    }

    /**
     * Business logic: checks if message is for a specific partition
     */
    public boolean hasExplicitPartition() {
        return partition != null;
    }

    /**
     * Business logic: checks if message has correlation ID
     */
    public boolean isCorrelated() {
        return correlationId != null && !correlationId.isBlank();
    }

    /**
     * Business logic: checks if message is an event
     */
    public boolean isEvent() {
        return eventType != null && !eventType.isBlank();
    }

    /**
     * Business logic: checks if message has custom headers
     */
    public boolean hasHeaders() {
        return headers != null && !headers.isEmpty();
    }

    /**
     * Business logic: gets header value
     */
    public String getHeader(String key) {
        return headers != null ? headers.get(key) : null;
    }

    /**
     * Creates a new message with updated content
     */
    public Message withContent(String newContent) {
        return toBuilder().content(newContent).build();
    }

    /**
     * Creates a new message with added header
     */
    public Message withHeader(String key, String value) {
        Map<String, String> newHeaders = new java.util.HashMap<>(headers);
        newHeaders.put(key, value);
        return toBuilder().headers(Map.copyOf(newHeaders)).build();
    }

    /**
     * Creates a new message with correlation ID
     */
    public Message withCorrelation(String correlationId) {
        return toBuilder().correlationId(correlationId).build();
    }
}
