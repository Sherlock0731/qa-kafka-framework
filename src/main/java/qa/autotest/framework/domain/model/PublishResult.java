package qa.autotest.framework.domain.model;

import lombok.Builder;
import lombok.Value;

import java.time.Instant;
import java.util.Objects;

/**
 * Domain Result: PublishResult
 * <p>
 * Represents the outcome of a message publication.
 * Following Result pattern for explicit error handling.
 * <p>
 * Error classification uses the shared {@link KafkaErrorCategory} enum,
 * which replaces the former inner {@code ErrorCategory} and aligns
 * produce-side failures with consume-side failures under a single taxonomy.
 */
@Value
@Builder
public class PublishResult {

    /**
     * Original message that was published
     */
    Message message;

    /**
     * Success indicator
     */
    boolean success;

    /**
     * Partition where message was stored
     */
    Integer partition;

    /**
     * Offset within the partition
     */
    Long offset;

    /**
     * Timestamp when message was stored
     */
    Long timestamp;

    /**
     * Error message if publication failed
     */
    String errorMessage;

    /**
     * Error category for failure classification.
     * Uses the shared {@link KafkaErrorCategory} — produce-specific values:
     * {@code SERIALIZATION_ERROR}, {@code TOPIC_NOT_FOUND},
     * {@code BROKER_NOT_AVAILABLE}, {@code BUFFER_EXHAUSTED}.
     */
    KafkaErrorCategory errorCategory;

    /**
     * Exception if publication failed
     */
    Throwable exception;

    /**
     * Domain validation
     */
    public void validate() {
        Objects.requireNonNull(message, "Message cannot be null");

        if (success) {
            Objects.requireNonNull(partition, "Partition must be set for successful publication");
            Objects.requireNonNull(offset, "Offset must be set for successful publication");
        } else {
            Objects.requireNonNull(errorMessage, "Error message must be set for failed publication");
            Objects.requireNonNull(errorCategory, "Error category must be set for failed publication");
        }
    }

    /**
     * Business logic: checks if result represents a successful publication
     */
    public boolean isSuccess() {
        return success;
    }

    /**
     * Business logic: checks if failure is retryable
     */
    public boolean isRetryable() {
        return !success && errorCategory != null && errorCategory.isRetryable();
    }

    /**
     * Business logic: gets partition information
     */
    public Partition getPartitionInfo() {
        if (!success || partition == null || message.getTopic() == null) {
            return null;
        }

        return Partition.builder()
                .topic(message.getTopic())
                .partitionNumber(partition)
                .currentOffset(offset)
                .endOffset(offset)
                .build();
    }

    /**
     * Business logic: checks if publication was to specific partition
     */
    public boolean wasTargetedPublish() {
        return success && message.hasExplicitPartition();
    }

    // ── Factory methods ────────────────────────────────────────────────────────

    /**
     * Factory: creates successful result
     */
    public static PublishResult success(Message message, int partition, long offset, long timestamp) {
        return PublishResult.builder()
                .message(message)
                .success(true)
                .partition(partition)
                .offset(offset)
                .timestamp(timestamp)
                .build();
    }

    /**
     * Factory: creates failed result
     */
    public static PublishResult failure(Message message, String errorMessage, KafkaErrorCategory category) {
        return PublishResult.builder()
                .message(message)
                .success(false)
                .errorMessage(errorMessage)
                .errorCategory(category)
                .build();
    }

    /**
     * Factory: creates failed result with exception.
     * Uses {@link KafkaErrorCategory#fromPublishException(Throwable)} for
     * automatic exception-to-category mapping.
     */
    public static PublishResult failure(Message message, String errorMessage, KafkaErrorCategory category, Throwable exception) {
        return PublishResult.builder()
                .message(message)
                .success(false)
                .errorMessage(errorMessage)
                .errorCategory(category)
                .exception(exception)
                .build();
    }

    /**
     * Factory: creates failed result by mapping the exception automatically.
     * Convenience overload — avoids the caller needing to call
     * {@link KafkaErrorCategory#fromPublishException(Throwable)} explicitly.
     *
     * @param message      the message that failed to publish
     * @param errorMessage human-readable description of the failure
     * @param exception    the root cause; drives category selection
     * @return failed {@code PublishResult} with category derived from the exception
     */
    public static PublishResult failureFrom(Message message, String errorMessage, Throwable exception) {
        return PublishResult.builder()
                .message(message)
                .success(false)
                .errorMessage(errorMessage)
                .errorCategory(KafkaErrorCategory.fromPublishException(exception))
                .exception(exception)
                .build();
    }
}
