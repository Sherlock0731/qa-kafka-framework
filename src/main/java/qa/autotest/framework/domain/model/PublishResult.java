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
     * Error category for failure classification
     */
    ErrorCategory errorCategory;

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
    public static PublishResult failure(Message message, String errorMessage, ErrorCategory category) {
        return PublishResult.builder()
                .message(message)
                .success(false)
                .errorMessage(errorMessage)
                .errorCategory(category)
                .build();
    }

    /**
     * Factory: creates failed result with exception
     */
    public static PublishResult failure(Message message, String errorMessage, ErrorCategory category, Throwable exception) {
        return PublishResult.builder()
                .message(message)
                .success(false)
                .errorMessage(errorMessage)
                .errorCategory(category)
                .exception(exception)
                .build();
    }

    /**
     * Error categories for failure classification
     */
    public enum ErrorCategory {
        NETWORK_ERROR(true),
        TIMEOUT_ERROR(true),
        SERIALIZATION_ERROR(false),
        AUTHENTICATION_ERROR(false),
        AUTHORIZATION_ERROR(false),
        TOPIC_NOT_FOUND(false),
        BROKER_NOT_AVAILABLE(true),
        BUFFER_EXHAUSTED(true),
        UNKNOWN_ERROR(true);

        private final boolean retryable;

        ErrorCategory(boolean retryable) {
            this.retryable = retryable;
        }

        public boolean isRetryable() {
            return retryable;
        }
    }
}
