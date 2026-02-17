package qa.autotest.framework.domain.model;

import lombok.Builder;
import lombok.Singular;
import lombok.Value;

import java.util.List;
import java.util.Objects;

/**
 * Domain Result: ConsumeResult
 * <p>
 * Represents the outcome of a message consumption operation.
 */
@Value
@Builder
public class ConsumeResult {

    /**
     * Successfully consumed messages
     */
    @Singular
    List<Message> messages;

    /**
     * Success indicator
     */
    boolean success;

    /**
     * Number of messages consumed
     */
    int messageCount;

    /**
     * Topics consumed from
     */
    @Singular
    List<Topic> topics;

    /**
     * Consumer group used
     */
    ConsumerGroup consumerGroup;

    /**
     * Error message if consumption failed
     */
    String errorMessage;

    /**
     * Error category
     */
    ErrorCategory errorCategory;

    /**
     * Timeout occurred
     */
    boolean timeout;

    /**
     * Domain validation
     */
    public void validate() {
        if (success) {
            Objects.requireNonNull(messages, "Messages list cannot be null for successful consumption");
            if (messageCount != messages.size()) {
                throw new IllegalArgumentException(
                        String.format("Message count mismatch: count=%d, actual=%d", messageCount, messages.size())
                );
            }
        } else {
            Objects.requireNonNull(errorMessage, "Error message must be set for failed consumption");
        }
    }

    /**
     * Business logic: checks if any messages were consumed
     */
    public boolean hasMessages() {
        return messages != null && !messages.isEmpty();
    }

    /**
     * Business logic: checks if consumption was from specific topics
     */
    public boolean consumedFrom(Topic topic) {
        return topics != null && topics.contains(topic);
    }

    /**
     * Business logic: checks if result is empty (no messages)
     */
    public boolean isEmpty() {
        return !hasMessages();
    }

    /**
     * Business logic: checks if timeout occurred
     */
    public boolean isTimeout() {
        return timeout;
    }

    /**
     * Factory: creates successful result
     */
    public static ConsumeResult success(List<Message> messages) {
        return ConsumeResult.builder()
                .success(true)
                .messages(messages)
                .messageCount(messages.size())
                .build();
    }

    /**
     * Factory: creates empty result (no messages available)
     */
    public static ConsumeResult empty() {
        return ConsumeResult.builder()
                .success(true)
                .messages(List.of())
                .messageCount(0)
                .build();
    }

    /**
     * Factory: creates timeout result
     */
    public static ConsumeResult timeout() {
        return ConsumeResult.builder()
                .success(true)
                .messages(List.of())
                .messageCount(0)
                .timeout(true)
                .build();
    }

    /**
     * Factory: creates failed result
     */
    public static ConsumeResult failure(String errorMessage, ErrorCategory category) {
        return ConsumeResult.builder()
                .success(false)
                .messages(List.of())
                .messageCount(0)
                .errorMessage(errorMessage)
                .errorCategory(category)
                .build();
    }

    /**
     * Error categories for consumption failures
     */
    public enum ErrorCategory {
        NETWORK_ERROR(true),
        TIMEOUT_ERROR(true),
        DESERIALIZATION_ERROR(false),
        AUTHENTICATION_ERROR(false),
        AUTHORIZATION_ERROR(false),
        GROUP_COORDINATION_ERROR(true),
        OFFSET_OUT_OF_RANGE(false),
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
