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
 * <p>
 * Error classification uses the shared {@link KafkaErrorCategory} enum,
 * which replaces the former inner {@code ErrorCategory} and aligns
 * consume-side failures with produce-side failures under a single taxonomy.
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
     * Error category for failure classification.
     * Uses the shared {@link KafkaErrorCategory} — consume-specific values:
     * {@code DESERIALIZATION_ERROR}, {@code GROUP_COORDINATION_ERROR},
     * {@code OFFSET_OUT_OF_RANGE}.
     */
    KafkaErrorCategory errorCategory;

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
     * Business logic: checks if failure is retryable
     */
    public boolean isRetryable() {
        return !success && errorCategory != null && errorCategory.isRetryable();
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
    public static ConsumeResult failure(String errorMessage, KafkaErrorCategory category) {
        return ConsumeResult.builder()
                .success(false)
                .messages(List.of())
                .messageCount(0)
                .errorMessage(errorMessage)
                .errorCategory(category)
                .build();
    }

    /**
     * Factory: creates failed result by mapping the exception automatically.
     * Convenience overload — avoids the caller needing to call
     * {@link KafkaErrorCategory#fromConsumeException(Throwable)} explicitly.
     *
     * @param errorMessage human-readable description of the failure
     * @param exception    the root cause; drives category selection
     * @return failed {@code ConsumeResult} with category derived from the exception
     */
    public static ConsumeResult failureFrom(String errorMessage, Throwable exception) {
        return ConsumeResult.builder()
                .success(false)
                .messages(List.of())
                .messageCount(0)
                .errorMessage(errorMessage)
                .errorCategory(KafkaErrorCategory.fromConsumeException(exception))
                .build();
    }
}
