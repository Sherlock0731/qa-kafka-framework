package qa.autotest.framework.domain.exceptions;

import java.time.Duration;

/**
 * Domain Exception: MessageNotFoundException
 * <p>
 * Thrown when {@code consumeUntil()} exhausts all poll attempts without
 * finding a message that satisfies the given predicate condition.
 * <p>
 * Replaces the previous {@code null} return, which caused silent
 * {@code NullPointerException} in test assertions instead of a descriptive
 * failure visible in Allure reports.
 * <p>
 * Usage in tests:
 * <pre>{@code
 * // Before (dangerous):
 * Message msg = facade.consumeUntil(m -> m.getKey().equals("order-123"), 10, Duration.ofSeconds(5));
 * assertThat(msg.getContent()).contains("CONFIRMED");  // NPE if not found — no context in Allure
 *
 * // After (explicit contract):
 * Optional<Message> msg = facade.consumeUntil(m -> m.getKey().equals("order-123"), 10, Duration.ofSeconds(5));
 * assertThat(msg).isPresent();
 * assertThat(msg.get().getContent()).contains("CONFIRMED");
 *
 * // Or with strict variant that throws:
 * Message msg = facade.consumeUntilOrThrow(m -> m.getKey().equals("order-123"), 10, Duration.ofSeconds(5));
 * assertThat(msg.getContent()).contains("CONFIRMED");  // AssertionError if not found, not NPE
 * }</pre>
 *
 * @see qa.autotest.framework.application.service.MessageConsumptionService#consumeUntil
 * @see qa.autotest.framework.application.service.MessageConsumptionService#consumeUntilOrThrow
 */
public class MessageNotFoundException extends RuntimeException {

    /**
     * Description of the predicate condition that was never satisfied.
     * Provided by the caller to make the failure message meaningful.
     */
    private final String conditionDescription;

    /**
     * Number of poll attempts exhausted before giving up.
     */
    private final int attemptsExhausted;

    /**
     * Timeout used per poll attempt.
     */
    private final Duration pollTimeout;

    /**
     * Total number of messages polled and inspected across all attempts.
     */
    private final int totalMessagesInspected;

    /**
     * @param conditionDescription  Human-readable description of the predicate
     *                              (e.g. "message with key='order-123'")
     * @param attemptsExhausted     How many poll rounds were performed
     * @param pollTimeout           Per-attempt poll timeout
     * @param totalMessagesInspected Total messages seen but none matched
     */
    public MessageNotFoundException(
            String conditionDescription,
            int attemptsExhausted,
            Duration pollTimeout,
            int totalMessagesInspected) {

        super(buildMessage(conditionDescription, attemptsExhausted, pollTimeout, totalMessagesInspected));
        this.conditionDescription = conditionDescription;
        this.attemptsExhausted = attemptsExhausted;
        this.pollTimeout = pollTimeout;
        this.totalMessagesInspected = totalMessagesInspected;
    }

    // ── Factory methods ────────────────────────────────────────────────────────

    /**
     * Creates an exception with full diagnostic context.
     *
     * @param conditionDescription  Human-readable condition that was never satisfied
     * @param attemptsExhausted     Total poll attempts made
     * @param pollTimeout           Timeout per poll attempt
     * @param totalMessagesInspected Total messages seen but not matching
     * @return New {@code MessageNotFoundException}
     */
    public static MessageNotFoundException afterExhaustingAttempts(
            String conditionDescription,
            int attemptsExhausted,
            Duration pollTimeout,
            int totalMessagesInspected) {

        return new MessageNotFoundException(
                conditionDescription,
                attemptsExhausted,
                pollTimeout,
                totalMessagesInspected
        );
    }

    /**
     * Creates an exception when no messages arrived at all during polling.
     *
     * @param conditionDescription Human-readable condition
     * @param attemptsExhausted    Total poll attempts made
     * @param pollTimeout          Timeout per poll attempt
     * @return New {@code MessageNotFoundException}
     */
    public static MessageNotFoundException noMessagesReceived(
            String conditionDescription,
            int attemptsExhausted,
            Duration pollTimeout) {

        return new MessageNotFoundException(conditionDescription, attemptsExhausted, pollTimeout, 0);
    }

    // ── Accessors ──────────────────────────────────────────────────────────────

    /**
     * Returns the human-readable description of the predicate condition.
     */
    public String getConditionDescription() {
        return conditionDescription;
    }

    /**
     * Returns the number of poll attempts that were made.
     */
    public int getAttemptsExhausted() {
        return attemptsExhausted;
    }

    /**
     * Returns the per-attempt poll timeout that was used.
     */
    public Duration getPollTimeout() {
        return pollTimeout;
    }

    /**
     * Returns the total number of messages that were inspected but did not match.
     */
    public int getTotalMessagesInspected() {
        return totalMessagesInspected;
    }

    /**
     * Calculates the total wall-clock time spent polling.
     */
    public Duration getTotalTimeSpent() {
        return pollTimeout.multipliedBy(attemptsExhausted);
    }

    // ── Private ────────────────────────────────────────────────────────────────

    private static String buildMessage(
            String conditionDescription,
            int attemptsExhausted,
            Duration pollTimeout,
            int totalMessagesInspected) {

        return String.format(
                "Message matching condition [%s] was not found after %d poll attempt(s). "
                        + "Poll timeout per attempt: %s. "
                        + "Total messages inspected: %d. "
                        + "Total time spent: %s.",
                conditionDescription,
                attemptsExhausted,
                pollTimeout,
                totalMessagesInspected,
                pollTimeout.multipliedBy(attemptsExhausted)
        );
    }
}
