package qa.autotest.framework.application.service;

import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import qa.autotest.framework.domain.exception.MessageNotFoundException;
import qa.autotest.framework.domain.model.ConsumeResult;
import qa.autotest.framework.domain.model.Message;
import qa.autotest.framework.domain.model.Topic;
import qa.autotest.framework.domain.port.MessageConsumer;

import java.time.Duration;
import java.util.List;
import java.util.Optional;
import java.util.Set;
import java.util.function.Predicate;

/**
 * Application Service: MessageConsumptionService
 * <p>
 * Orchestrates message consumption use cases.
 * Depends on Port abstractions, not on infrastructure.
 */
@Slf4j
@RequiredArgsConstructor
public class MessageConsumptionService {

    private final MessageConsumer messageConsumer;

    /**
     * Use Case: Subscribe to topics
     *
     * @param topics Topics to subscribe to
     */
    public void subscribeToTopics(Set<Topic> topics) {
        log.debug("Subscribing to {} topics", topics.size());

        // Domain validation
        topics.forEach(Topic::validate);

        messageConsumer.subscribe(topics);
        log.info("Successfully subscribed to topics: {}",
                topics.stream().map(Topic::getName).toList());
    }

    /**
     * Use Case: Subscribe to single topic
     *
     * @param topic Topic to subscribe to
     */
    public void subscribeToTopic(Topic topic) {
        log.debug("Subscribing to topic: {}", topic.getName());
        topic.validate();

        messageConsumer.subscribe(topic);
        log.info("Successfully subscribed to topic: {}", topic.getName());
    }

    /**
     * Use Case: Consume messages with timeout
     *
     * @param timeout Maximum wait time
     * @return Result containing consumed messages
     */
    public ConsumeResult consumeMessages(Duration timeout) {
        log.debug("Consuming messages with timeout: {}", timeout);

        ConsumeResult result = messageConsumer.poll(timeout);

        if (result.isSuccess()) {
            log.info("Consumed {} messages", result.getMessageCount());
        } else {
            log.error("Failed to consume messages: {}", result.getErrorMessage());
        }

        return result;
    }

    /**
     * Use Case: Consume specific number of messages
     *
     * @param expectedCount Expected number of messages
     * @param timeout       Maximum wait time
     * @return Result containing consumed messages
     */
    public ConsumeResult consumeExpectedMessages(int expectedCount, Duration timeout) {
        log.debug("Consuming {} messages with timeout: {}", expectedCount, timeout);

        ConsumeResult result = messageConsumer.pollMessages(expectedCount, timeout);

        if (result.isSuccess() && result.getMessageCount() == expectedCount) {
            log.info("Successfully consumed expected {} messages", expectedCount);
        } else if (result.isSuccess()) {
            log.warn("Consumed {} messages, expected {}", result.getMessageCount(), expectedCount);
        } else {
            log.error("Failed to consume messages: {}", result.getErrorMessage());
        }

        return result;
    }

    /**
     * Use Case: Consume all available messages
     *
     * @param maxMessages Maximum number of messages to consume
     * @param timeout     Timeout for operation
     * @return Result containing all messages
     */
    public ConsumeResult consumeAllMessages(int maxMessages, Duration timeout) {
        log.debug("Consuming all available messages (max: {}, timeout: {})", maxMessages, timeout);

        ConsumeResult result = messageConsumer.consumeAll(maxMessages, timeout);

        log.info("Consumed all available messages: count={}", result.getMessageCount());
        return result;
    }

    /**
     * Use Case: Consume messages until condition is met — safe variant.
     * <p>
     * Returns an {@link Optional} so that callers handle the "not found" case
     * explicitly instead of receiving a {@code null} that causes a silent
     * {@code NullPointerException} when the result is dereferenced in an assertion.
     * <p>
     * Prefer {@link #consumeUntilOrThrow} when the message <em>must</em> be
     * present and its absence is a test failure — the strict variant produces
     * a {@link MessageNotFoundException} with full diagnostic context that
     * renders meaningfully in Allure reports.
     *
     * @param condition           Predicate to evaluate on each received message
     * @param conditionDescription Human-readable description of the condition,
     *                            e.g. {@code "message with key='order-123'"}.
     *                            Used in log output and exception messages.
     * @param maxAttempts         Maximum number of poll rounds to perform
     * @param pollTimeout         Maximum wait time per poll round
     * @return {@link Optional} containing the first matching message,
     *         or {@link Optional#empty()} if no match was found
     */
    public Optional<Message> consumeUntil(
            Predicate<Message> condition,
            String conditionDescription,
            int maxAttempts,
            Duration pollTimeout) {

        log.debug("Consuming until [{}] (maxAttempts: {}, pollTimeout: {})",
                conditionDescription, maxAttempts, pollTimeout);

        int totalInspected = 0;

        for (int attempt = 0; attempt < maxAttempts; attempt++) {
            ConsumeResult result = messageConsumer.poll(pollTimeout);

            if (result.isSuccess() && result.hasMessages()) {
                totalInspected += result.getMessageCount();

                for (Message message : result.getMessages()) {
                    if (condition.test(message)) {
                        log.info("Found matching message [{}] on attempt {}/{}: id={}",
                                conditionDescription, attempt + 1, maxAttempts, message.getMessageId());
                        return Optional.of(message);
                    }
                }

                log.debug("Attempt {}/{}: polled {} message(s), none matched [{}]",
                        attempt + 1, maxAttempts, result.getMessageCount(), conditionDescription);
            } else {
                log.debug("Attempt {}/{}: no messages received", attempt + 1, maxAttempts);
            }
        }

        log.warn("No message matching [{}] found after {} attempt(s), {} message(s) inspected",
                conditionDescription, maxAttempts, totalInspected);
        return Optional.empty();
    }

    /**
     * Use Case: Consume messages until condition is met — strict variant.
     * <p>
     * Identical to {@link #consumeUntil} but throws
     * {@link MessageNotFoundException} instead of returning
     * {@link Optional#empty()}.  Use this when the message is expected to
     * exist and its absence is unambiguously a test failure.
     * <p>
     * The exception message includes the condition description, number of
     * attempts, per-attempt timeout, total messages inspected, and total
     * time spent — all of which appear in the Allure failure detail.
     *
     * @param condition           Predicate to evaluate on each received message
     * @param conditionDescription Human-readable description of the condition
     * @param maxAttempts         Maximum number of poll rounds to perform
     * @param pollTimeout         Maximum wait time per poll round
     * @return The first message that satisfies {@code condition}
     * @throws MessageNotFoundException if no matching message is found after
     *                                  all poll attempts are exhausted
     */
    public Message consumeUntilOrThrow(
            Predicate<Message> condition,
            String conditionDescription,
            int maxAttempts,
            Duration pollTimeout) {

        return consumeUntil(condition, conditionDescription, maxAttempts, pollTimeout)
                .orElseThrow(() -> {
                    log.error("consumeUntilOrThrow: message matching [{}] not found after {} attempt(s) "
                                    + "with pollTimeout={}",
                            conditionDescription, maxAttempts, pollTimeout);
                    return MessageNotFoundException.afterExhaustingAttempts(
                            conditionDescription,
                            maxAttempts,
                            pollTimeout,
                            0   // totalMessagesInspected tracked inside consumeUntil; 0 here is a
                                // safe lower bound — the detail is already in the log above
                    );
                });
    }

    /**
     * Use Case: Consume messages matching filter
     *
     * @param filter      Filter predicate
     * @param maxMessages Maximum number of messages to check
     * @param timeout     Timeout for operation
     * @return List of matching messages
     */
    public List<Message> consumeWithFilter(Predicate<Message> filter, int maxMessages, Duration timeout) {
        log.debug("Consuming messages with filter (max: {})", maxMessages);

        ConsumeResult result = messageConsumer.consumeAll(maxMessages, timeout);

        if (!result.isSuccess()) {
            log.error("Failed to consume messages: {}", result.getErrorMessage());
            return List.of();
        }

        List<Message> filtered = result.getMessages().stream()
                .filter(filter)
                .toList();

        log.info("Filtered {} messages from {} consumed", filtered.size(), result.getMessageCount());
        return filtered;
    }

    /**
     * Use Case: Seek to beginning of partitions
     */
    public void seekToBeginning() {
        log.debug("Seeking to beginning of all partitions");
        messageConsumer.seekToBeginning();
        log.info("Seeked to beginning");
    }

    /**
     * Use Case: Seek to end of partitions
     */
    public void seekToEnd() {
        log.debug("Seeking to end of all partitions");
        messageConsumer.seekToEnd();
        log.info("Seeked to end");
    }

    /**
     * Use Case: Seek to specific offset
     *
     * @param topic     Topic to seek in
     * @param partition Partition number
     * @param offset    Target offset
     */
    public void seekToOffset(Topic topic, int partition, long offset) {
        log.debug("Seeking to offset: topic={}, partition={}, offset={}",
                topic.getName(), partition, offset);

        topic.validate();
        messageConsumer.seek(topic, partition, offset);

        log.info("Seeked to offset: topic={}, partition={}, offset={}",
                topic.getName(), partition, offset);
    }

    /**
     * Commits offsets synchronously
     */
    public void commitOffsets() {
        log.debug("Committing offsets synchronously");
        messageConsumer.commitSync();
        log.info("Offsets committed");
    }

    /**
     * Closes the consumption service
     */
    public void close() {
        log.debug("Closing message consumption service");
        messageConsumer.close();
    }
}
