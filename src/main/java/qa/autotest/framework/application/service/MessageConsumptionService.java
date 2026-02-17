package qa.autotest.framework.application.service;

import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import qa.autotest.framework.domain.model.ConsumeResult;
import qa.autotest.framework.domain.model.Message;
import qa.autotest.framework.domain.model.Topic;
import qa.autotest.framework.domain.port.MessageConsumer;

import java.time.Duration;
import java.util.ArrayList;
import java.util.List;
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
     * Use Case: Consume messages until condition is met
     *
     * @param condition   Condition to check for each message
     * @param maxAttempts Maximum number of poll attempts
     * @param pollTimeout Timeout for each poll
     * @return First message matching the condition
     */
    public Message consumeUntil(Predicate<Message> condition, int maxAttempts, Duration pollTimeout) {
        log.debug("Consuming messages until condition is met (maxAttempts: {})", maxAttempts);

        for (int attempt = 0; attempt < maxAttempts; attempt++) {
            ConsumeResult result = messageConsumer.poll(pollTimeout);

            if (result.isSuccess() && result.hasMessages()) {
                for (Message message : result.getMessages()) {
                    if (condition.test(message)) {
                        log.info("Found matching message on attempt {}: id={}",
                                attempt + 1, message.getMessageId());
                        return message;
                    }
                }
            }
        }

        log.warn("No matching message found after {} attempts", maxAttempts);
        return null;
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
