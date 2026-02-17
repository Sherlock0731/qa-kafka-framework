package qa.autotest.framework.domain.port;

import qa.autotest.framework.domain.model.ConsumeResult;
import qa.autotest.framework.domain.model.ConsumerGroup;
import qa.autotest.framework.domain.model.Message;
import qa.autotest.framework.domain.model.Topic;

import java.time.Duration;
import java.util.List;
import java.util.Set;

/**
 * Port Interface: MessageConsumer (Outbound Port)
 * <p>
 * Defines the contract for consuming messages from the messaging system.
 * Infrastructure adapters (KafkaConsumerAdapter) will implement this interface.
 */
public interface MessageConsumer {

    /**
     * Subscribes to topics
     *
     * @param topics Topics to subscribe to
     */
    void subscribe(Set<Topic> topics);

    /**
     * Subscribes to a single topic
     *
     * @param topic Topic to subscribe to
     */
    void subscribe(Topic topic);

    /**
     * Polls for messages with timeout
     *
     * @param timeout Maximum time to wait for messages
     * @return Result containing consumed messages
     */
    ConsumeResult poll(Duration timeout);

    /**
     * Polls for a specific number of messages
     *
     * @param expectedCount Expected number of messages
     * @param timeout       Maximum time to wait
     * @return Result containing consumed messages
     */
    ConsumeResult pollMessages(int expectedCount, Duration timeout);

    /**
     * Consumes all available messages from subscribed topics
     *
     * @param maxMessages Maximum number of messages to consume
     * @param timeout     Timeout for operation
     * @return Result containing all consumed messages
     */
    ConsumeResult consumeAll(int maxMessages, Duration timeout);

    /**
     * Seeks to a specific offset in a partition
     *
     * @param topic     Topic to seek in
     * @param partition Partition number
     * @param offset    Target offset
     */
    void seek(Topic topic, int partition, long offset);

    /**
     * Seeks to beginning of all partitions
     */
    void seekToBeginning();

    /**
     * Seeks to end of all partitions
     */
    void seekToEnd();

    /**
     * Commits current offsets synchronously
     */
    void commitSync();

    /**
     * Commits current offsets asynchronously
     */
    void commitAsync();

    /**
     * Gets the consumer group this consumer belongs to
     *
     * @return Consumer group information
     */
    ConsumerGroup getConsumerGroup();

    /**
     * Closes the consumer and releases resources
     */
    void close();
}
