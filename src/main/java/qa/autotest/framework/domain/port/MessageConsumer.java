package qa.autotest.framework.domain.port;

import qa.autotest.framework.domain.model.ConsumeResult;
import qa.autotest.framework.domain.model.ConsumerGroup;
import qa.autotest.framework.domain.model.Message;
import qa.autotest.framework.domain.model.Topic;

import java.time.Duration;
import java.util.List;
import java.util.Set;
import java.util.Map;

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
     * Commits a specific offset for a single partition synchronously.
     * <p>
     * Uses {@code consumer.commitSync(Map)} — the only Kafka-correct way to
     * commit an explicit offset without calling {@code poll()} first.
     * The committed offset must be {@code lastConsumedOffset + 1} so that
     * the next fetch starts at the record <em>after</em> the one just processed.
     *
     * @param topic     topic the offset belongs to
     * @param partition partition number (0-based)
     * @param offset    offset of the <strong>last consumed</strong> record;
     *                  the committed position will be {@code offset + 1}
     */
    void commitSync(Topic topic, int partition, long offset);

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
