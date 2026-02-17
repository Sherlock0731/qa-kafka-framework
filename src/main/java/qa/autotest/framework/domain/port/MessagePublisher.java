package qa.autotest.framework.domain.port;

import qa.autotest.framework.domain.model.Message;
import qa.autotest.framework.domain.model.PublishResult;

import java.util.List;
import java.util.concurrent.CompletableFuture;

/**
 * Port Interface: MessagePublisher (Outbound Port)
 * <p>
 * Defines the contract for publishing messages to the messaging system.
 * This is an interface at the boundary of the hexagon.
 * <p>
 * Infrastructure adapters (KafkaProducerAdapter) will implement this interface.
 * Application services will depend on this abstraction, not on concrete implementations.
 * <p>
 * Following Dependency Inversion Principle:
 * - High-level policy (Application) depends on abstraction (Port)
 * - Low-level details (Infrastructure) depends on abstraction (Port)
 */
public interface MessagePublisher {

    /**
     * Publishes a message synchronously
     *
     * @param message Message to publish
     * @return Result of the publication
     */
    PublishResult publish(Message message);

    /**
     * Publishes a message asynchronously
     *
     * @param message Message to publish
     * @return Future containing the result
     */
    CompletableFuture<PublishResult> publishAsync(Message message);

    /**
     * Publishes multiple messages synchronously
     *
     * @param messages Messages to publish
     * @return List of results for each message
     */
    List<PublishResult> publishBatch(List<Message> messages);

    /**
     * Publishes multiple messages asynchronously
     *
     * @param messages Messages to publish
     * @return Future containing list of results
     */
    CompletableFuture<List<PublishResult>> publishBatchAsync(List<Message> messages);

    /**
     * Flushes any pending messages
     * Ensures all buffered messages are sent
     */
    void flush();

    /**
     * Closes the publisher and releases resources
     */
    void close();
}
