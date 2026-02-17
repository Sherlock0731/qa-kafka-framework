package qa.autotest.framework.application.service;

import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import qa.autotest.framework.domain.model.Message;
import qa.autotest.framework.domain.model.PublishResult;
import qa.autotest.framework.domain.port.MessagePublisher;

import java.util.List;
import java.util.concurrent.CompletableFuture;

/**
 * Application Service: MessagePublishingService
 * <p>
 * Orchestrates message publishing use cases.
 * Depends on Port abstractions, not on infrastructure implementations.
 * <p>
 * Following Hexagonal Architecture:
 * - Application layer orchestrates domain logic
 * - Depends on domain ports (interfaces)
 * - No dependencies on infrastructure
 */
@Slf4j
@RequiredArgsConstructor
public class MessagePublishingService {

    private final MessagePublisher messagePublisher;

    /**
     * Use Case: Publish a single message
     *
     * @param message Message to publish
     * @return Result of publication
     */
    public PublishResult publishMessage(Message message) {
        log.debug("Publishing message: id={}, topic={}", message.getMessageId(), message.getTopic().getName());

        // Domain validation
        message.validate();

        // Delegate to port
        PublishResult result = messagePublisher.publish(message);

        if (result.isSuccess()) {
            log.info("Message published successfully: id={}, partition={}, offset={}",
                    message.getMessageId(), result.getPartition(), result.getOffset());
        } else {
            log.error("Failed to publish message: id={}, error={}",
                    message.getMessageId(), result.getErrorMessage());
        }

        return result;
    }

    /**
     * Use Case: Publish message asynchronously
     *
     * @param message Message to publish
     * @return Future with result
     */
    public CompletableFuture<PublishResult> publishMessageAsync(Message message) {
        log.debug("Publishing message asynchronously: id={}, topic={}",
                message.getMessageId(), message.getTopic().getName());

        // Domain validation
        message.validate();

        // Delegate to port
        return messagePublisher.publishAsync(message)
                .thenApply(result -> {
                    if (result.isSuccess()) {
                        log.info("Message published asynchronously: id={}, partition={}, offset={}",
                                message.getMessageId(), result.getPartition(), result.getOffset());
                    } else {
                        log.error("Failed to publish message asynchronously: id={}, error={}",
                                message.getMessageId(), result.getErrorMessage());
                    }
                    return result;
                });
    }

    /**
     * Use Case: Publish multiple messages
     *
     * @param messages Messages to publish
     * @return List of results
     */
    public List<PublishResult> publishBatch(List<Message> messages) {
        log.debug("Publishing batch of {} messages", messages.size());

        // Domain validation for all messages
        messages.forEach(Message::validate);

        // Delegate to port
        List<PublishResult> results = messagePublisher.publishBatch(messages);

        long successCount = results.stream().filter(PublishResult::isSuccess).count();
        log.info("Batch publish completed: total={}, success={}, failed={}",
                messages.size(), successCount, messages.size() - successCount);

        return results;
    }

    /**
     * Use Case: Publish with retry logic
     *
     * @param message    Message to publish
     * @param maxRetries Maximum number of retries
     * @return Final result
     */
    public PublishResult publishWithRetry(Message message, int maxRetries) {
        log.debug("Publishing message with retry: id={}, maxRetries={}",
                message.getMessageId(), maxRetries);

        message.validate();

        PublishResult result = null;
        int attempt = 0;

        while (attempt <= maxRetries) {
            result = messagePublisher.publish(message);

            if (result.isSuccess()) {
                log.info("Message published on attempt {}: id={}", attempt + 1, message.getMessageId());
                return result;
            }

            if (!result.isRetryable()) {
                log.error("Message publication failed with non-retryable error: id={}, error={}",
                        message.getMessageId(), result.getErrorMessage());
                return result;
            }

            attempt++;
            if (attempt <= maxRetries) {
                log.warn("Retrying message publication (attempt {}/{}): id={}",
                        attempt, maxRetries, message.getMessageId());

                try {
                    Thread.sleep(1000L * attempt); // Exponential backoff
                } catch (InterruptedException e) {
                    Thread.currentThread().interrupt();
                    break;
                }
            }
        }

        log.error("Message publication failed after {} attempts: id={}",
                maxRetries + 1, message.getMessageId());
        return result;
    }

    /**
     * Flushes pending messages
     */
    public void flush() {
        log.debug("Flushing pending messages");
        messagePublisher.flush();
    }

    /**
     * Closes the publishing service
     */
    public void close() {
        log.debug("Closing message publishing service");
        messagePublisher.close();
    }
}
