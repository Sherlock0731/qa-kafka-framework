package qa.autotest.framework.domain.port;

import java.util.List;

/**
 * Outbound Port: CleanupPort
 * <p>
 * Defines the contract for deleting Kafka topics from an external source of
 * truth (e.g. a cloud-provider management API).  The domain and application
 * layers depend only on this interface; concrete implementations live in the
 * Infrastructure layer.
 *
 * <h3>DIP rationale</h3>
 * Previously {@link qa.autotest.framework.infrastructure.KafkaTopicCleanupManager}
 * held a direct {@code new AivenApiController(config)} call, coupling the
 * application-level cleanup orchestration to a specific REST client
 * implementation.  Introducing this port inverts that dependency:
 * <ul>
 *   <li>{@code KafkaTopicCleanupManager} — depends on {@code CleanupPort} (domain)</li>
 *   <li>{@code AivenApiController}       — implements {@code CleanupPort} (infrastructure)</li>
 * </ul>
 * This also enables unit-testing {@code KafkaTopicCleanupManager} with a mock
 * implementation without any HTTP calls.
 *
 * <h3>Method design</h3>
 * The interface exposes only the three operations consumed by
 * {@code KafkaTopicCleanupManager}.  Low-level HTTP details (request specs,
 * status codes, DTO mapping) remain encapsulated in the adapter.
 */
public interface CleanupPort {

    /**
     * Returns the names of all topics currently present in the remote service.
     *
     * @return list of topic names; empty list on any error (never {@code null})
     */
    List<String> getTopicList();

    /**
     * Deletes a single topic from the remote service.
     *
     * @param topicName topic to delete; must not be {@code null}
     * @return {@code true} if the topic was deleted or did not exist;
     * {@code false} on a non-recoverable error
     */
    boolean deleteTopic(String topicName);

    /**
     * Deletes multiple topics from the remote service.
     * Implementations should continue after individual failures and report the
     * total number of successful deletions.
     *
     * @param topicNames topics to delete; must not be {@code null}
     * @return number of topics deleted successfully
     */
    int deleteTopics(List<String> topicNames);

    /**
     * Performs a lightweight connectivity check against the remote service.
     * Used by {@code KafkaTopicCleanupManager} to fast-fail before attempting
     * bulk deletions.
     *
     * @return {@code true} if the remote endpoint is reachable and authenticated
     */
    boolean verifyConnection();
}
