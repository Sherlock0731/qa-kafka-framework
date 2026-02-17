package qa.autotest.framework.domain.port;

import qa.autotest.framework.domain.model.Partition;
import qa.autotest.framework.domain.model.Topic;

import java.util.List;
import java.util.Set;

/**
 * Port Interface: TopicRepository (Outbound Port)
 * <p>
 * Defines the contract for topic management operations.
 * Infrastructure adapters (KafkaAdminAdapter) will implement this interface.
 * <p>
 * Following Repository pattern from DDD.
 */
public interface TopicRepository {

    /**
     * Creates a new topic
     *
     * @param topic Topic to create
     * @return true if created successfully
     */
    boolean createTopic(Topic topic);

    /**
     * Creates multiple topics
     *
     * @param topics Topics to create
     * @return Number of topics created successfully
     */
    int createTopics(Set<Topic> topics);

    /**
     * Deletes a topic
     *
     * @param topic Topic to delete
     * @return true if deleted successfully
     */
    boolean deleteTopic(Topic topic);

    /**
     * Deletes multiple topics
     *
     * @param topics Topics to delete
     * @return Number of topics deleted successfully
     */
    int deleteTopics(Set<Topic> topics);

    /**
     * Checks if a topic exists
     *
     * @param topic Topic to check
     * @return true if topic exists
     */
    boolean exists(Topic topic);

    /**
     * Gets all existing topics
     *
     * @return Set of all topics
     */
    Set<Topic> getAllTopics();

    /**
     * Gets topics matching a pattern
     *
     * @param pattern Pattern to match (e.g., "test-*")
     * @return Set of matching topics
     */
    Set<Topic> getTopicsByPattern(String pattern);

    /**
     * Gets partition information for a topic
     *
     * @param topic Topic to get partitions for
     * @return List of partitions
     */
    List<Partition> getPartitions(Topic topic);

    /**
     * Gets detailed information about a topic
     *
     * @param topicName Topic name
     * @return Topic with full configuration
     */
    Topic getTopicDetails(String topicName);

    /**
     * Waits for a topic to be created and available
     *
     * @param topic          Topic to wait for
     * @param timeoutSeconds Maximum wait time in seconds
     * @return true if topic became available
     */
    boolean waitForTopicCreation(Topic topic, int timeoutSeconds);

    /**
     * Closes the repository and releases resources
     */
    void close();
}
