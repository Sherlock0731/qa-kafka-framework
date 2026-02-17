package qa.autotest.framework.application.service;

import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import qa.autotest.framework.domain.model.Partition;
import qa.autotest.framework.domain.model.Topic;
import qa.autotest.framework.domain.port.TopicRepository;

import java.util.List;
import java.util.Set;
import java.util.stream.Collectors;

/**
 * Application Service: TopicManagementService
 * <p>
 * Orchestrates topic management use cases.
 * Depends on Port abstractions, not on infrastructure.
 */
@Slf4j
@RequiredArgsConstructor
public class TopicManagementService {

    private final TopicRepository topicRepository;

    /**
     * Use Case: Create a topic
     *
     * @param topic Topic to create
     * @return true if created successfully
     */
    public boolean createTopic(Topic topic) {
        log.debug("Creating topic: {}", topic.getName());

        // Domain validation
        topic.validate();

        // Check if topic already exists
        if (topicRepository.exists(topic)) {
            log.warn("Topic already exists: {}", topic.getName());
            return false;
        }

        boolean created = topicRepository.createTopic(topic);

        if (created) {
            log.info("Successfully created topic: {} (partitions={}, replication={})",
                    topic.getName(), topic.getPartitionCount(), topic.getReplicationFactor());
        } else {
            log.error("Failed to create topic: {}", topic.getName());
        }

        return created;
    }

    /**
     * Use Case: Create multiple topics
     *
     * @param topics Topics to create
     * @return Number of topics created successfully
     */
    public int createTopics(Set<Topic> topics) {
        log.debug("Creating {} topics", topics.size());

        // Domain validation
        topics.forEach(Topic::validate);

        // Filter out existing topics
        Set<Topic> topicsToCreate = topics.stream()
                .filter(topic -> !topicRepository.exists(topic))
                .collect(Collectors.toSet());

        if (topicsToCreate.size() < topics.size()) {
            log.warn("{} topics already exist, creating only {} new topics",
                    topics.size() - topicsToCreate.size(), topicsToCreate.size());
        }

        int created = topicRepository.createTopics(topicsToCreate);
        log.info("Successfully created {}/{} topics", created, topicsToCreate.size());

        return created;
    }

    /**
     * Use Case: Create topic with DLQ
     *
     * @param topic Main topic
     * @return true if both topics created
     */
    public boolean createTopicWithDlq(Topic topic) {
        log.debug("Creating topic with DLQ: {}", topic.getName());

        topic.validate();

        // Create main topic
        boolean mainCreated = createTopic(topic);
        if (!mainCreated) {
            return false;
        }

        // Create DLQ topic
        Topic dlqTopic = topic.createDlqTopic();
        boolean dlqCreated = createTopic(dlqTopic);

        if (dlqCreated) {
            log.info("Successfully created topic with DLQ: {} and {}",
                    topic.getName(), dlqTopic.getName());
        } else {
            log.error("Created main topic but failed to create DLQ: {}", topic.getName());
        }

        return dlqCreated;
    }

    /**
     * Use Case: Create topic with retry topics
     *
     * @param topic       Main topic
     * @param retryLevels Number of retry levels
     * @return true if all topics created
     */
    public boolean createTopicWithRetries(Topic topic, int retryLevels) {
        log.debug("Creating topic with {} retry levels: {}", retryLevels, topic.getName());

        topic.validate();

        // Create main topic
        if (!createTopic(topic)) {
            return false;
        }

        // Create retry topics
        boolean allCreated = true;
        for (int level = 1; level <= retryLevels; level++) {
            Topic retryTopic = topic.createRetryTopic(level);
            if (!createTopic(retryTopic)) {
                allCreated = false;
                log.error("Failed to create retry topic level {}: {}", level, retryTopic.getName());
            }
        }

        if (allCreated) {
            log.info("Successfully created topic with {} retry levels: {}", retryLevels, topic.getName());
        }

        return allCreated;
    }

    /**
     * Use Case: Delete a topic
     *
     * @param topic Topic to delete
     * @return true if deleted successfully
     */
    public boolean deleteTopic(Topic topic) {
        log.debug("Deleting topic: {}", topic.getName());

        if (!topicRepository.exists(topic)) {
            log.warn("Topic does not exist: {}", topic.getName());
            return false;
        }

        boolean deleted = topicRepository.deleteTopic(topic);

        if (deleted) {
            log.info("Successfully deleted topic: {}", topic.getName());
        } else {
            log.error("Failed to delete topic: {}", topic.getName());
        }

        return deleted;
    }

    /**
     * Use Case: Delete test topics
     *
     * @return Number of topics deleted
     */
    public int deleteTestTopics() {
        log.debug("Deleting all test topics");

        Set<Topic> allTopics = topicRepository.getAllTopics();
        Set<Topic> testTopics = allTopics.stream()
                .filter(Topic::isTestTopic)
                .collect(Collectors.toSet());

        if (testTopics.isEmpty()) {
            log.info("No test topics found to delete");
            return 0;
        }

        int deleted = topicRepository.deleteTopics(testTopics);
        log.info("Deleted {}/{} test topics", deleted, testTopics.size());

        return deleted;
    }

    /**
     * Use Case: Delete topics by pattern
     *
     * @param pattern Pattern to match
     * @return Number of topics deleted
     */
    public int deleteTopicsByPattern(String pattern) {
        log.debug("Deleting topics matching pattern: {}", pattern);

        Set<Topic> matchingTopics = topicRepository.getTopicsByPattern(pattern);

        if (matchingTopics.isEmpty()) {
            log.info("No topics found matching pattern: {}", pattern);
            return 0;
        }

        int deleted = topicRepository.deleteTopics(matchingTopics);
        log.info("Deleted {}/{} topics matching pattern: {}", deleted, matchingTopics.size(), pattern);

        return deleted;
    }

    /**
     * Use Case: Check if topic exists
     *
     * @param topic Topic to check
     * @return true if exists
     */
    public boolean topicExists(Topic topic) {
        return topicRepository.exists(topic);
    }

    /**
     * Use Case: Get all topics
     *
     * @return Set of all topics
     */
    public Set<Topic> getAllTopics() {
        Set<Topic> topics = topicRepository.getAllTopics();
        log.debug("Retrieved {} topics", topics.size());
        return topics;
    }

    /**
     * Use Case: Get partition information
     *
     * @param topic Topic to get partitions for
     * @return List of partitions with details
     */
    public List<Partition> getPartitionInfo(Topic topic) {
        log.debug("Getting partition info for topic: {}", topic.getName());

        List<Partition> partitions = topicRepository.getPartitions(topic);
        log.info("Topic {} has {} partitions", topic.getName(), partitions.size());

        return partitions;
    }

    /**
     * Use Case: Wait for topic to be available
     *
     * @param topic          Topic to wait for
     * @param timeoutSeconds Maximum wait time
     * @return true if topic became available
     */
    public boolean waitForTopic(Topic topic, int timeoutSeconds) {
        log.debug("Waiting for topic to be available: {} (timeout: {}s)",
                topic.getName(), timeoutSeconds);

        boolean available = topicRepository.waitForTopicCreation(topic, timeoutSeconds);

        if (available) {
            log.info("Topic is available: {}", topic.getName());
        } else {
            log.warn("Topic did not become available within timeout: {}", topic.getName());
        }

        return available;
    }

    /**
     * Closes the management service
     */
    public void close() {
        log.debug("Closing topic management service");
        topicRepository.close();
    }
}
