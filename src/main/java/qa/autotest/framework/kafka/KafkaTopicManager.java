package qa.autotest.framework.kafka;

import io.qameta.allure.Step;
import lombok.extern.slf4j.Slf4j;
import org.apache.kafka.clients.admin.Admin;
import org.apache.kafka.clients.admin.AdminClientConfig;
import org.apache.kafka.clients.admin.CreateTopicsResult;
import org.apache.kafka.clients.admin.DeleteTopicsResult;
import org.apache.kafka.clients.admin.ListTopicsResult;
import org.apache.kafka.clients.admin.NewTopic;
import qa.autotest.framework.config.KafkaConfig;

import java.time.Duration;
import java.util.Collections;
import java.util.Properties;
import java.util.Set;
import java.util.UUID;
import java.util.concurrent.ExecutionException;

/**
 * Kafka Topic Manager
 * Manages creation, deletion and inspection of Kafka topics
 */
@Slf4j
public class KafkaTopicManager implements AutoCloseable {

    private final KafkaConfig config;
    private final Admin adminClient;

    public KafkaTopicManager(KafkaConfig config) {
        this.config = config;
        this.adminClient = createAdminClient();
    }

    /**
     * Creates Kafka admin client
     */
    private Admin createAdminClient() {
        Properties props = new Properties();
        props.put(AdminClientConfig.BOOTSTRAP_SERVERS_CONFIG, config.kafkaBootstrapServers());
        props.put(AdminClientConfig.REQUEST_TIMEOUT_MS_CONFIG, 30000);

        // SSL configuration
        if ("SSL".equals(config.securityProtocol()) || "SASL_SSL".equals(config.securityProtocol())) {
            props.put("security.protocol", config.securityProtocol());
            props.put("ssl.truststore.location", config.sslTruststoreLocation());
            props.put("ssl.truststore.password", config.sslTruststorePassword());
            props.put("ssl.truststore.type", config.sslTruststoreType());
            props.put("ssl.keystore.location", config.sslKeystoreLocation());
            props.put("ssl.keystore.password", config.sslKeyPassword());
            props.put("ssl.keystore.type", config.sslKeystoreType());

            if (config.sslKeyPassword() != null) {
                props.put("ssl.key.password", config.sslKeyPassword());
            }
        }

        return Admin.create(props);
    }

    /**
     * Creates a unique test topic
     *
     * @return Topic name
     */
    @Step("Create unique test topic")
    public String createUniqueTopic() {
        String topicName = config.testTopicPrefix() + "-" + UUID.randomUUID();
        createTopic(topicName, config.testTopicPartitions(), config.testTopicReplicationFactor());
        return topicName;
    }

    /**
     * Creates a topic with specified partitions
     *
     * @param partitions Number of partitions
     * @return Topic name
     */
    @Step("Create topic with {partitions} partitions")
    public String createTopicWithPartitions(int partitions) {
        String topicName = config.testTopicPrefix() + "-" + UUID.randomUUID();
        createTopic(topicName, partitions, config.testTopicReplicationFactor());
        return topicName;
    }

    /**
     * Creates a DLQ (Dead Letter Queue) topic for a given main topic
     *
     * @param mainTopic Main topic name
     * @return DLQ topic name
     */
    @Step("Create DLQ topic for: {mainTopic}")
    public String createDlqTopic(String mainTopic) {
        String dlqTopic = mainTopic + config.dlqTopicSuffix();
        createTopic(dlqTopic, config.testTopicPartitions(), config.testTopicReplicationFactor());
        log.info("Created DLQ topic '{}' for main topic '{}'", dlqTopic, mainTopic);
        return dlqTopic;
    }

    /**
     * Creates a topic with given name
     *
     * @param topicName Topic name
     * @param partitions Number of partitions
     * @param replicationFactor Replication factor
     */
    @Step("Create topic: {topicName}")
    public void createTopic(String topicName, int partitions, short replicationFactor) {
        long startTime = System.currentTimeMillis();

        try {
            NewTopic newTopic = new NewTopic(topicName, partitions, replicationFactor);
            CreateTopicsResult result = adminClient.createTopics(Collections.singleton(newTopic));
            result.all().get(); // Wait for completion

            long duration = System.currentTimeMillis() - startTime;
            log.info("Topic '{}' created with {} partitions and replication factor {} (took {} ms)",
                    topicName, partitions, replicationFactor, duration);

            // Record metrics
            qa.autotest.framework.metrics.TestMetricsCollector.recordDuration("topic_create", duration);

        } catch (InterruptedException e) {
            Thread.currentThread().interrupt();
            long duration = System.currentTimeMillis() - startTime;
            log.error("Topic creation interrupted for '{}' after {} ms", topicName, duration);
            throw new qa.autotest.framework.exceptions.KafkaTopicManagementException(
                    String.format("Topic creation interrupted: %s", topicName),
                    e,
                    qa.autotest.framework.exceptions.KafkaTestException.ErrorType.TOPIC_MANAGEMENT
            ).addContext("topic", topicName)
                    .addContext("duration_ms", String.valueOf(duration));
        } catch (ExecutionException e) {
            long duration = System.currentTimeMillis() - startTime;

            // Check if topic already exists
            if (e.getMessage() != null && e.getMessage().contains("TopicExistsException")) {
                log.warn("Topic '{}' already exists", topicName);
                return; // Topic exists, that's okay
            }

            log.error("Failed to create topic '{}' after {} ms: {}", topicName, duration, e.getMessage());
            throw qa.autotest.framework.exceptions.KafkaTopicManagementException.creationFailed(
                            topicName,
                            1,
                            e
                    ).addContext("partitions", String.valueOf(partitions))
                    .addContext("replication_factor", String.valueOf(replicationFactor))
                    .addContext("duration_ms", String.valueOf(duration));
        }
    }

    /**
     * Deletes a topic
     *
     * @param topicName Topic name to delete
     */
    @Step("Delete topic: {topicName}")
    public void deleteTopic(String topicName) {
        try {
            DeleteTopicsResult result = adminClient.deleteTopics(Collections.singleton(topicName));
            result.all().get(); // Wait for completion

            log.info("Topic '{}' deleted successfully", topicName);

        } catch (InterruptedException | ExecutionException e) {
            log.error("Failed to delete topic '{}': {}", topicName, e.getMessage());
            throw new RuntimeException("Failed to delete topic: " + topicName, e);
        }
    }

    /**
     * Lists all topics
     *
     * @return Set of topic names
     */
    @Step("List all topics")
    public Set<String> listTopics() {
        try {
            ListTopicsResult result = adminClient.listTopics();
            return result.names().get();

        } catch (InterruptedException | ExecutionException e) {
            log.error("Failed to list topics: {}", e.getMessage());
            throw new RuntimeException("Failed to list topics", e);
        }
    }

    /**
     * Checks if topic exists
     *
     * @param topicName Topic name to check
     * @return true if topic exists, false otherwise
     */
    @Step("Check if topic exists: {topicName}")
    public boolean topicExists(String topicName) {
        try {
            Set<String> topics = listTopics();
            return topics.contains(topicName);
        } catch (Exception e) {
            log.error("Failed to check if topic exists: {}", e.getMessage());
            return false;
        }
    }

    /**
     * Closes the admin client with timeout
     * AdminClient is thread-safe and shared across threads, so single close is sufficient
     */
    @Override
    public void close() {
        if (adminClient != null) {
            try {
                adminClient.close(Duration.ofSeconds(5));
                log.debug("Admin client closed successfully");
            } catch (Exception e) {
                log.warn("Error closing admin client: {}", e.getMessage());
            }
        }
    }
}
