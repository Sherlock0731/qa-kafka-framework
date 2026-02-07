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
     * Creates a topic with given name
     * 
     * @param topicName Topic name
     * @param partitions Number of partitions
     * @param replicationFactor Replication factor
     */
    @Step("Create topic: {topicName}")
    public void createTopic(String topicName, int partitions, short replicationFactor) {
        try {
            NewTopic newTopic = new NewTopic(topicName, partitions, replicationFactor);
            CreateTopicsResult result = adminClient.createTopics(Collections.singleton(newTopic));
            result.all().get(); // Wait for completion
            
            log.info("Topic '{}' created with {} partitions and replication factor {}", 
                    topicName, partitions, replicationFactor);
        } catch (InterruptedException | ExecutionException e) {
            if (e.getMessage().contains("TopicExistsException")) {
                log.warn("Topic '{}' already exists", topicName);
            } else {
                log.error("Failed to create topic '{}': {}", topicName, e.getMessage(), e);
                throw new RuntimeException("Failed to create topic", e);
            }
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
            log.error("Failed to delete topic '{}': {}", topicName, e.getMessage(), e);
        }
    }
    
    /**
     * Checks if topic exists
     * 
     * @param topicName Topic name
     * @return True if topic exists
     */
    @Step("Check if topic exists: {topicName}")
    public boolean topicExists(String topicName) {
        try {
            ListTopicsResult result = adminClient.listTopics();
            Set<String> topics = result.names().get();
            return topics.contains(topicName);
        } catch (InterruptedException | ExecutionException e) {
            log.error("Failed to check if topic exists: {}", e.getMessage(), e);
            return false;
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
            Set<String> topics = result.names().get();
            log.info("Found {} topics", topics.size());
            return topics;
        } catch (InterruptedException | ExecutionException e) {
            log.error("Failed to list topics: {}", e.getMessage(), e);
            throw new RuntimeException("Failed to list topics", e);
        }
    }
    
    /**
     * Creates DLQ topic for a given topic
     * 
     * @param originalTopic Original topic name
     * @return DLQ topic name
     */
    @Step("Create DLQ topic for: {originalTopic}")
    public String createDlqTopic(String originalTopic) {
        String dlqTopic = originalTopic + config.dlqTopicSuffix();
        createTopic(dlqTopic, config.testTopicPartitions(), config.testTopicReplicationFactor());
        return dlqTopic;
    }
    
    /**
     * Closes the admin client
     */
    @Override
    public void close() {
        if (adminClient != null) {
            log.debug("Closing admin client");
            adminClient.close();
        }
    }
}
