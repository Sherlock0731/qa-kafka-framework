package qa.autotest.framework.infrastructure.kafka.adapter;

import lombok.extern.slf4j.Slf4j;
import org.apache.kafka.clients.admin.*;
import org.apache.kafka.common.config.ConfigResource;
import qa.autotest.framework.config.KafkaConfig;
import qa.autotest.framework.domain.model.Partition;
import qa.autotest.framework.domain.model.Topic;
import qa.autotest.framework.domain.port.TopicRepository;
import qa.autotest.framework.kafka.KafkaPropertiesBuilder;

import java.time.Duration;
import java.util.*;
import java.util.concurrent.ExecutionException;
import java.util.stream.Collectors;

/**
 * Infrastructure Adapter: KafkaAdminAdapter
 * <p>
 * Implements the TopicRepository port using Kafka Admin API.
 * Manages topic lifecycle and metadata operations.
 */
@Slf4j
public class KafkaAdminAdapter implements TopicRepository {

    private final KafkaConfig config;
    private final AdminClient adminClient;

    public KafkaAdminAdapter(KafkaConfig config) {
        this.config = config;
        this.adminClient = createAdminClient();
        log.debug("KafkaAdminAdapter initialized");
    }

    /**
     * Creates Kafka Admin client
     */
    private AdminClient createAdminClient() {
        Properties props = KafkaPropertiesBuilder.buildBaseProperties(config);
        props.put(AdminClientConfig.REQUEST_TIMEOUT_MS_CONFIG, "30000");
        props.put(AdminClientConfig.DEFAULT_API_TIMEOUT_MS_CONFIG, "60000");

        KafkaPropertiesBuilder.configureSecurity(props, config);

        return AdminClient.create(props);
    }

    @Override
    public boolean createTopic(Topic topic) {
        try {
            NewTopic newTopic = new NewTopic(
                    topic.getName(),
                    topic.getPartitionCount(),
                    topic.getReplicationFactor()
            );

            // Add topic configurations
            Map<String, String> configs = new HashMap<>();
            configs.put("retention.ms", String.valueOf(topic.getRetentionMs()));
            configs.put("compression.type", topic.getCompressionType());
            newTopic.configs(configs);

            CreateTopicsResult result = adminClient.createTopics(Collections.singleton(newTopic));
            result.all().get();

            log.info("Created topic: {} (partitions={}, replication={})",
                    topic.getName(), topic.getPartitionCount(), topic.getReplicationFactor());
            return true;

        } catch (Exception e) {
            log.error("Failed to create topic: {}, error: {}", topic.getName(), e.getMessage(), e);
            return false;
        }
    }

    @Override
    public int createTopics(Set<Topic> topics) {
        if (topics.isEmpty()) {
            return 0;
        }

        Collection<NewTopic> newTopics = topics.stream()
                .map(topic -> {
                    NewTopic nt = new NewTopic(
                            topic.getName(),
                            topic.getPartitionCount(),
                            topic.getReplicationFactor()
                    );

                    Map<String, String> configs = new HashMap<>();
                    configs.put("retention.ms", String.valueOf(topic.getRetentionMs()));
                    configs.put("compression.type", topic.getCompressionType());
                    nt.configs(configs);

                    return nt;
                })
                .collect(Collectors.toList());

        try {
            CreateTopicsResult result = adminClient.createTopics(newTopics);
            result.all().get();

            log.info("Created {} topics", topics.size());
            return topics.size();

        } catch (Exception e) {
            log.error("Failed to create topics: {}", e.getMessage(), e);

            // Count successful creations
            int created = 0;
            for (Topic topic : topics) {
                if (exists(topic)) {
                    created++;
                }
            }
            return created;
        }
    }

    @Override
    public boolean deleteTopic(Topic topic) {
        try {
            DeleteTopicsResult result = adminClient.deleteTopics(Collections.singleton(topic.getName()));
            result.all().get();

            log.info("Deleted topic: {}", topic.getName());
            return true;

        } catch (Exception e) {
            log.error("Failed to delete topic: {}, error: {}", topic.getName(), e.getMessage());
            return false;
        }
    }

    @Override
    public int deleteTopics(Set<Topic> topics) {
        if (topics.isEmpty()) {
            return 0;
        }

        Collection<String> topicNames = topics.stream()
                .map(Topic::getName)
                .collect(Collectors.toList());

        try {
            DeleteTopicsResult result = adminClient.deleteTopics(topicNames);
            result.all().get();

            log.info("Deleted {} topics", topics.size());
            return topics.size();

        } catch (Exception e) {
            log.error("Failed to delete topics: {}", e.getMessage());

            // Count successful deletions
            int deleted = 0;
            for (Topic topic : topics) {
                if (!exists(topic)) {
                    deleted++;
                }
            }
            return deleted;
        }
    }

    @Override
    public boolean exists(Topic topic) {
        try {
            ListTopicsResult result = adminClient.listTopics();
            Set<String> topicNames = result.names().get();
            return topicNames.contains(topic.getName());

        } catch (Exception e) {
            log.error("Failed to check topic existence: {}", topic.getName(), e);
            return false;
        }
    }

    @Override
    public Set<Topic> getAllTopics() {
        try {
            ListTopicsResult result = adminClient.listTopics();
            Set<String> topicNames = result.names().get();

            return topicNames.stream()
                    .map(name -> getTopicDetails(name))
                    .filter(Objects::nonNull)
                    .collect(Collectors.toSet());

        } catch (Exception e) {
            log.error("Failed to get all topics: {}", e.getMessage());
            return Set.of();
        }
    }

    @Override
    public Set<Topic> getTopicsByPattern(String pattern) {
        Set<Topic> allTopics = getAllTopics();

        // Convert glob pattern to regex
        String regex = pattern
                .replace(".", "\\.")
                .replace("*", ".*")
                .replace("?", ".");

        return allTopics.stream()
                .filter(topic -> topic.getName().matches(regex))
                .collect(Collectors.toSet());
    }

    @Override
    public List<Partition> getPartitions(Topic topic) {
        try {
            DescribeTopicsResult result = adminClient.describeTopics(Collections.singleton(topic.getName()));
            TopicDescription description = result.allTopicNames().get().get(topic.getName());

            return description.partitions().stream()
                    .map(info -> Partition.builder()
                            .topic(topic)
                            .partitionNumber(info.partition())
                            .leader(info.leader() != null ? info.leader().id() : null)
                            .isrCount(info.isr().size())
                            .build())
                    .collect(Collectors.toList());

        } catch (Exception e) {
            log.error("Failed to get partitions for topic: {}", topic.getName(), e);
            return List.of();
        }
    }

    @Override
    public Topic getTopicDetails(String topicName) {
        try {
            DescribeTopicsResult topicsResult = adminClient.describeTopics(Collections.singleton(topicName));
            TopicDescription description = topicsResult.allTopicNames().get().get(topicName);

            if (description == null) {
                return null;
            }

            // Get topic configs
            ConfigResource resource = new ConfigResource(ConfigResource.Type.TOPIC, topicName);
            DescribeConfigsResult configsResult = adminClient.describeConfigs(Collections.singleton(resource));
            Config config = configsResult.all().get().get(resource);

            String retentionMs = getConfigValue(config, "retention.ms", "604800000");
            String compression = getConfigValue(config, "compression.type", "none");

            return Topic.builder()
                    .name(topicName)
                    .partitionCount(description.partitions().size())
                    .replicationFactor((short) (description.partitions().isEmpty() ? 1 :
                            description.partitions().get(0).replicas().size()))
                    .retentionMs(Long.parseLong(retentionMs))
                    .compressionType(compression)
                    .build();

        } catch (Exception e) {
            log.error("Failed to get topic details: {}", topicName, e);
            return null;
        }
    }

    @Override
    public boolean waitForTopicCreation(Topic topic, int timeoutSeconds) {
        long endTime = System.currentTimeMillis() + (timeoutSeconds * 1000L);

        while (System.currentTimeMillis() < endTime) {
            if (exists(topic)) {
                log.debug("Topic available: {}", topic.getName());
                return true;
            }

            try {
                Thread.sleep(500);
            } catch (InterruptedException e) {
                Thread.currentThread().interrupt();
                return false;
            }
        }

        log.warn("Topic not available after {}s: {}", timeoutSeconds, topic.getName());
        return false;
    }

    @Override
    public void close() {
        if (adminClient != null) {
            log.debug("Closing Kafka Admin client");
            adminClient.close(Duration.ofSeconds(5));
        }
    }

    /**
     * Helper: Gets config value with default
     */
    private String getConfigValue(Config config, String key, String defaultValue) {
        ConfigEntry entry = config.get(key);
        return entry != null && entry.value() != null ? entry.value() : defaultValue;
    }
}
