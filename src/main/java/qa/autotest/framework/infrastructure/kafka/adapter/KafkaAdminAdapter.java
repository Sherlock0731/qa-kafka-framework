package qa.autotest.framework.infrastructure.kafka.adapter;

import lombok.extern.slf4j.Slf4j;
import org.apache.kafka.clients.admin.*;
import org.apache.kafka.common.config.ConfigResource;
import qa.autotest.framework.config.KafkaConfig;
import qa.autotest.framework.domain.model.Partition;
import qa.autotest.framework.domain.model.Topic;
import qa.autotest.framework.domain.port.TopicRepository;
import qa.autotest.framework.infrastructure.KafkaPropertiesBuilder;

import java.time.Duration;
import java.util.*;
import java.util.stream.Collectors;

/**
 * Infrastructure Adapter: KafkaAdminAdapter
 * <p>
 * Implements <strong>only</strong> the {@link TopicRepository} port — topic
 * lifecycle and metadata operations via {@code AdminClient}.
 * <p>
 * <h3>ISP fix</h3>
 * Previously this class also implemented {@link qa.autotest.framework.domain.port.ConsumerGroupReader},
 * forcing {@code TopicManagementService} to depend on a contract it never uses.
 * Consumer-group introspection has been moved to the dedicated
 * {@link KafkaConsumerGroupAdapter}, which holds its own {@code AdminClient}
 * instance and implements only {@code ConsumerGroupReader}.
 */
@Slf4j
public class KafkaAdminAdapter implements TopicRepository {

    private final AdminClient adminClient;

    public KafkaAdminAdapter(KafkaConfig config) {
        this.adminClient = createAdminClient(config);
        log.debug("KafkaAdminAdapter initialized");
    }

    // ── AdminClient factory ───────────────────────────────────────────────────

    private static AdminClient createAdminClient(KafkaConfig config) {
        Properties props = KafkaPropertiesBuilder.buildBaseProperties(config);
        props.put(AdminClientConfig.REQUEST_TIMEOUT_MS_CONFIG, "30000");
        props.put(AdminClientConfig.DEFAULT_API_TIMEOUT_MS_CONFIG, "60000");
        KafkaPropertiesBuilder.configureSecurity(props, config);
        return AdminClient.create(props);
    }

    // ── TopicRepository ───────────────────────────────────────────────────────

    @Override
    public boolean createTopic(Topic topic) {
        try {
            NewTopic newTopic = new NewTopic(
                    topic.getName(),
                    topic.getPartitionCount(),
                    topic.getReplicationFactor()
            );

            Map<String, String> configs = new HashMap<>();
            configs.put("retention.ms", String.valueOf(topic.getRetentionMs()));
            configs.put("compression.type", topic.getCompressionType());
            newTopic.configs(configs);

            adminClient.createTopics(Collections.singleton(newTopic)).all().get();

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
        if (topics.isEmpty()) return 0;

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
            adminClient.createTopics(newTopics).all().get();
            log.info("Created {} topics", topics.size());
            return topics.size();

        } catch (Exception e) {
            log.error("Failed to create topics: {}", e.getMessage(), e);
            int created = 0;
            for (Topic topic : topics) {
                if (exists(topic)) created++;
            }
            return created;
        }
    }

    @Override
    public boolean deleteTopic(Topic topic) {
        try {
            adminClient.deleteTopics(Collections.singleton(topic.getName())).all().get();
            log.info("Deleted topic: {}", topic.getName());
            return true;

        } catch (Exception e) {
            log.error("Failed to delete topic: {}, error: {}", topic.getName(), e.getMessage());
            return false;
        }
    }

    @Override
    public int deleteTopics(Set<Topic> topics) {
        if (topics.isEmpty()) return 0;

        Collection<String> topicNames = topics.stream()
                .map(Topic::getName)
                .collect(Collectors.toList());

        try {
            adminClient.deleteTopics(topicNames).all().get();
            log.info("Deleted {} topics", topics.size());
            return topics.size();

        } catch (Exception e) {
            log.error("Failed to delete topics: {}", e.getMessage());
            int deleted = 0;
            for (Topic topic : topics) {
                if (!exists(topic)) deleted++;
            }
            return deleted;
        }
    }

    @Override
    public boolean exists(Topic topic) {
        try {
            Set<String> names = adminClient.listTopics().names().get();
            return names.contains(topic.getName());

        } catch (Exception e) {
            log.error("Failed to check topic existence: {}", topic.getName(), e);
            return false;
        }
    }

    @Override
    public Set<Topic> getAllTopics() {
        try {
            Set<String> names = adminClient.listTopics().names().get();
            return names.stream()
                    .map(this::getTopicDetails)
                    .filter(Objects::nonNull)
                    .collect(Collectors.toSet());

        } catch (Exception e) {
            log.error("Failed to get all topics: {}", e.getMessage());
            return Set.of();
        }
    }

    @Override
    public Set<Topic> getTopicsByPattern(String pattern) {
        String regex = pattern
                .replace(".", "\\.")
                .replace("*", ".*")
                .replace("?", ".");
        return getAllTopics().stream()
                .filter(t -> t.getName().matches(regex))
                .collect(Collectors.toSet());
    }

    @Override
    public List<Partition> getPartitions(Topic topic) {
        try {
            TopicDescription description = adminClient
                    .describeTopics(Collections.singleton(topic.getName()))
                    .allTopicNames().get()
                    .get(topic.getName());

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
            TopicDescription description = adminClient
                    .describeTopics(Collections.singleton(topicName))
                    .allTopicNames().get()
                    .get(topicName);

            if (description == null) return null;

            ConfigResource resource = new ConfigResource(ConfigResource.Type.TOPIC, topicName);
            Config config = adminClient.describeConfigs(Collections.singleton(resource))
                    .all().get()
                    .get(resource);

            return Topic.builder()
                    .name(topicName)
                    .partitionCount(description.partitions().size())
                    .replicationFactor((short) (description.partitions().isEmpty() ? 1
                            : description.partitions().get(0).replicas().size()))
                    .retentionMs(Long.parseLong(getConfigValue(config, "retention.ms", "604800000")))
                    .compressionType(getConfigValue(config, "compression.type", "none"))
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
        log.debug("Closing KafkaAdminAdapter");
        adminClient.close(Duration.ofSeconds(5));
    }

    // ── helpers ───────────────────────────────────────────────────────────────

    private String getConfigValue(Config config, String key, String defaultValue) {
        ConfigEntry entry = config.get(key);
        return entry != null && entry.value() != null ? entry.value() : defaultValue;
    }
}
