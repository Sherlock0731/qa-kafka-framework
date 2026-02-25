package qa.autotest.framework.infrastructure.kafka.adapter;

import lombok.extern.slf4j.Slf4j;
import org.apache.kafka.clients.admin.AdminClient;
import org.apache.kafka.clients.admin.AdminClientConfig;
import org.apache.kafka.clients.admin.ConsumerGroupDescription;
import org.apache.kafka.clients.admin.MemberDescription;
import org.apache.kafka.common.ConsumerGroupState;
import qa.autotest.framework.config.KafkaConfig;
import qa.autotest.framework.domain.model.ConsumerGroup;
import qa.autotest.framework.domain.model.Topic;
import qa.autotest.framework.domain.port.ConsumerGroupReader;
import qa.autotest.framework.infrastructure.KafkaPropertiesBuilder;

import java.time.Duration;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Properties;
import java.util.Set;
import java.util.concurrent.TimeUnit;
import java.util.stream.Collectors;

/**
 * Infrastructure Adapter: KafkaConsumerGroupAdapter
 * <p>
 * Implements <strong>only</strong> the {@link ConsumerGroupReader} port —
 * consumer-group introspection via {@code AdminClient.describeConsumerGroups()}.
 * <p>
 * <h3>ISP rationale</h3>
 * Consumer-group state is an administrative read concern: it has nothing to do
 * with topic CRUD ({@link KafkaAdminAdapter}) or message production/consumption
 * ({@link KafkaProducerAdapter}/{@link KafkaConsumerAdapter}).
 * Keeping the responsibility in its own adapter means each client class
 * ({@code TopicManagementService}, {@code MessageConsumptionService}) depends
 * only on the port it actually uses.
 * <p>
 * <h3>AdminClient ownership</h3>
 * This adapter owns a dedicated {@code AdminClient} instance.  Although sharing
 * an instance with {@link KafkaAdminAdapter} is safe (AdminClient is
 * thread-safe), sharing would re-couple the two adapters at the object level
 * and contradict the ISP split.  The extra connection overhead is negligible
 * for a test framework.
 */
@Slf4j
public class KafkaConsumerGroupAdapter implements ConsumerGroupReader {

    private final AdminClient adminClient;

    public KafkaConsumerGroupAdapter(KafkaConfig config) {
        this.adminClient = createAdminClient(config);
        log.debug("KafkaConsumerGroupAdapter initialized");
    }

    // ── AdminClient factory ───────────────────────────────────────────────────

    private static AdminClient createAdminClient(KafkaConfig config) {
        Properties props = KafkaPropertiesBuilder.buildBaseProperties(config);
        props.put(AdminClientConfig.REQUEST_TIMEOUT_MS_CONFIG, "10000");
        props.put(AdminClientConfig.DEFAULT_API_TIMEOUT_MS_CONFIG, "15000");
        KafkaPropertiesBuilder.configureSecurity(props, config);
        return AdminClient.create(props);
    }

    // ── ConsumerGroupReader ───────────────────────────────────────────────────

    /**
     * Fetches the current state of the given consumer group from the broker
     * using {@code AdminClient.describeConsumerGroups()}.
     * <p>
     * On any broker or timeout error returns a minimal {@code EMPTY} group so
     * that Awaitility callers fail with a {@code ConditionTimeoutException}
     * rather than an unexpected exception.
     *
     * @param groupId consumer group ID to inspect
     * @return current {@link ConsumerGroup} snapshot; never {@code null}
     */
    @Override
    public ConsumerGroup describeConsumerGroup(String groupId) {
        try {
            Map<String, ConsumerGroupDescription> result =
                    adminClient.describeConsumerGroups(List.of(groupId))
                            .all()
                            .get(10, TimeUnit.SECONDS);

            ConsumerGroupDescription desc = result.get(groupId);
            if (desc == null) {
                log.warn("describeConsumerGroups returned no entry for groupId={}", groupId);
                return emptyGroup(groupId);
            }

            ConsumerGroup.GroupState domainState = mapGroupState(desc.state());

            Set<Topic> subscribedTopics = desc.members().stream()
                    .flatMap(m -> m.assignment().topicPartitions().stream())
                    .map(tp -> Topic.builder().name(tp.topic()).build())
                    .collect(Collectors.toSet());

            Map<Integer, String> partitionAssignments = new HashMap<>();
            for (MemberDescription member : desc.members()) {
                for (org.apache.kafka.common.TopicPartition tp : member.assignment().topicPartitions()) {
                    partitionAssignments.put(tp.partition(), member.clientId());
                }
            }

            Integer coordinatorId = desc.coordinator() != null ? desc.coordinator().id() : null;

            ConsumerGroup group = ConsumerGroup.builder()
                    .groupId(groupId)
                    .state(domainState)
                    .memberCount(desc.members().size())
                    .subscribedTopics(subscribedTopics)
                    .partitionAssignments(partitionAssignments)
                    .coordinatorId(coordinatorId)
                    .build();

            log.debug("ConsumerGroup described: groupId={}, state={}, members={}, partitions={}",
                    groupId, domainState, desc.members().size(), partitionAssignments.size());
            return group;

        } catch (Exception e) {
            log.warn("Failed to describe consumer group '{}': {}", groupId, e.getMessage());
            return emptyGroup(groupId);
        }
    }

    // ── lifecycle ─────────────────────────────────────────────────────────────

    public void close() {
        log.debug("Closing KafkaConsumerGroupAdapter");
        adminClient.close(Duration.ofSeconds(5));
    }

    // ── helpers ───────────────────────────────────────────────────────────────

    private ConsumerGroup.GroupState mapGroupState(ConsumerGroupState kafkaState) {
        if (kafkaState == null) return ConsumerGroup.GroupState.DEAD;
        return switch (kafkaState) {
            case STABLE -> ConsumerGroup.GroupState.STABLE;
            case PREPARING_REBALANCE -> ConsumerGroup.GroupState.PREPARING_REBALANCE;
            case COMPLETING_REBALANCE -> ConsumerGroup.GroupState.COMPLETING_REBALANCE;
            case EMPTY -> ConsumerGroup.GroupState.EMPTY;
            case DEAD -> ConsumerGroup.GroupState.DEAD;
            default -> ConsumerGroup.GroupState.DEAD;
        };
    }

    private ConsumerGroup emptyGroup(String groupId) {
        return ConsumerGroup.builder()
                .groupId(groupId)
                .state(ConsumerGroup.GroupState.EMPTY)
                .memberCount(0)
                .subscribedTopics(Set.of())
                .partitionAssignments(Map.of())
                .build();
    }
}
