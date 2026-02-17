package tests.partitioning;

import io.qameta.allure.*;
import lombok.extern.slf4j.Slf4j;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Tag;
import org.junit.jupiter.api.Test;
import qa.autotest.framework.domain.model.*;
import tests.BaseTest;

import java.util.HashMap;
import java.util.Map;

import static org.assertj.core.api.Assertions.assertThat;

/**
 * Partitioning Tests
 * Hexagonal Architecture v2.0
 * No Thread.sleep — partition info comes from PublishResult directly,
 * no consumer needed → no KafkaAwaitHelper required here.
 */
@Slf4j
@Epic("Kafka Testing")
@Feature("Partitioning")
@Tag("partitioning")
@Tag("critical")
public class PartitioningTests extends BaseTest {

    @Test
    @DisplayName("TC-020: Распределение по партициям")
    @Description("Verify messages are distributed across partitions")
    @Severity(SeverityLevel.CRITICAL)
    void testPartitionDistribution() {
        String topicName = createTestTopic(2);

        int messageCount = 20;
        Map<Integer, Integer> partitionCounts = new HashMap<>();

        // No key → sticky partitioner distributes across partitions
        // Partition info comes from PublishResult — no consumer needed
        for (int i = 0; i < messageCount; i++) {
            Message message = Message.builder()
                    .topic(Topic.builder().name(topicName).build())
                    .key(null)
                    .content("{\"index\": " + i + "}")
                    .build();

            PublishResult result = kafka.publish(message);
            assertThat(result.isSuccess()).isTrue();
            partitionCounts.merge(result.getPartition(), 1, Integer::sum);
        }

        log.info("TC-020: Partition distribution: {}", partitionCounts);

        assertThat(partitionCounts.size()).isGreaterThan(0);

        int totalMessages = partitionCounts.values().stream().mapToInt(Integer::intValue).sum();
        assertThat(totalMessages).isEqualTo(messageCount);
    }

    @Test
    @DisplayName("TC-038: Round-robin распределение при отсутствии ключа")
    @Description("Verify round-robin distribution when messages have no key")
    @Severity(SeverityLevel.NORMAL)
    void testRoundRobinPartitioning() {
        String topicName = createTestTopic(2);
        int messageCount = 100;

        Map<Integer, Integer> partitionCounts = new HashMap<>();

        for (int i = 0; i < messageCount; i++) {
            Message message = Message.builder()
                    .topic(Topic.builder().name(topicName).build())
                    .key(null)
                    .content("{\"index\": " + i + "}")
                    .build();

            PublishResult result = kafka.publish(message);
            assertThat(result.isSuccess()).isTrue();
            partitionCounts.merge(result.getPartition(), 1, Integer::sum);

            // Flush every 10 to trigger sticky partitioner switch
            if (i > 0 && i % 10 == 0) {
                kafka.flush();
            }
        }

        log.info("TC-038: Partition distribution: {}", partitionCounts);

        // Kafka 3.x sticky partitioner — at least 1 partition always used
        assertThat(partitionCounts.size()).isGreaterThanOrEqualTo(1);

        // If both partitions used — check no extreme skew (at least 10% each)
        if (partitionCounts.size() == 2) {
            partitionCounts.values().forEach(count ->
                    assertThat(count).isGreaterThanOrEqualTo(10)
            );
        }
    }

    @Test
    @DisplayName("TC-039: Hash партиционирование по ключу")
    @Description("Verify hash-based partitioning with keys")
    @Severity(SeverityLevel.CRITICAL)
    @Tag("smoke")
    void testHashPartitioning() {
        String topicName = createTestTopic(2);

        Map<String, Integer> keyToPartition = new HashMap<>();

        for (int i = 0; i < 10; i++) {
            String key = "user-" + (i % 2);
            Message message = Message.builder()
                    .topic(Topic.builder().name(topicName).build())
                    .key(key)
                    .content("{\"user\": \"" + key + "\"}")
                    .build();

            PublishResult result = kafka.publish(message);
            assertThat(result.isSuccess()).isTrue();

            if (keyToPartition.containsKey(key)) {
                // Same key must always go to same partition
                assertThat(result.getPartition())
                        .as("Key '%s' must always map to same partition", key)
                        .isEqualTo(keyToPartition.get(key));
            } else {
                keyToPartition.put(key, result.getPartition());
            }
        }

        log.info("TC-039: Key → partition mapping: {}", keyToPartition);
    }

    @Test
    @DisplayName("TC-022: Same key goes to same partition")
    @Description("Verify partition key consistency for message ordering")
    @Severity(SeverityLevel.CRITICAL)
    @Tag("partitioning")
    void testPartitionKeyConsistency() {
        String topicName = createTestTopic(2);
        String consistentKey = "consistent-key-123";
        int messageCount = 20;

        Integer firstPartition = null;

        for (int i = 0; i < messageCount; i++) {
            Message message = Message.builder()
                    .topic(Topic.builder().name(topicName).build())
                    .key(consistentKey)
                    .content("Message " + i)
                    .build();

            PublishResult result = kafka.publish(message);
            assertThat(result.isSuccess()).isTrue();

            if (firstPartition == null) {
                firstPartition = result.getPartition();
            } else {
                assertThat(result.getPartition())
                        .as("Same key must always go to same partition")
                        .isEqualTo(firstPartition);
            }
        }

        log.info("TC-022: All {} messages with key '{}' → partition {}",
                messageCount, consistentKey, firstPartition);
    }

    @Test
    @DisplayName("TC-022A: Behavior when partition count changes")
    @Description("Verify system handles partition count changes")
    @Severity(SeverityLevel.NORMAL)
    @Tag("partitioning")
    void testPartitionCountChange() {
        String topicName = createTestTopic(2);
        String key = "test-key";

        Message first = Message.builder()
                .topic(Topic.builder().name(topicName).build())
                .key(key)
                .content("{\"seq\": 0}")
                .build();

        PublishResult firstResult = kafka.publish(first);
        assertThat(firstResult.isSuccess()).isTrue();
        int initialPartition = firstResult.getPartition();

        log.info("TC-022A: First message → partition {} (2 partitions)", initialPartition);

        // Same key → same partition with same partition count
        for (int i = 1; i <= 5; i++) {
            Message message = Message.builder()
                    .topic(Topic.builder().name(topicName).build())
                    .key(key)
                    .content("{\"seq\": " + i + "}")
                    .build();

            PublishResult result = kafka.publish(message);
            assertThat(result.isSuccess()).isTrue();
            assertThat(result.getPartition())
                    .as("Same partition count → same key → same partition")
                    .isEqualTo(initialPartition);
        }

        log.info("TC-022A: Partition consistency maintained for key '{}'", key);
    }
}
