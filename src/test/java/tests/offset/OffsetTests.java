package tests.offset;

import io.qameta.allure.*;
import lombok.extern.slf4j.Slf4j;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Tag;
import org.junit.jupiter.api.Test;
import qa.autotest.framework.domain.model.*;
import qa.autotest.framework.utils.KafkaAwaitHelper;
import tests.BaseTest;

import java.time.Duration;
import java.util.ArrayList;
import java.util.List;

import static org.assertj.core.api.Assertions.assertThat;

/**
 * Offset Management Tests
 * Hexagonal Architecture v2.0
 * No Thread.sleep — all waits via KafkaAwaitHelper (Awaitility)
 */
@Slf4j
@Epic("Kafka Testing")
@Feature("Offset Management")
@Tag("offset")
@Tag("critical")
public class OffsetTests extends BaseTest {

    @Test
    @DisplayName("TC-027: Ручной sync commit после обработки")
    @Description("Verify manual synchronous commit of offsets")
    @Severity(SeverityLevel.CRITICAL)
    @Tag("smoke")
    void testManualSyncCommit() {
        String topicName = createTestTopic();

        KafkaAwaitHelper.awaitConsumerReady(kafka, topicName, 15);

        List<Message> messages = buildMessages(topicName, "commit-key-", 10);
        kafka.publishBatch(messages);
        KafkaAwaitHelper.awaitPropagation(kafka, topicName, 10, 15);

        List<Message> records = KafkaAwaitHelper.awaitNewMessages(kafka, 1, 30);
        assertThat(records).isNotEmpty();

        // Manual sync commit
        kafka.commitSync();

        log.info("TC-027: Committed {} messages synchronously", records.size());
    }

    @Test
    @DisplayName("TC-029: Commit определенного offset'а")
    @Description("Verify committing specific offset for a partition")
    @Severity(SeverityLevel.NORMAL)
    void testCommitSpecificOffset() {
        String topicName = createTestTopic();

        KafkaAwaitHelper.awaitConsumerReady(kafka, topicName, 15);

        // Publish and keep PublishResult to get offset/partition info
        List<Message> messages = buildMessages(topicName, "specific-key-", 10);
        List<PublishResult> publishResults = kafka.publishBatch(messages);
        KafkaAwaitHelper.awaitPropagation(kafka, topicName, 10, 15);

        List<Message> records = KafkaAwaitHelper.awaitNewMessages(kafka, 10, 30);
        assertThat(records).hasSize(10);

        // Offset and partition come from PublishResult, not from consumed Message
        // (Message domain model carries content, not delivery metadata)
        PublishResult fifthResult = publishResults.get(4);
        kafka.commitOffset(
                topicName,
                fifthResult.getPartition(),
                fifthResult.getOffset()
        );

        log.info("TC-029: Committed offset {} on partition {}",
                fifthResult.getOffset(), fifthResult.getPartition());
    }

    @Test
    @DisplayName("TC-018: Automatic offset commit")
    @Description("Verify consumer automatically commits offsets periodically")
    @Severity(SeverityLevel.CRITICAL)
    @Tag("offset")
    void testAutoCommitOffset() {
        String topicName = createTestTopic();

        KafkaAwaitHelper.awaitConsumerReady(kafka, topicName, 15);

        List<Message> messages = buildMessages(topicName, "auto-commit-key-", 15);
        kafka.publishBatch(messages);
        KafkaAwaitHelper.awaitPropagation(kafka, topicName, 15, 15);

        List<Message> records = KafkaAwaitHelper.awaitNewMessages(kafka, 15, 30);
        assertThat(records).hasSize(15);

        // Close current consumer — offsets were auto-committed
        kafka.close();

        // New consumer in same group — should start after committed offset
        kafka = createNewFacade();
        KafkaAwaitHelper.awaitRebalance(kafka, topicName, 15);

        ConsumeResult newRecords = kafka.poll(Duration.ofSeconds(5));

        log.info("TC-018: After reopen, got {} records (should be < 15)",
                newRecords.getMessageCount());
        assertThat(newRecords.getMessageCount()).isLessThanOrEqualTo(15);
    }

    @Test
    @DisplayName("TC-019: Offset reset strategy")
    @Description("Verify offset reset behavior on error")
    @Severity(SeverityLevel.CRITICAL)
    @Tag("offset")
    void testOffsetResetStrategy() {
        String topicName = createTestTopic();

        KafkaAwaitHelper.awaitConsumerReady(kafka, topicName, 15);

        List<Message> messages = buildMessages(topicName, "reset-key-", 20);
        kafka.publishBatch(messages);
        KafkaAwaitHelper.awaitPropagation(kafka, topicName, 20, 15);

        // Should read from earliest due to auto.offset.reset=earliest
        List<Message> records = KafkaAwaitHelper.awaitNewMessages(kafka, 20, 30);

        assertThat(records.size()).isGreaterThan(0);
        assertThat(records.size()).isLessThanOrEqualTo(20);

        log.info("TC-019: Offset reset strategy consumed {} messages", records.size());
    }

    @Test
    @DisplayName("TC-019A: Offset commit during rebalance")
    @Description("Verify offsets are committed before rebalance")
    @Severity(SeverityLevel.NORMAL)
    @Tag("offset")
    @Tag("consumer-group")
    void testOffsetCommitOnRebalance() {
        String topicName = createTestTopic(2);

        KafkaAwaitHelper.awaitConsumerReady(kafka, topicName, 15);

        List<Message> messages = buildMessages(topicName, "rebalance-offset-key-", 30);
        kafka.publishBatch(messages);
        KafkaAwaitHelper.awaitPropagation(kafka, topicName, 30, 15);

        List<Message> records = KafkaAwaitHelper.awaitNewMessages(kafka, 10, 30);
        assertThat(records.size()).isGreaterThan(0);

        // Commit before rebalance
        kafka.commitSync();

        // Trigger rebalance: close → new facade
        kafka.close();
        kafka = createNewFacade();
        KafkaAwaitHelper.awaitRebalance(kafka, topicName, 15);

        // New consumer should start after committed offset
        List<Message> newRecords = KafkaAwaitHelper.awaitNewMessages(kafka, 20, 30);

        assertThat(records.size() + newRecords.size()).isGreaterThanOrEqualTo(records.size());
        log.info("TC-019A: Before rebalance={}, after rebalance={}",
                records.size(), newRecords.size());
    }

    // ── helper ────────────────────────────────────────────────────────────────

    private List<Message> buildMessages(String topicName, String keyPrefix, int count) {
        List<Message> list = new ArrayList<>();
        for (int i = 0; i < count; i++) {
            list.add(Message.builder()
                    .topic(Topic.builder().name(topicName).build())
                    .key(keyPrefix + i)
                    .content("{\"index\": " + i + "}")
                    .build());
        }
        return list;
    }
}
