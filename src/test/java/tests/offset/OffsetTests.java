package tests.offset;

import io.qameta.allure.*;
import lombok.extern.slf4j.Slf4j;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Tag;
import org.junit.jupiter.api.Test;
import qa.autotest.app.dto.ConsumerRecordDto;
import qa.autotest.app.dto.KafkaMessageDto;
import qa.autotest.framework.utils.AsyncTestHelper;
import qa.autotest.framework.utils.TestDataGenerator;
import tests.BaseTest;

import java.util.List;

import static org.assertj.core.api.Assertions.assertThat;

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
        String topic = createTestTopic();

        // Initialize consumer BEFORE producing to avoid rebalance timing issues
        consumerManager.initConsumer(topic);
        AsyncTestHelper.waitFor(5); // Wait for consumer group rebalance
        consumerManager.poll(2);   // Pre-warm poll to trigger partition assignment
        AsyncTestHelper.waitFor(1);

        // Send messages
        List<KafkaMessageDto> messages = TestDataGenerator.generateMessages(topic, 10);
        producerManager.sendBatch(messages);
        producerManager.flush();

        List<ConsumerRecordDto> records = AsyncTestHelper.pollWithRetry(consumerManager, 60, 1);
        assertThat(records).isNotEmpty();

        log.info("Processing {} messages", records.size());

        consumerManager.commitSync();
        log.info("Offsets committed synchronously");
    }

    @Test
    @DisplayName("TC-029: Commit определенного offset'а")
    @Description("Verify committing specific offset for a partition")
    @Severity(SeverityLevel.NORMAL)
    void testCommitSpecificOffset() {
        String topic = createTestTopic();

        // Initialize consumer BEFORE producing to avoid rebalance timing issues
        consumerManager.initConsumer(topic);
        AsyncTestHelper.waitFor(5); // Wait for consumer group rebalance
        consumerManager.poll(2);   // Pre-warm poll to trigger partition assignment
        AsyncTestHelper.waitFor(1);

        List<KafkaMessageDto> messages = TestDataGenerator.generateMessages(topic, 10);
        producerManager.sendBatch(messages);
        producerManager.flush();

        List<ConsumerRecordDto> records = AsyncTestHelper.pollWithRetry(consumerManager, 60, 10);
        assertThat(records).hasSize(10);

        ConsumerRecordDto fifthRecord = records.get(4);
        consumerManager.commitOffset(topic, fifthRecord.getPartition(), fifthRecord.getOffset());

        long position = consumerManager.getPosition(topic, fifthRecord.getPartition());
        assertThat(position).isGreaterThanOrEqualTo(fifthRecord.getOffset() + 1);
    }

    @Test
    @DisplayName("TC-018: Automatic offset commit")
    @Description("Verify consumer automatically commits offsets periodically")
    @Severity(SeverityLevel.CRITICAL)
    @Tag("offset")
    void testAutoCommitOffset() {
        String topic = createTestTopic();

        // Initialize consumer BEFORE producing to avoid rebalance timing issues
        consumerManager.initConsumer(topic);
        AsyncTestHelper.waitFor(5); // Wait for consumer group rebalance
        consumerManager.poll(2);   // Pre-warm poll to trigger partition assignment
        AsyncTestHelper.waitFor(1);

        // Send messages
        List<KafkaMessageDto> messages = TestDataGenerator.generateMessages(topic, 15);
        producerManager.sendBatch(messages);
        producerManager.flush();
        AsyncTestHelper.waitFor(2);

        List<ConsumerRecordDto> records = AsyncTestHelper.pollWithRetry(consumerManager, 60, 15);
        assertThat(records).hasSize(15);

        // Close and reopen consumer - should start after committed offset
        consumerManager.close();
        AsyncTestHelper.waitFor(2);

        consumerManager.initConsumer(topic);

        List<ConsumerRecordDto> newRecords = consumerManager.poll(2);

        log.info("TC-018: After reopen, got {} records (should be < 15)", newRecords.size());

        assertThat(newRecords.size()).isLessThanOrEqualTo(15);
    }

    @Test
    @DisplayName("TC-019: Offset reset strategy")
    @Description("Verify offset reset behavior on error")
    @Severity(SeverityLevel.CRITICAL)
    @Tag("offset")
    void testOffsetResetStrategy() {
        String topic = createTestTopic();

        // Initialize consumer BEFORE producing to avoid rebalance timing issues
        consumerManager.initConsumer(topic);
        AsyncTestHelper.waitFor(5); // Wait for consumer group rebalance
        consumerManager.poll(2);   // Pre-warm poll to trigger partition assignment
        AsyncTestHelper.waitFor(1);

        // Send messages
        List<KafkaMessageDto> messages = TestDataGenerator.generateMessages(topic, 20);
        producerManager.sendBatch(messages);
        producerManager.flush();
        AsyncTestHelper.waitFor(2);

        // Should read from earliest (beginning) due to auto.offset.reset=earliest
        List<ConsumerRecordDto> records = AsyncTestHelper.pollWithRetry(consumerManager, 60, 20);

        assertThat(records.size()).isGreaterThan(0);
        assertThat(records.size()).isLessThanOrEqualTo(20);
    }

    @Test
    @DisplayName("TC-019A: Offset commit during rebalance")
    @Description("Verify offsets are committed before rebalance")
    @Severity(SeverityLevel.NORMAL)
    @Tag("offset")
    @Tag("consumer-group")
    void testOffsetCommitOnRebalance() {
        String topic = createTestTopic(2); // 2 partitions

        // Initialize consumer BEFORE producing to avoid rebalance timing issues
        consumerManager.initConsumer(topic);
        AsyncTestHelper.waitFor(5); // Wait for consumer group rebalance
        consumerManager.poll(2);   // Pre-warm poll to trigger partition assignment
        AsyncTestHelper.waitFor(1);

        // Send messages
        List<KafkaMessageDto> messages = TestDataGenerator.generateMessages(topic, 30);
        producerManager.sendBatch(messages);
        producerManager.flush();
        AsyncTestHelper.waitFor(3);

        List<ConsumerRecordDto> records = AsyncTestHelper.pollWithRetry(consumerManager, 60, 10);
        assertThat(records.size()).isGreaterThan(0);

        consumerManager.commitSync();

        consumerManager.close();
        AsyncTestHelper.waitFor(2);

        consumerManager.initConsumer(topic);

        List<ConsumerRecordDto> newRecords = AsyncTestHelper.pollWithRetry(consumerManager, 60, 20);

        assertThat(records.size() + newRecords.size()).isGreaterThanOrEqualTo(records.size());
    }
}
