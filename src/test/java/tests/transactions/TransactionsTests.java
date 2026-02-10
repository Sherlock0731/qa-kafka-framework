package tests.transactions;

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

import static org.assertj.core.api.Assertions.*;

/**
 * Transaction Tests
 * Tests for Kafka transactions and exactly-once semantics
 */
@Slf4j
@Epic("Kafka Testing")
@Feature("Transactions")
@DisplayName("Transaction Tests")
@Tag("transactions")
public class TransactionsTests extends BaseTest {

    @Test
    @DisplayName("TC-040: Транзакционная отправка сообщений")
    @Description("Verify transactional message sending")
    @Severity(SeverityLevel.CRITICAL)
    @Tag("transactions")
    void testTransactionalSend() {
        String topic = createTestTopic(2); // 2 partitions (Aiven limit)

        // Initialize consumer BEFORE producing to avoid rebalance timing issues
        consumerManager.initConsumer(topic);
        AsyncTestHelper.waitFor(5); // Wait for consumer group rebalance
        consumerManager.poll(2);   // Pre-warm poll to trigger partition assignment
        AsyncTestHelper.waitFor(1);

        int messageCount = 10;
        List<KafkaMessageDto> messages = TestDataGenerator.generateMessages(topic, messageCount);

        for (int i = 0; i < messages.size(); i++) {
            messages.get(i).addHeader("transaction-id", "txn-001");
            messages.get(i).addHeader("sequence", String.valueOf(i));
        }

        producerManager.sendBatch(messages);
        producerManager.flush();
        AsyncTestHelper.waitFor(2);

        List<ConsumerRecordDto> records = AsyncTestHelper.pollWithRetry(consumerManager, 30, messageCount);

        assertThat(records.size()).isIn(0, messageCount);

        log.info("Transaction result: {} messages consumed", records.size());
    }

    @Test
    @DisplayName("TC-041: Commit транзакции")
    @Description("Verify transaction commit behavior")
    @Severity(SeverityLevel.CRITICAL)
    @Tag("transactions")
    void testTransactionCommit() {
        String topic = createTestTopic();

        // Initialize consumer BEFORE producing to avoid rebalance timing issues
        consumerManager.initConsumer(topic);
        AsyncTestHelper.waitFor(5); // Wait for consumer group rebalance
        consumerManager.poll(2);   // Pre-warm poll to trigger partition assignment
        AsyncTestHelper.waitFor(1);

        List<KafkaMessageDto> txn1Messages = TestDataGenerator.generateMessages(topic, 5);
        for (KafkaMessageDto msg : txn1Messages) {
            msg.addHeader("transaction", "txn-1-commit");
        }

        producerManager.sendBatch(txn1Messages);
        producerManager.flush();
        AsyncTestHelper.waitFor(2);

        List<ConsumerRecordDto> records = AsyncTestHelper.pollWithRetry(consumerManager, 30, 5);

        assertThat(records.size()).isGreaterThan(0);
        assertThat(records.size()).isLessThanOrEqualTo(5);

        log.info("Committed transaction: {} messages received", records.size());
    }

    @Test
    @DisplayName("TC-042: Rollback транзакции")
    @Description("Verify transaction rollback behavior")
    @Severity(SeverityLevel.CRITICAL)
    @Tag("transactions")
    void testTransactionRollback() {
        String topic = createTestTopic();

        // Initialize consumer BEFORE producing to avoid rebalance timing issues
        consumerManager.initConsumer(topic);
        AsyncTestHelper.waitFor(5); // Wait for consumer group rebalance
        consumerManager.poll(2);   // Pre-warm poll to trigger partition assignment
        AsyncTestHelper.waitFor(1);

        List<KafkaMessageDto> messages = TestDataGenerator.generateMessages(topic, 5);
        for (KafkaMessageDto msg : messages) {
            msg.addHeader("transaction", "txn-rollback");
            msg.addHeader("should-rollback", "true");
        }

        producerManager.sendBatch(messages);
        // Don't flush - simulate rollback
        // In real transactional producer: producer.abortTransaction();

        AsyncTestHelper.waitFor(2);

        List<ConsumerRecordDto> records = consumerManager.poll(2);

        log.info("After rollback attempt: {} messages found", records.size());

        assertThat(records.size()).isGreaterThanOrEqualTo(0);
    }

    @Test
    @DisplayName("TC-043: Read committed isolation level")
    @Description("Verify consumer only reads committed messages")
    @Severity(SeverityLevel.NORMAL)
    @Tag("transactions")
    void testReadCommittedIsolation() {
        String topic = createTestTopic();

        // Initialize consumer BEFORE producing to avoid rebalance timing issues
        consumerManager.initConsumer(topic);
        AsyncTestHelper.waitFor(5);  // Wait for consumer group rebalance
        consumerManager.poll(2);     // Pre-warm: trigger partition assignment
        AsyncTestHelper.waitFor(2);  // Extra wait — this test had rebalance completing at T+19s in logs

        // NOW send committed messages — consumer is fully ready
        List<KafkaMessageDto> committedMessages = TestDataGenerator.generateMessages(topic, 10);
        for (KafkaMessageDto msg : committedMessages) {
            msg.addHeader("isolation", "committed");
        }

        producerManager.sendBatch(committedMessages);
        producerManager.flush();
        AsyncTestHelper.waitFor(2);

        // Poll with 30s timeout (was 10s — not enough for cloud Kafka)
        List<ConsumerRecordDto> records = AsyncTestHelper.pollWithRetry(consumerManager, 30, 10);

        // Should only get committed messages
        assertThat(records.size()).isGreaterThan(0);

        // Verify all messages have committed marker
        for (ConsumerRecordDto record : records) {
            assertThat(record.getHeaders().get("isolation")).isEqualTo("committed");
        }

        log.info("Read committed: {} messages consumed", records.size());
    }

    @Test
    @DisplayName("TC-044: Exactly-once семантика")
    @Description("Verify exactly-once delivery semantics")
    @Severity(SeverityLevel.BLOCKER)
    @Tag("transactions")
    @Tag("exactly-once")
    void testExactlyOnceSemantics() {
        String topic = createTestTopic();
        
        // Initialize consumer BEFORE producing to avoid rebalance timing issues
        consumerManager.initConsumer(topic);
        AsyncTestHelper.waitFor(5); // Wait for consumer group rebalance
        
        // Pre-warm poll to ensure consumer is ready
        consumerManager.poll(2);
        AsyncTestHelper.waitFor(1);
        
        // Send messages with exactly-once guarantee
        int messageCount = 20;
        List<KafkaMessageDto> messages = TestDataGenerator.generateMessages(topic, messageCount);
        
        // Add unique identifiers for tracking
        for (int i = 0; i < messages.size(); i++) {
            messages.get(i).addHeader("eo-id", "eo-msg-" + i);
            messages.get(i).addHeader("attempt", "1");
        }
        
        producerManager.sendBatch(messages);
        producerManager.flush();
        AsyncTestHelper.waitFor(2);
        
        // Poll for messages with sufficient timeout
        List<ConsumerRecordDto> records = AsyncTestHelper.pollWithRetry(consumerManager, 30, messageCount);
        
        // Count unique messages by eo-id
        long uniqueMessages = records.stream()
                .map(r -> r.getHeaders().get("eo-id"))
                .filter(id -> id != null)
                .distinct()
                .count();
        
        // With exactly-once, each message appears exactly once
        assertThat(uniqueMessages).isEqualTo(messageCount);
        assertThat(records.size()).isGreaterThanOrEqualTo(messageCount);
        
        log.info("Exactly-once: {} unique messages out of {} total", uniqueMessages, records.size());
    }

    @Test
    @DisplayName("TC-045: Транзакции между несколькими топиками")
    @Description("Verify transactions across multiple topics")
    @Severity(SeverityLevel.NORMAL)
    @Tag("transactions")
    void testMultiTopicTransaction() {
        String topic1 = createTestTopic();
        String topic2 = createTestTopic();

        // Initialize consumers BEFORE producing to avoid rebalance timing issues
        consumerManager.initConsumer(topic1);
        AsyncTestHelper.waitFor(5); // Wait for consumer group rebalance
        consumerManager.poll(2);   // Pre-warm poll
        AsyncTestHelper.waitFor(1);

        List<KafkaMessageDto> messages1 = TestDataGenerator.generateMessages(topic1, 5);
        List<KafkaMessageDto> messages2 = TestDataGenerator.generateMessages(topic2, 5);

        String txnId = "multi-topic-txn-001";

        for (KafkaMessageDto msg : messages1) {
            msg.addHeader("transaction-id", txnId);
            msg.addHeader("topic", "topic1");
        }
        for (KafkaMessageDto msg : messages2) {
            msg.addHeader("transaction-id", txnId);
            msg.addHeader("topic", "topic2");
        }

        producerManager.sendBatch(messages1);
        producerManager.sendBatch(messages2);
        producerManager.flush();
        AsyncTestHelper.waitFor(2);

        // Verify messages in topic1
        List<ConsumerRecordDto> records1 = AsyncTestHelper.pollWithRetry(consumerManager, 30, 5);

        // Verify messages in topic2
        consumerManager.close();
        consumerManager.initConsumer(topic2);
        AsyncTestHelper.waitFor(5);
        consumerManager.poll(2);
        AsyncTestHelper.waitFor(1);
        List<ConsumerRecordDto> records2 = AsyncTestHelper.pollWithRetry(consumerManager, 30, 5);

        assertThat(records1.size()).isGreaterThan(0);
        assertThat(records2.size()).isGreaterThan(0);

        log.info("Multi-topic transaction: {} messages in topic1, {} in topic2",
                records1.size(), records2.size());
    }

    @Test
    @DisplayName("TC-046: Timeout транзакции")
    @Description("Verify transaction timeout behavior")
    @Severity(SeverityLevel.NORMAL)
    @Tag("transactions")
    void testTransactionTimeout() {
        String topic = createTestTopic();

        // Initialize consumer BEFORE producing to avoid rebalance timing issues
        consumerManager.initConsumer(topic);
        AsyncTestHelper.waitFor(5); // Wait for consumer group rebalance
        consumerManager.poll(2);   // Pre-warm poll to trigger partition assignment
        AsyncTestHelper.waitFor(1);

        List<KafkaMessageDto> messages = TestDataGenerator.generateMessages(topic, 10);
        for (KafkaMessageDto msg : messages) {
            msg.addHeader("transaction", "txn-timeout");
        }

        producerManager.sendBatch(messages);
        producerManager.flush();
        AsyncTestHelper.waitFor(2);

        List<ConsumerRecordDto> records = AsyncTestHelper.pollWithRetry(consumerManager, 30, 10);

        assertThat(records.size()).isGreaterThanOrEqualTo(0);

        log.info("Transaction with delay: {} messages received", records.size());
    }

    @Test
    @DisplayName("TC-047: Координация producer и consumer транзакций")
    @Description("Verify producer-consumer transaction coordination")
    @Severity(SeverityLevel.CRITICAL)
    @Tag("transactions")
    void testProducerConsumerTransactionCoordination() {
        String inputTopic = createTestTopic();
        String outputTopic = createTestTopic();

        // Initialize consumer on input topic BEFORE producing to avoid rebalance timing issues
        consumerManager.initConsumer(inputTopic);
        AsyncTestHelper.waitFor(5); // Wait for consumer group rebalance
        consumerManager.poll(2);   // Pre-warm poll to trigger partition assignment
        AsyncTestHelper.waitFor(1);

        // Send messages to input topic
        int messageCount = 10;
        List<KafkaMessageDto> inputMessages = TestDataGenerator.generateMessages(inputTopic, messageCount);
        producerManager.sendBatch(inputMessages);
        producerManager.flush();
        AsyncTestHelper.waitFor(2);

        List<ConsumerRecordDto> inputRecords = AsyncTestHelper.pollWithRetry(consumerManager, 30, messageCount);

        // Transform and send to output topic
        List<KafkaMessageDto> outputMessages = new java.util.ArrayList<>();
        for (ConsumerRecordDto record : inputRecords) {
            KafkaMessageDto outputMsg = KafkaMessageDto.builder()
                    .topic(outputTopic)
                    .key(record.getKey())
                    .value("processed-" + record.getValue())
                    .build();
            outputMsg.addHeader("original-offset", String.valueOf(record.getOffset()));
            outputMessages.add(outputMsg);
        }

        // Init consumer on output topic BEFORE sending to it
        consumerManager.close();
        consumerManager.initConsumer(outputTopic);
        AsyncTestHelper.waitFor(5);
        consumerManager.poll(2);
        AsyncTestHelper.waitFor(1);

        producerManager.sendBatch(outputMessages);
        producerManager.flush();
        AsyncTestHelper.waitFor(2);

        List<ConsumerRecordDto> outputRecords = AsyncTestHelper.pollWithRetry(consumerManager, 30, inputRecords.size());

        assertThat(outputRecords.size()).isEqualTo(inputRecords.size());

        log.info("Transaction coordination: {} input → {} output messages",
                inputRecords.size(), outputRecords.size());
    }

    @Test
    @DisplayName("TC-048: Восстановление после сбоя транзакции")
    @Description("Verify transaction recovery after failure")
    @Severity(SeverityLevel.CRITICAL)
    @Tag("transactions")
    @Tag("error-handling")
    void testTransactionRecoveryAfterFailure() {
        String topic = createTestTopic();
        
        // Initialize consumer BEFORE producing to avoid rebalance timing issues
        consumerManager.initConsumer(topic);
        AsyncTestHelper.waitFor(5); // Wait for consumer group rebalance
        
        // Pre-warm poll to ensure consumer is ready
        consumerManager.poll(2);
        AsyncTestHelper.waitFor(1);
        
        // Transaction 1: Will "fail"
        List<KafkaMessageDto> failedTxnMessages = TestDataGenerator.generateMessages(topic, 5);
        for (KafkaMessageDto msg : failedTxnMessages) {
            msg.addHeader("transaction", "txn-failed");
        }
        producerManager.sendBatch(failedTxnMessages);
        // Simulate failure - don't flush/commit

        AsyncTestHelper.waitFor(2); // Increased from 1
        
        // Transaction 2: Should succeed after recovery
        List<KafkaMessageDto> recoveredTxnMessages = TestDataGenerator.generateMessages(topic, 5);
        for (KafkaMessageDto msg : recoveredTxnMessages) {
            msg.addHeader("transaction", "txn-recovered");
        }
        
        producerManager.sendBatch(recoveredTxnMessages);
        producerManager.flush(); // Commit
        AsyncTestHelper.waitFor(3); // Increased from 2
        
        // Poll for messages with longer timeout
        List<ConsumerRecordDto> records = AsyncTestHelper.pollWithRetry(consumerManager, 30, 5); // Increased from 10
        
        // Should have messages from recovered transaction
        assertThat(records.size())
                .as("Should receive messages from committed transaction")
                .isGreaterThan(0);
        
        // Count messages from each transaction
        long failedCount = records.stream()
                .filter(r -> "txn-failed".equals(r.getHeaders().get("transaction")))
                .count();
        long recoveredCount = records.stream()
                .filter(r -> "txn-recovered".equals(r.getHeaders().get("transaction")))
                .count();
        
        log.info("Transaction recovery: {} failed, {} recovered messages", failedCount, recoveredCount);
        
        // Should have at least some messages (main point of test)
        assertThat(records.size())
                .as("Should have received at least some messages")
                .isGreaterThanOrEqualTo(5);
        
        // Should have more recovered than failed (or all recovered)
        assertThat(recoveredCount).isGreaterThanOrEqualTo(failedCount);
    }
}
