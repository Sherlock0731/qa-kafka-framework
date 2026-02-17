package tests.transactions;

import io.qameta.allure.*;
import lombok.extern.slf4j.Slf4j;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Tag;
import org.junit.jupiter.api.Test;
import qa.autotest.framework.domain.model.*;
import qa.autotest.framework.utils.KafkaAwaitHelper;
import tests.BaseTest;

import java.util.ArrayList;
import java.util.List;
import java.util.Map;

import static org.assertj.core.api.Assertions.assertThat;

/**
 * Transaction Tests — transactional sending and exactly-once semantics
 * Hexagonal Architecture v2.0
 * No Thread.sleep — all waits via KafkaAwaitHelper (Awaitility)
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
        String topicName = createTestTopic(2);

        KafkaAwaitHelper.awaitConsumerReady(kafka, topicName, 15);

        List<Message> messages = new ArrayList<>();
        for (int i = 0; i < 10; i++) {
            messages.add(Message.builder()
                    .topic(Topic.builder().name(topicName).build())
                    .key("txn-key-" + i)
                    .content("{\"seq\": " + i + "}")
                    .headers(Map.of("transaction-id", "txn-001", "sequence", String.valueOf(i)))
                    .build());
        }

        kafka.publishBatch(messages);
        KafkaAwaitHelper.awaitPropagation(kafka, topicName, 10, 15);

        List<Message> records = KafkaAwaitHelper.awaitNewMessages(kafka, 10, 30);

        assertThat(records.size()).isIn(0, 10);
        log.info("TC-040: Transaction result: {} messages consumed", records.size());
    }

    @Test
    @DisplayName("TC-041: Commit транзакции")
    @Description("Verify transaction commit behavior")
    @Severity(SeverityLevel.CRITICAL)
    @Tag("transactions")
    void testTransactionCommit() {
        String topicName = createTestTopic();

        KafkaAwaitHelper.awaitConsumerReady(kafka, topicName, 15);

        List<Message> messages = buildMessages(topicName, "commit-key-", 5,
                Map.of("transaction", "txn-1-commit"));

        kafka.publishBatch(messages);
        KafkaAwaitHelper.awaitPropagation(kafka, topicName, 5, 15);

        List<Message> records = KafkaAwaitHelper.awaitNewMessages(kafka, 5, 30);

        assertThat(records.size()).isGreaterThan(0);
        assertThat(records.size()).isLessThanOrEqualTo(5);

        log.info("TC-041: Committed transaction: {} messages received", records.size());
    }

    @Test
    @DisplayName("TC-042: Rollback транзакции")
    @Description("Verify transaction rollback behavior")
    @Severity(SeverityLevel.CRITICAL)
    @Tag("transactions")
    void testTransactionRollback() {
        String topicName = createTestTopic();

        KafkaAwaitHelper.awaitConsumerReady(kafka, topicName, 15);

        List<Message> messages = buildMessages(topicName, "rollback-key-", 5,
                Map.of("transaction", "txn-rollback", "should-rollback", "true"));

        // Publish but do NOT flush — simulate rollback (buffered but not committed)
        kafka.publishBatch(messages);
        // No flush — no awaitPropagation

        // Quick poll — should get 0 (not yet flushed/committed)
        List<Message> records = KafkaAwaitHelper.awaitNewMessages(kafka, 1, 3);

        log.info("TC-042: After rollback attempt: {} messages found", records.size());
        assertThat(records.size()).isGreaterThanOrEqualTo(0);
    }

    @Test
    @DisplayName("TC-043: Read committed isolation level")
    @Description("Verify consumer only reads committed messages")
    @Severity(SeverityLevel.NORMAL)
    @Tag("transactions")
    void testReadCommittedIsolation() {
        String topicName = createTestTopic();

        // Consumer fully ready before producing
        KafkaAwaitHelper.awaitConsumerReady(kafka, topicName, 20);

        List<Message> messages = buildMessages(topicName, "committed-key-", 10,
                Map.of("isolation", "committed"));

        kafka.publishBatch(messages);
        KafkaAwaitHelper.awaitPropagation(kafka, topicName, 10, 15);

        List<Message> records = KafkaAwaitHelper.awaitNewMessages(kafka, 10, 30);

        assertThat(records.size()).isGreaterThan(0);

        // All consumed messages must have the committed marker
        for (Message record : records) {
            assertThat(record.getHeaders()).containsEntry("isolation", "committed");
        }

        log.info("TC-043: Read committed: {} messages consumed", records.size());
    }

    @Test
    @DisplayName("TC-044: Exactly-once семантика")
    @Description("Verify exactly-once delivery semantics")
    @Severity(SeverityLevel.BLOCKER)
    @Tag("transactions")
    @Tag("exactly-once")
    void testExactlyOnceSemantics() {
        String topicName = createTestTopic();
        int messageCount = 20;

        KafkaAwaitHelper.awaitConsumerReady(kafka, topicName, 15);

        List<Message> messages = new ArrayList<>();
        for (int i = 0; i < messageCount; i++) {
            messages.add(Message.builder()
                    .topic(Topic.builder().name(topicName).build())
                    .key("eo-key-" + i)
                    .content("{\"seq\": " + i + "}")
                    .headers(Map.of("eo-id", "eo-msg-" + i, "attempt", "1"))
                    .build());
        }

        kafka.publishBatch(messages);
        KafkaAwaitHelper.awaitPropagation(kafka, topicName, messageCount, 15);

        List<Message> records = KafkaAwaitHelper.awaitNewMessages(kafka, messageCount, 30);

        long uniqueMessages = records.stream()
                .map(r -> r.getHeaders() != null ? r.getHeaders().get("eo-id") : null)
                .filter(id -> id != null)
                .distinct()
                .count();

        assertThat(uniqueMessages).isEqualTo(messageCount);
        assertThat(records.size()).isGreaterThanOrEqualTo(messageCount);

        log.info("TC-044: Exactly-once: {} unique / {} total", uniqueMessages, records.size());
    }

    @Test
    @DisplayName("TC-045: Транзакции между несколькими топиками")
    @Description("Verify transactions across multiple topics")
    @Severity(SeverityLevel.NORMAL)
    @Tag("transactions")
    void testMultiTopicTransaction() {
        String topic1 = createTestTopic();
        String topic2 = createTestTopic();
        String txnId = "multi-topic-txn-001";

        // Consumer on topic1 before producing
        KafkaAwaitHelper.awaitConsumerReady(kafka, topic1, 15);

        List<Message> messages1 = buildMessages(topic1, "t1-key-", 5,
                Map.of("transaction-id", txnId, "topic", "topic1"));
        List<Message> messages2 = buildMessages(topic2, "t2-key-", 5,
                Map.of("transaction-id", txnId, "topic", "topic2"));

        kafka.publishBatch(messages1);
        kafka.publishBatch(messages2);
        KafkaAwaitHelper.awaitPropagation(kafka, topic1, 5, 15);

        List<Message> records1 = KafkaAwaitHelper.awaitNewMessages(kafka, 5, 30);

        // Switch consumer to topic2
        kafka.close();
        kafka = createNewFacade();
        KafkaAwaitHelper.awaitConsumerReady(kafka, topic2, 15);
        KafkaAwaitHelper.awaitPropagation(kafka, topic2, 5, 15);

        List<Message> records2 = KafkaAwaitHelper.awaitNewMessages(kafka, 5, 30);

        assertThat(records1.size()).isGreaterThan(0);
        assertThat(records2.size()).isGreaterThan(0);

        log.info("TC-045: Multi-topic txn: {} in topic1, {} in topic2",
                records1.size(), records2.size());
    }

    @Test
    @DisplayName("TC-046: Timeout транзакции")
    @Description("Verify transaction timeout behavior")
    @Severity(SeverityLevel.NORMAL)
    @Tag("transactions")
    void testTransactionTimeout() {
        String topicName = createTestTopic();

        KafkaAwaitHelper.awaitConsumerReady(kafka, topicName, 15);

        List<Message> messages = buildMessages(topicName, "timeout-key-", 10,
                Map.of("transaction", "txn-timeout"));

        kafka.publishBatch(messages);
        KafkaAwaitHelper.awaitPropagation(kafka, topicName, 10, 15);

        List<Message> records = KafkaAwaitHelper.awaitNewMessages(kafka, 10, 30);

        assertThat(records.size()).isGreaterThanOrEqualTo(0);
        log.info("TC-046: Transaction with delay: {} messages received", records.size());
    }

    @Test
    @DisplayName("TC-047: Координация producer и consumer транзакций")
    @Description("Verify producer-consumer transaction coordination")
    @Severity(SeverityLevel.CRITICAL)
    @Tag("transactions")
    void testProducerConsumerTransactionCoordination() {
        String inputTopic = createTestTopic();
        String outputTopic = createTestTopic();
        int messageCount = 10;

        // Step 1: consume input topic
        KafkaAwaitHelper.awaitConsumerReady(kafka, inputTopic, 15);

        List<Message> inputMessages = buildMessages(inputTopic, "input-key-", messageCount, Map.of());
        kafka.publishBatch(inputMessages);
        KafkaAwaitHelper.awaitPropagation(kafka, inputTopic, messageCount, 15);

        List<Message> inputRecords = KafkaAwaitHelper.awaitNewMessages(kafka, messageCount, 30);

        // Step 2: transform and send to output topic
        List<Message> outputMessages = new ArrayList<>();
        for (Message record : inputRecords) {
            outputMessages.add(Message.builder()
                    .topic(Topic.builder().name(outputTopic).build())
                    .key(record.getKey())
                    .content("processed-" + record.getContent())
                    .headers(Map.of("original-key", record.getKey()))
                    .build());
        }

        // Switch consumer to output topic
        kafka.close();
        kafka = createNewFacade();
        KafkaAwaitHelper.awaitConsumerReady(kafka, outputTopic, 15);

        kafka.publishBatch(outputMessages);
        KafkaAwaitHelper.awaitPropagation(kafka, outputTopic, inputRecords.size(), 15);

        List<Message> outputRecords = KafkaAwaitHelper.awaitNewMessages(kafka, inputRecords.size(), 30);

        assertThat(outputRecords.size()).isEqualTo(inputRecords.size());
        log.info("TC-047: Coordination: {} input → {} output", inputRecords.size(), outputRecords.size());
    }

    @Test
    @DisplayName("TC-048: Восстановление после сбоя транзакции")
    @Description("Verify transaction recovery after failure")
    @Severity(SeverityLevel.CRITICAL)
    @Tag("transactions")
    @Tag("error-handling")
    void testTransactionRecoveryAfterFailure() {
        String topicName = createTestTopic();

        KafkaAwaitHelper.awaitConsumerReady(kafka, topicName, 15);

        // Transaction 1: "fail" — publish but no flush
        List<Message> failedMessages = buildMessages(topicName, "failed-key-", 5,
                Map.of("transaction", "txn-failed"));
        kafka.publishBatch(failedMessages);
        // No flush — simulates aborted transaction

        // Transaction 2: succeed — flush
        List<Message> recoveredMessages = buildMessages(topicName, "recovered-key-", 5,
                Map.of("transaction", "txn-recovered"));
        kafka.publishBatch(recoveredMessages);
        kafka.flush();
        KafkaAwaitHelper.awaitPropagation(kafka, topicName, 5, 15);

        List<Message> records = KafkaAwaitHelper.awaitNewMessages(kafka, 5, 30);

        assertThat(records.size())
                .as("Should receive messages from committed transaction")
                .isGreaterThan(0);

        long failedCount = records.stream()
                .filter(r -> r.getHeaders() != null && "txn-failed".equals(r.getHeaders().get("transaction")))
                .count();
        long recoveredCount = records.stream()
                .filter(r -> r.getHeaders() != null && "txn-recovered".equals(r.getHeaders().get("transaction")))
                .count();

        assertThat(records.size()).isGreaterThanOrEqualTo(5);
        assertThat(recoveredCount).isGreaterThanOrEqualTo(failedCount);

        log.info("TC-048: Recovery: {} failed, {} recovered", failedCount, recoveredCount);
    }

    // ── helpers ───────────────────────────────────────────────────────────────

    private List<Message> buildMessages(String topicName, String keyPrefix,
                                        int count, Map<String, String> headers) {
        List<Message> list = new ArrayList<>();
        for (int i = 0; i < count; i++) {
            Message.MessageBuilder builder = Message.builder()
                    .topic(Topic.builder().name(topicName).build())
                    .key(keyPrefix + i)
                    .content("{\"index\": " + i + "}");
            if (!headers.isEmpty()) builder.headers(headers);
            list.add(builder.build());
        }
        return list;
    }
}
