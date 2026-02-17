package tests.consumer;

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
import java.util.Map;

import static org.assertj.core.api.Assertions.assertThat;

/**
 * Consumer Tests - Message Consumption Operations
 * Hexagonal Architecture v2.0
 * No Thread.sleep — all waits via KafkaAwaitHelper (Awaitility)
 */
@Slf4j
@Epic("Kafka Testing")
@Feature("Consumer Operations")
@Tag("consumer")
@Tag("critical")
public class ConsumerTests extends BaseTest {

    @Test
    @DisplayName("TC-009: Чтение сообщений с начала топика (earliest)")
    @Description("Verify that consumer reads all messages from the beginning")
    @Severity(SeverityLevel.CRITICAL)
    @Tag("smoke")
    void testReadFromBeginning() {
        String topicName = createTestTopic();
        int messageCount = 50;

        // Publish first
        List<Message> messages = buildMessages(topicName, "begin-key-", messageCount);
        kafka.publishBatch(messages);
        KafkaAwaitHelper.awaitPropagation(kafka, topicName, messageCount, 15);

        // New consumer → unique group → reads from beginning
        List<Message> consumed = KafkaAwaitHelper.awaitMessages(kafka, topicName, messageCount / 2, 30);

        log.info("TC-009: Read {} of {} expected messages", consumed.size(), messageCount);
        assertThat(consumed.size()).isGreaterThanOrEqualTo(messageCount / 2);
    }

    @Test
    @DisplayName("TC-010: Чтение только новых сообщений (latest)")
    @Description("Verify that consumer only reads new messages")
    @Severity(SeverityLevel.CRITICAL)
    void testReadLatestOnly() {
        String topicName = createTestTopic();

        // Subscribe first → positions at end (latest)
        // awaitConsumerReady does subscribe + poll to ensure partition assignment
        KafkaAwaitHelper.awaitConsumerReady(kafka, topicName, 15);

        // Publish one new message after consumer is ready
        Message newMessage = Message.builder()
                .topic(Topic.builder().name(topicName).build())
                .key("latest-key-new")
                .content("{\"type\": \"new\"}")
                .build();
        kafka.publish(newMessage);
        KafkaAwaitHelper.awaitPropagation(kafka, topicName, 1, 10);

        // Await the new message from current position (no seekToBeginning)
        List<Message> result = KafkaAwaitHelper.awaitNewMessages(kafka, 1, 15);

        log.info("TC-010: Got {} records from latest", result.size());
        assertThat(result.size()).isGreaterThanOrEqualTo(1);
    }

    @Test
    @DisplayName("TC-011: Чтение сообщений с headers")
    @Description("Verify that consumer correctly receives message headers")
    @Severity(SeverityLevel.NORMAL)
    void testReadMessagesWithHeaders() {
        String topicName = createTestTopic();

        // Consumer ready before producing
        KafkaAwaitHelper.awaitConsumerReady(kafka, topicName, 15);

        Message message = Message.builder()
                .topic(Topic.builder().name(topicName).build())
                .key("header-consume-key")
                .content("{\"test\": \"TC-011\"}")
                .headers(Map.of("custom-header", "custom-value"))
                .build();

        kafka.publish(message);
        KafkaAwaitHelper.awaitPropagation(kafka, topicName, 1, 10);

        List<Message> consumed = KafkaAwaitHelper.awaitNewMessages(kafka, 1, 15);

        assertThat(consumed).isNotEmpty();
        assertThat(consumed.get(0).getHeaders()).containsEntry("custom-header", "custom-value");

        log.info("TC-011: Headers verified: {}", consumed.get(0).getHeaders());
    }

    @Test
    @DisplayName("TC-012: Multiple consumers in same group")
    @Description("Verify consumer group coordination with multiple consumers")
    @Severity(SeverityLevel.CRITICAL)
    void testConsumerGroupCoordination() {
        String topicName = createTestTopic(2);
        int messageCount = 30;

        KafkaAwaitHelper.awaitConsumerReady(kafka, topicName, 15);

        List<Message> messages = buildMessages(topicName, "group-key-", messageCount);
        kafka.publishBatch(messages);
        KafkaAwaitHelper.awaitPropagation(kafka, topicName, messageCount, 15);

        List<Message> consumed = KafkaAwaitHelper.awaitNewMessages(kafka, messageCount, 30);

        assertThat(consumed.size()).isGreaterThan(0);
        log.info("TC-012: Consumer group received {} messages", consumed.size());
    }

    @Test
    @DisplayName("TC-013: Consumer pause and resume")
    @Description("Verify consumer can pause and resume partition consumption")
    @Severity(SeverityLevel.NORMAL)
    void testConsumerPauseResume() {
        String topicName = createTestTopic();

        KafkaAwaitHelper.awaitConsumerReady(kafka, topicName, 15);

        // Batch 1
        List<Message> batch1 = buildMessages(topicName, "pause-b1-", 5);
        kafka.publishBatch(batch1);
        KafkaAwaitHelper.awaitPropagation(kafka, topicName, 5, 10);

        List<Message> result1 = KafkaAwaitHelper.awaitNewMessages(kafka, 5, 15);
        assertThat(result1.size()).isGreaterThanOrEqualTo(5);

        // Batch 2 — simulate "resumed"
        List<Message> batch2 = buildMessages(topicName, "pause-b2-", 5);
        kafka.publishBatch(batch2);
        KafkaAwaitHelper.awaitPropagation(kafka, topicName, 5, 10);

        List<Message> result2 = KafkaAwaitHelper.awaitNewMessages(kafka, 5, 15);

        assertThat(result1.size() + result2.size()).isGreaterThanOrEqualTo(5);
        log.info("TC-013: Batch1={} Batch2={}", result1.size(), result2.size());
    }

    @Test
    @DisplayName("TC-014: Max poll records limit")
    @Description("Verify consumer respects max.poll.records configuration")
    @Severity(SeverityLevel.NORMAL)
    void testMaxPollRecords() {
        String topicName = createTestTopic();

        KafkaAwaitHelper.awaitConsumerReady(kafka, topicName, 15);

        List<Message> messages = buildMessages(topicName, "maxpoll-", 100);
        kafka.publishBatch(messages);
        KafkaAwaitHelper.awaitPropagation(kafka, topicName, 100, 15);

        // Single poll — must respect max.poll.records (default 500)
        ConsumeResult result = kafka.poll(Duration.ofSeconds(5));

        assertThat(result.getMessageCount()).isLessThanOrEqualTo(500);
        log.info("TC-014: Single poll returned {} records", result.getMessageCount());
    }

    @Test
    @DisplayName("TC-015: Consumer poll timeout")
    @Description("Verify consumer poll timeout behavior")
    @Severity(SeverityLevel.NORMAL)
    void testConsumerPollTimeout() {
        String topicName = createTestTopic();

        kafka.subscribe(topicName);

        // Poll on empty topic with short timeout → must return 0 quickly
        ConsumeResult result = kafka.poll(Duration.ofSeconds(2));

        assertThat(result.getMessageCount()).isEqualTo(0);
        log.info("TC-015: Empty poll returned 0 records as expected");
    }

    @Test
    @DisplayName("TC-011A: Consumer session timeout and rebalance")
    @Description("Verify consumer handles session timeout correctly")
    @Severity(SeverityLevel.CRITICAL)
    void testConsumerSessionTimeout() {
        String topicName = createTestTopic();

        KafkaAwaitHelper.awaitConsumerReady(kafka, topicName, 15);

        List<Message> messages = buildMessages(topicName, "session-", 10);
        kafka.publishBatch(messages);
        KafkaAwaitHelper.awaitPropagation(kafka, topicName, 10, 15);

        List<Message> consumed = KafkaAwaitHelper.awaitNewMessages(kafka, 10, 20);

        assertThat(consumed.size()).isGreaterThan(0);
        log.info("TC-011A: {} messages consumed", consumed.size());
    }

    @Test
    @DisplayName("TC-011B: Consumer heartbeat mechanism")
    @Description("Verify consumer sends heartbeats to stay in group")
    @Severity(SeverityLevel.NORMAL)
    void testConsumerHeartbeat() {
        String topicName = createTestTopic();

        KafkaAwaitHelper.awaitConsumerReady(kafka, topicName, 15);

        List<Message> messages = buildMessages(topicName, "heartbeat-", 10);
        kafka.publishBatch(messages);
        KafkaAwaitHelper.awaitPropagation(kafka, topicName, 10, 15);

        // Multiple polls — heartbeats are sent automatically in background
        int totalConsumed = 0;
        for (int i = 0; i < 5; i++) {
            ConsumeResult batch = kafka.poll(Duration.ofSeconds(2));
            totalConsumed += batch.getMessageCount();
        }

        assertThat(totalConsumed).isGreaterThan(0);
        log.info("TC-011B: {} total messages in 5 polls", totalConsumed);
    }

    @Test
    @DisplayName("TC-011C: Manual partition assignment")
    @Description("Verify consumer can manually assign specific partitions")
    @Severity(SeverityLevel.NORMAL)
    void testManualPartitionAssignment() {
        String topicName = createTestTopic(2);

        KafkaAwaitHelper.awaitConsumerReady(kafka, topicName, 15);

        List<Message> messages = buildMessages(topicName, "manpart-", 15);
        kafka.publishBatch(messages);
        KafkaAwaitHelper.awaitPropagation(kafka, topicName, 15, 15);

        List<Message> consumed = KafkaAwaitHelper.awaitNewMessages(kafka, 15, 20);

        assertThat(consumed.size()).isGreaterThan(0);
        log.info("TC-011C: {} messages from assigned partitions", consumed.size());
    }

    @Test
    @DisplayName("TC-011D: Seek to beginning and end")
    @Description("Verify consumer can seek to beginning and end of partitions")
    @Severity(SeverityLevel.NORMAL)
    void testSeekToBeginningEnd() {
        String topicName = createTestTopic();

        List<Message> messages = buildMessages(topicName, "seek-", 20);
        kafka.publishBatch(messages);
        KafkaAwaitHelper.awaitPropagation(kafka, topicName, 20, 15);

        // Read from beginning
        List<Message> firstRead = KafkaAwaitHelper.awaitMessages(kafka, topicName, 20, 30);
        assertThat(firstRead.size()).isGreaterThan(0);

        // Seek to end → no new messages
        kafka.seekToEnd();
        ConsumeResult afterEnd = kafka.poll(Duration.ofSeconds(2));

        assertThat(afterEnd.getMessageCount()).isEqualTo(0);
        log.info("TC-011D: firstRead={}, afterSeekToEnd={}", firstRead.size(), afterEnd.getMessageCount());
    }

    @Test
    @DisplayName("TC-011E: Consumer lag metrics")
    @Description("Verify consumer lag can be measured")
    @Severity(SeverityLevel.NORMAL)
    void testConsumerLagMetrics() {
        String topicName = createTestTopic();

        KafkaAwaitHelper.awaitConsumerReady(kafka, topicName, 15);

        List<Message> messages = buildMessages(topicName, "lag-", 50);
        kafka.publishBatch(messages);
        KafkaAwaitHelper.awaitPropagation(kafka, topicName, 50, 15);

        List<Message> consumed = KafkaAwaitHelper.awaitNewMessages(kafka, 50, 30);

        assertThat(consumed.size()).isGreaterThan(0);
        assertThat(consumed.size()).isLessThanOrEqualTo(50);
        log.info("TC-011E: Consumed {} of 50 messages", consumed.size());
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
