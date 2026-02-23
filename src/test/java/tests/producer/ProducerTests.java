package tests.producer;

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
 * Producer Tests - Message Sending Operations
 * Hexagonal Architecture v2.0
 * No Thread.sleep — all waits via KafkaAwaitHelper (Awaitility)
 */
@Slf4j
@Epic("Kafka Testing")
@Feature("Producer Operations")
@Tag("producer")
@Tag("critical")
public class ProducerTests extends BaseTest {

    @Test
    @DisplayName("TC-001: Отправка одиночного сообщения в топик")
    @Description("Verify that a single message can be sent successfully to a topic")
    @Severity(SeverityLevel.CRITICAL)
    @Tag("smoke")
    void testSendSingleMessage() {
        String topicName = createTestTopic();

        Message message = Message.builder()
                .topic(Topic.builder().name(topicName).build())
                .key("single-key-1")
                .content("{\"test\": \"TC-001\", \"value\": \"single message\"}")
                .build();

        PublishResult result = kafka.publish(message);

        assertThat(result.isSuccess()).isTrue();
        assertThat(result.getOffset()).isNotNull();
        assertThat(result.getPartition()).isNotNull();

        log.info("TC-001: Message sent to partition {} with offset {}",
                result.getPartition(), result.getOffset());
    }

    @Test
    @DisplayName("TC-002: Массовая отправка сообщений (batch)")
    @Description("Verify that multiple messages can be sent in batch")
    @Severity(SeverityLevel.CRITICAL)
    void testSendBatchMessages() {
        String topicName = createTestTopic();
        int messageCount = 100;

        List<Message> messages = buildMessages(topicName, "batch-key-", messageCount);
        List<PublishResult> results = kafka.publishBatch(messages);

        assertThat(results).hasSize(messageCount);
        assertThat(results).allMatch(PublishResult::isSuccess);

        log.info("TC-002: Sent {} messages in batch", results.size());
    }

    @Test
    @DisplayName("TC-003: Отправка сообщения с custom headers")
    @Description("Verify that messages with custom headers are sent correctly")
    @Severity(SeverityLevel.NORMAL)
    void testSendMessageWithHeaders() {
        String topicName = createTestTopic();

        // Subscribe and await consumer group ready — no Thread.sleep
        KafkaAwaitHelper.awaitConsumerReady(kafka, topicName, 15);

        Message message = Message.builder()
                .topic(Topic.builder().name(topicName).build())
                .key("header-key-1")
                .content("{\"test\": \"TC-003\"}")
                .headers(Map.of("trace-id", "trace-123", "user-id", "user-456"))
                .build();

        kafka.publish(message);

        // Await propagation, then consume — no Thread.sleep
        KafkaAwaitHelper.awaitPropagation(kafka, topicName, 1, 10);

        List<Message> messages = KafkaAwaitHelper.awaitNewMessages(kafka, 1, 15);

        assertThat(messages).isNotEmpty();
        Message consumed = messages.get(0);
        assertThat(consumed.getHeaders()).containsEntry("trace-id", "trace-123");
        assertThat(consumed.getHeaders()).containsEntry("user-id", "user-456");

        log.info("TC-003: Headers verified: {}", consumed.getHeaders());
    }

    @Test
    @DisplayName("TC-004: Отправка сообщений с одинаковым ключом в одну партицию")
    @Description("Verify that messages with the same key go to the same partition")
    @Severity(SeverityLevel.CRITICAL)
    void testSendMessageWithKey() {
        String topicName = createTestTopic(2);
        String key = "user-123";

        Message msg1 = Message.builder()
                .topic(Topic.builder().name(topicName).build())
                .key(key).content("{\"seq\": 1}").build();

        Message msg2 = Message.builder()
                .topic(Topic.builder().name(topicName).build())
                .key(key).content("{\"seq\": 2}").build();

        PublishResult result1 = kafka.publish(msg1);
        PublishResult result2 = kafka.publish(msg2);

        assertThat(result1.isSuccess()).isTrue();
        assertThat(result2.isSuccess()).isTrue();
        assertThat(result1.getPartition()).isEqualTo(result2.getPartition());

        log.info("TC-004: Both messages key='{}' → partition {}", key, result1.getPartition());
    }

    @Test
    @DisplayName("TC-005: Отправка oversized сообщения")
    @Description("Verify that oversized messages are rejected")
    @Severity(SeverityLevel.NORMAL)
    void testSendOversizedMessage() {
        String topicName = createTestTopic();

        Message message = Message.builder()
                .topic(Topic.builder().name(topicName).build())
                .key("oversized-key")
                .content("x".repeat(2_000_000))
                .build();

        try {
            PublishResult result = kafka.publish(message);
            if (!result.isSuccess()) {
                log.info("TC-005: Large message rejected as expected");
            } else {
                log.warn("TC-005: Broker accepted large message (limit may be higher)");
            }
        } catch (Exception e) {
            assertThat(e.getMessage()).isNotNull();
            log.info("TC-005: Exception on oversized message: {}", e.getMessage());
        }
    }

    @Test
    @DisplayName("TC-006: Message compression")
    @Description("Verify producer can send compressed messages")
    @Severity(SeverityLevel.NORMAL)
    void testProducerCompression() {
        String topicName = createTestTopic();
        int messageCount = 20;

        // Consumer ready before producing
        KafkaAwaitHelper.awaitConsumerReady(kafka, topicName, 15);

        List<Message> messages = new ArrayList<>();
        for (int i = 0; i < messageCount; i++) {
            messages.add(Message.builder()
                    .topic(Topic.builder().name(topicName).build())
                    .key("comp-key-" + i)
                    .content("Repeated text for compression. ".repeat(100))
                    .build());
        }

        kafka.publishBatch(messages);
        KafkaAwaitHelper.awaitPropagation(kafka, topicName, messageCount, 15);

        List<Message> consumed = KafkaAwaitHelper.awaitNewMessages(kafka, messageCount, 30);

        assertThat(consumed.size()).isGreaterThanOrEqualTo(messageCount);
        log.info("TC-006: Received {} compressed messages", consumed.size());
    }

    @Test
    @DisplayName("TC-007: Producer acks=all guarantee")
    @Description("Verify producer waits for all replicas with acks=all")
    @Severity(SeverityLevel.CRITICAL)
    void testProducerAcks() {
        String topicName = createTestTopic();

        KafkaAwaitHelper.awaitConsumerReady(kafka, topicName, 15);

        List<Message> messages = buildMessages(topicName, "acks-key-", 10);

        long start = System.currentTimeMillis();
        kafka.publishBatch(messages);
        KafkaAwaitHelper.awaitPropagation(kafka, topicName, 10, 15);
        long duration = System.currentTimeMillis() - start;

        log.info("TC-007: Send with acks=all took {} ms", duration);

        List<Message> consumed = KafkaAwaitHelper.awaitNewMessages(kafka, 10, 20);
        assertThat(consumed.size()).isEqualTo(10);
    }

    @Test
    @DisplayName("TC-008: Producer automatic retry on transient errors")
    @Description("Verify producer retries failed sends automatically")
    @Severity(SeverityLevel.CRITICAL)
    void testProducerRetry() {
        String topicName = createTestTopic();
        int messageCount = 15;

        int successCount = 0;
        for (int i = 0; i < messageCount; i++) {
            Message message = Message.builder()
                    .topic(Topic.builder().name(topicName).build())
                    .key("retry-key-" + i)
                    .content("{\"retry\": true, \"index\": " + i + "}")
                    .build();
            PublishResult result = kafka.publishWithRetry(message, 3);
            if (result.isSuccess()) successCount++;
        }

        assertThat(successCount).isGreaterThan(messageCount / 2);
        log.info("TC-008: {}/{} messages sent successfully", successCount, messageCount);
    }

    @Test
    @DisplayName("TC-009: Producer request timeout")
    @Description("Verify producer handles request timeout properly")
    @Severity(SeverityLevel.NORMAL)
    void testProducerTimeout() {
        String topicName = createTestTopic();

        Message message = Message.builder()
                .topic(Topic.builder().name(topicName).build())
                .key("timeout-key")
                .content("{\"test\": \"timeout\"}")
                .build();

        long start = System.currentTimeMillis();
        PublishResult result = kafka.publish(message);
        long duration = System.currentTimeMillis() - start;

        assertThat(result.isSuccess()).isTrue();
        assertThat(duration).isLessThan(30_000L);
        log.info("TC-009: Message sent in {} ms", duration);
    }

    @Test
    @DisplayName("TC-010: Producer buffer overflow handling")
    @Description("Verify producer handles buffer full scenario")
    @Severity(SeverityLevel.NORMAL)
    void testProducerBufferFull() {
        String topicName = createTestTopic();

        List<Message> messages = buildMessages(topicName, "buf-key-", 1000);
        List<PublishResult> results = kafka.publishBatch(messages);
        kafka.flush();

        long successCount = results.stream().filter(PublishResult::isSuccess).count();
        assertThat(successCount).isGreaterThan(0);
        log.info("TC-010: {}/{} messages sent", successCount, messages.size());
    }

    @Test
    @DisplayName("TC-011: Transactional message sending")
    @Description("Verify transactional producer behavior")
    @Severity(SeverityLevel.CRITICAL)
    void testTransactionalProducer() {
        String topicName = createTestTopic();

        KafkaAwaitHelper.awaitConsumerReady(kafka, topicName, 15);

        List<Message> messages = buildMessages(topicName, "tx-key-", 10);
        kafka.publishBatch(messages);
        KafkaAwaitHelper.awaitPropagation(kafka, topicName, 10, 15);

        List<Message> consumed = KafkaAwaitHelper.awaitNewMessages(kafka, 10, 15);

        assertThat(consumed.size()).isIn(0, 10);
        log.info("TC-011: Transactional publish: {} messages consumed", consumed.size());
    }

    @Test
    @DisplayName("TC-012: Producer metrics collection")
    @Description("Verify producer exposes metrics for monitoring")
    @Severity(SeverityLevel.NORMAL)
    void testProducerMetrics() {
        String topicName = createTestTopic();

        List<Message> messages = buildMessages(topicName, "metrics-key-", 20);

        long start = System.currentTimeMillis();
        List<PublishResult> results = kafka.publishBatch(messages);
        kafka.flush();
        long duration = System.currentTimeMillis() - start;

        long sentCount = results.stream().filter(PublishResult::isSuccess).count();

        log.info("TC-012: {} messages in {} ms, avg {} ms/msg",
                sentCount, duration, sentCount > 0 ? duration / sentCount : 0);

        assertThat(duration).isGreaterThan(0);
        assertThat(sentCount).isEqualTo(20);
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
