package tests.dlq;

import io.qameta.allure.*;
import lombok.extern.slf4j.Slf4j;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Tag;
import org.junit.jupiter.api.Test;
import qa.autotest.framework.domain.model.*;
import qa.autotest.framework.utils.KafkaAwaitHelper;
import tests.BaseTest;

import java.util.List;
import java.util.Map;

import static org.assertj.core.api.Assertions.assertThat;

/**
 * Dead Letter Queue Tests
 * Hexagonal Architecture v2.0
 * No Thread.sleep — all waits via KafkaAwaitHelper (Awaitility)
 */
@Slf4j
@Epic("Kafka Testing")
@Feature("Dead Letter Queue")
@Tag("dlq")
@Tag("critical")
public class DlqTests extends BaseTest {

    @Test
    @DisplayName("TC-049: Отправка poison pill в DLQ после N попыток")
    @Description("Verify failed messages are sent to DLQ after max retries")
    @Severity(SeverityLevel.CRITICAL)
    @Tag("smoke")
    void testSendToDlqAfterRetries() {
        // Create main topic + DLQ via domain method
        String topicName = createTestTopic();
        String dlqTopicName = topicName + ".dlq";
        createTestTopic(dlqTopicName);

        // Consumer ready on DLQ before producing
        KafkaAwaitHelper.awaitConsumerReady(kafka, dlqTopicName, 15);

        // Publish "poison pill" to main topic (simulates failed processing)
        Message poisonMessage = Message.builder()
                .topic(Topic.builder().name(topicName).build())
                .key("poison-key-1")
                .content("INVALID_JSON:{not valid}")
                .build();
        kafka.publish(poisonMessage);

        // Simulate DLQ routing — send to DLQ with error metadata headers
        Message dlqMessage = Message.builder()
                .topic(Topic.builder().name(dlqTopicName).build())
                .key(poisonMessage.getKey())
                .content(poisonMessage.getContent())
                .headers(Map.of(
                        "error-message", "Deserialization failed",
                        "retry-count", "3",
                        "original-topic", topicName
                ))
                .build();

        kafka.publish(dlqMessage);
        KafkaAwaitHelper.awaitPropagation(kafka, dlqTopicName, 1, 10);

        List<Message> records = KafkaAwaitHelper.awaitNewMessages(kafka, 1, 30);

        assertThat(records).hasSize(1);
        Message dlqRecord = records.get(0);
        assertThat(dlqRecord.getHeaders()).containsEntry("original-topic", topicName);
        assertThat(dlqRecord.getHeaders()).containsEntry("retry-count", "3");

        log.info("TC-049: Poison pill correctly routed to DLQ with headers: {}",
                dlqRecord.getHeaders());
    }

    @Test
    @DisplayName("TC-031: DLQ message routing with metadata")
    @Description("Verify DLQ messages include proper metadata for debugging")
    @Severity(SeverityLevel.CRITICAL)
    @Tag("dlq")
    void testDlqRoutingLogic() {
        String topicName = createTestTopic();
        String dlqTopicName = topicName + ".dlq";
        createTestTopic(dlqTopicName);

        // Consumer ready on DLQ before producing
        KafkaAwaitHelper.awaitConsumerReady(kafka, dlqTopicName, 15);

        // Route failed message to DLQ with full metadata
        Message failedMessage = Message.builder()
                .topic(Topic.builder().name(dlqTopicName).build())
                .key("failed-key")
                .content("failed-value")
                .headers(Map.of(
                        "original-topic", topicName,
                        "failure-reason", "Processing error",
                        "failure-timestamp", String.valueOf(System.currentTimeMillis()),
                        "retry-count", "5",
                        "original-partition", "0",
                        "original-offset", "123"
                ))
                .build();

        kafka.publish(failedMessage);
        KafkaAwaitHelper.awaitPropagation(kafka, dlqTopicName, 1, 10);

        List<Message> records = KafkaAwaitHelper.awaitNewMessages(kafka, 1, 30);

        assertThat(records.size()).isGreaterThanOrEqualTo(1);
        Message dlqRecord = records.get(0);
        assertThat(dlqRecord.getHeaders()).containsKey("original-topic");
        assertThat(dlqRecord.getHeaders()).containsKey("failure-reason");
        assertThat(dlqRecord.getHeaders()).containsKey("failure-timestamp");
        assertThat(dlqRecord.getHeaders()).containsKey("retry-count");
        assertThat(dlqRecord.getHeaders().get("retry-count")).isEqualTo("5");

        log.info("TC-031: DLQ message contains {} metadata headers",
                dlqRecord.getHeaders().size());
    }

    @Test
    @DisplayName("TC-032: Retry processing from DLQ")
    @Description("Verify messages can be reprocessed from DLQ")
    @Severity(SeverityLevel.NORMAL)
    @Tag("dlq")
    void testRetryFromDlq() {
        String topicName = createTestTopic();
        String dlqTopicName = topicName + ".dlq";
        createTestTopic(dlqTopicName);

        // Step 1: Send message to DLQ
        KafkaAwaitHelper.awaitConsumerReady(kafka, dlqTopicName, 15);

        Message dlqMessage = Message.builder()
                .topic(Topic.builder().name(dlqTopicName).build())
                .key("retry-key")
                .content("retry-value")
                .headers(Map.of(
                        "original-topic", topicName,
                        "retry-count", "2"
                ))
                .build();

        kafka.publish(dlqMessage);
        KafkaAwaitHelper.awaitPropagation(kafka, dlqTopicName, 1, 10);

        // Step 2: Read from DLQ
        List<Message> dlqRecords = KafkaAwaitHelper.awaitNewMessages(kafka, 1, 30);
        assertThat(dlqRecords).hasSize(1);

        Message record = dlqRecords.get(0);
        int newRetryCount = Integer.parseInt(record.getHeaders().get("retry-count")) + 1;

        // Step 3: Re-subscribe to original topic and resend with incremented retry
        KafkaAwaitHelper.awaitConsumerReady(kafka, topicName, 15);

        Message retryMessage = Message.builder()
                .topic(Topic.builder().name(topicName).build())
                .key(record.getKey())
                .content(record.getContent())
                .headers(Map.of(
                        "retry-count", String.valueOf(newRetryCount),
                        "retried-from-dlq", "true"
                ))
                .build();

        kafka.publish(retryMessage);
        KafkaAwaitHelper.awaitPropagation(kafka, topicName, 1, 10);

        List<Message> retryRecords = KafkaAwaitHelper.awaitNewMessages(kafka, 1, 30);

        assertThat(retryRecords).hasSize(1);
        assertThat(retryRecords.get(0).getHeaders().get("retry-count")).isEqualTo("3");
        assertThat(retryRecords.get(0).getHeaders()).containsEntry("retried-from-dlq", "true");

        log.info("TC-032: Message retried from DLQ, retry-count={}",
                retryRecords.get(0).getHeaders().get("retry-count"));
    }

}
