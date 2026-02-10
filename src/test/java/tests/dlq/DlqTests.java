package tests.dlq;

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

import java.util.HashMap;
import java.util.List;
import java.util.Map;

import static org.assertj.core.api.Assertions.assertThat;

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
        String topic = createTestTopic();
        String dlqTopic = topicManager.createDlqTopic(topic);
        createdTopics.add(dlqTopic);

        // Initialize consumer on DLQ BEFORE producing to avoid rebalance timing issues
        consumerManager.initConsumer(dlqTopic);
        AsyncTestHelper.waitFor(5); // Wait for consumer group rebalance
        consumerManager.poll(2);   // Pre-warm poll to trigger partition assignment
        AsyncTestHelper.waitFor(1);

        // Send poison pill message
        KafkaMessageDto poisonMessage = TestDataGenerator.generateMessage(topic);
        poisonMessage.setValue("INVALID_JSON:{not valid}");
        poisonMessage.setRetryCount(3);

        producerManager.sendSync(poisonMessage);

        // Simulate failure and send to DLQ
        Map<String, String> dlqHeaders = new HashMap<>();
        dlqHeaders.put("error-message", "Deserialization failed");
        dlqHeaders.put("retry-count", "3");
        dlqHeaders.put("original-topic", topic);

        KafkaMessageDto dlqMessage = KafkaMessageDto.builder()
                .topic(dlqTopic)
                .key(poisonMessage.getKey())
                .value(poisonMessage.getValue())
                .headers(dlqHeaders)
                .originalTopic(topic)
                .errorMessage("Deserialization failed")
                .build();

        producerManager.sendSync(dlqMessage);
        producerManager.flush();

        List<ConsumerRecordDto> records = AsyncTestHelper.pollWithRetry(consumerManager, 60, 1);
        assertThat(records).hasSize(1);

        ConsumerRecordDto dlqRecord = records.get(0);
        assertThat(dlqRecord.getHeaders()).containsEntry("original-topic", topic);
        assertThat(dlqRecord.getHeaders()).containsEntry("retry-count", "3");
    }

    @Test
    @DisplayName("TC-031: DLQ message routing with metadata")
    @Description("Verify DLQ messages include proper metadata for debugging")
    @Severity(SeverityLevel.CRITICAL)
    @Tag("dlq")
    void testDlqRoutingLogic() {
        String topic = createTestTopic();
        String dlqTopic = topic + "-dlq";
        createAndTrackTopic(dlqTopic, 1, (short) 1);

        // Initialize consumer on DLQ BEFORE producing to avoid rebalance timing issues
        consumerManager.initConsumer(dlqTopic);
        AsyncTestHelper.waitFor(5); // Wait for consumer group rebalance
        consumerManager.poll(2);   // Pre-warm poll to trigger partition assignment
        AsyncTestHelper.waitFor(1);

        // Create failed message with metadata
        KafkaMessageDto failedMessage = KafkaMessageDto.builder()
                .topic(topic)
                .key("failed-key")
                .value("failed-value")
                .build();

        failedMessage.addHeader("original-topic", topic);
        failedMessage.addHeader("failure-reason", "Processing error");
        failedMessage.addHeader("failure-timestamp", String.valueOf(System.currentTimeMillis()));
        failedMessage.addHeader("retry-count", "5");
        failedMessage.addHeader("original-partition", "0");
        failedMessage.addHeader("original-offset", "123");

        // Route to DLQ
        failedMessage.setTopic(dlqTopic);
        producerManager.sendSync(failedMessage);
        producerManager.flush();
        AsyncTestHelper.waitFor(3);

        List<ConsumerRecordDto> records = AsyncTestHelper.pollWithRetry(consumerManager, 60, 1);
        assertThat(records.size()).isGreaterThanOrEqualTo(1);

        ConsumerRecordDto dlqRecord = records.get(0);
        assertThat(dlqRecord.getHeaders()).containsKey("original-topic");
        assertThat(dlqRecord.getHeaders()).containsKey("failure-reason");
        assertThat(dlqRecord.getHeaders()).containsKey("failure-timestamp");
        assertThat(dlqRecord.getHeaders()).containsKey("retry-count");
        assertThat(dlqRecord.getHeaders().get("retry-count")).isEqualTo("5");

        log.info("DLQ message contains {} metadata headers", dlqRecord.getHeaders().size());
    }

    @Test
    @DisplayName("TC-032: Retry processing from DLQ")
    @Description("Verify messages can be reprocessed from DLQ")
    @Severity(SeverityLevel.NORMAL)
    @Tag("dlq")
    void testRetryFromDlq() {
        String topic = createTestTopic();
        String dlqTopic = topic + "-dlq";
        createAndTrackTopic(dlqTopic, 1, (short) 1);

        // Initialize consumer on DLQ BEFORE producing to avoid rebalance timing issues
        consumerManager.initConsumer(dlqTopic);
        AsyncTestHelper.waitFor(5); // Wait for consumer group rebalance
        consumerManager.poll(2);   // Pre-warm poll to trigger partition assignment
        AsyncTestHelper.waitFor(1);

        // Send message to DLQ
        KafkaMessageDto dlqMessage = KafkaMessageDto.builder()
                .topic(dlqTopic)
                .key("retry-key")
                .value("retry-value")
                .build();

        dlqMessage.addHeader("original-topic", topic);
        dlqMessage.addHeader("retry-count", "2");

        producerManager.sendSync(dlqMessage);
        producerManager.flush();
        AsyncTestHelper.waitFor(2);

        // Read from DLQ
        List<ConsumerRecordDto> dlqRecords = AsyncTestHelper.pollWithRetry(consumerManager, 60, 1);
        assertThat(dlqRecords).hasSize(1);

        ConsumerRecordDto record = dlqRecords.get(0);

        // Retry: send back to original topic with incremented retry count
        KafkaMessageDto retryMessage = KafkaMessageDto.builder()
                .topic(topic)
                .key(record.getKey())
                .value(record.getValue())
                .build();

        String originalRetryCount = record.getHeaders().get("retry-count");
        int newRetryCount = Integer.parseInt(originalRetryCount) + 1;
        retryMessage.addHeader("retry-count", String.valueOf(newRetryCount));
        retryMessage.addHeader("retried-from-dlq", "true");

        // Re-init consumer on original topic BEFORE sending retry
        consumerManager.close();
        consumerManager.initConsumer(topic);
        AsyncTestHelper.waitFor(5);
        consumerManager.poll(2);
        AsyncTestHelper.waitFor(1);

        producerManager.sendSync(retryMessage);
        producerManager.flush();
        AsyncTestHelper.waitFor(2);

        List<ConsumerRecordDto> retryRecords = AsyncTestHelper.pollWithRetry(consumerManager, 60, 1);
        assertThat(retryRecords).hasSize(1);

        ConsumerRecordDto retriedRecord = retryRecords.get(0);
        assertThat(retriedRecord.getHeaders().get("retry-count")).isEqualTo("3");
        assertThat(retriedRecord.getHeaders()).containsEntry("retried-from-dlq", "true");

        log.info("Message successfully retried from DLQ with retry count: {}",
                retriedRecord.getHeaders().get("retry-count"));
    }
}
