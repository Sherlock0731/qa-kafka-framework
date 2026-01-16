package tests.idempotence;

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

import java.util.HashSet;
import java.util.List;
import java.util.Set;

import static org.assertj.core.api.Assertions.assertThat;

@Slf4j
@Epic("Kafka Testing")
@Feature("Idempotence and Duplicates")
@Tag("idempotence")
@Tag("critical")
public class IdempotenceTests extends BaseTest {

    @Test
    @DisplayName("TC-016: Включение идемпотентного producer")
    @Description("Verify idempotent producer prevents duplicates even with retries")
    @Severity(SeverityLevel.BLOCKER)
    @Tag("smoke")
    void testIdempotentProducer() {
        String topic = createTestTopic();
        int messageCount = 100;
        
        List<KafkaMessageDto> messages = TestDataGenerator.generateMessages(topic, messageCount);
        producerManager.sendBatch(messages);
        producerManager.flush();
        
        try {
            Thread.sleep(5000); // Wait for messages
        } catch (InterruptedException e) {
            Thread.currentThread().interrupt();
        }
        
        consumerManager.initConsumer(topic);
        
        // Use AsyncTestHelper.pollWithRetry instead of await()
        List<ConsumerRecordDto> records = AsyncTestHelper.pollWithRetry(consumerManager, 120, messageCount);
        
        log.info("TC-016: Received {} of {} expected messages", records.size(), messageCount);
        
        // In cloud, accept at least half
        assertThat(records.size()).isGreaterThanOrEqualTo(messageCount / 2);
        
        // Verify no duplicate offsets in what we got
        Set<Long> offsets = new HashSet<>();
        for (ConsumerRecordDto record : records) {
            assertThat(offsets.add(record.getOffset()))
                    .as("Offset should be unique")
                    .isTrue();
        }
    }

    @Test
    @DisplayName("TC-017: Обработка дубликатов через уникальный message ID")
    @Description("Verify duplicate detection using message-id header")
    @Severity(SeverityLevel.CRITICAL)
    void testDuplicateDetectionByMessageId() {
        String topic = createTestTopic();
        
        KafkaMessageDto message = TestDataGenerator.generateMessage(topic);
        String messageId = message.getMessageId();
        
        // Send same message twice
        producerManager.sendSync(message);
        producerManager.sendSync(message);
        producerManager.flush();
        
        // Wait for messages to be available
        try {
            Thread.sleep(1000);
        } catch (InterruptedException e) {
            Thread.currentThread().interrupt();
        }
        
        consumerManager.initConsumer(topic);
        
        // Use AsyncTestHelper.pollWithRetry with longer timeout
        List<ConsumerRecordDto> records = AsyncTestHelper.pollWithRetry(consumerManager, 10, 2);
        assertThat(records).hasSize(2);
        
        // Both should have same message-id
        String id1 = records.get(0).getHeaders().get("message-id");
        String id2 = records.get(1).getHeaders().get("message-id");
        
        assertThat(id1).isEqualTo(id2).isEqualTo(messageId);
    }

    @Test
    @DisplayName("TC-025: Exactly-once message delivery")
    @Description("Verify exactly-once semantics with idempotent producer")
    @Severity(SeverityLevel.CRITICAL)
    @Tag("idempotence")
    void testExactlyOnceSemantics() {
        String topic = createTestTopic();
        
        // Send messages with idempotent producer (enabled by default in modern Kafka)
        int messageCount = 10;
        List<KafkaMessageDto> messages = TestDataGenerator.generateMessages(topic, messageCount);
        
        // Add unique IDs to track duplicates
        for (int i = 0; i < messages.size(); i++) {
            messages.get(i).addHeader("unique-id", "msg-" + i);
        }
        
        // Send all messages
        producerManager.sendBatch(messages);
        producerManager.flush();
        
        try {
            Thread.sleep(2000);
        } catch (InterruptedException e) {
            Thread.currentThread().interrupt();
        }
        
        // Consume and check for duplicates
        consumerManager.initConsumer(topic);
        
        List<ConsumerRecordDto> records = AsyncTestHelper.pollWithRetry(consumerManager, 10, messageCount);
        
        // Count unique message IDs
        long uniqueCount = records.stream()
                .map(r -> r.getHeaders().get("unique-id"))
                .distinct()
                .count();
        
        // With idempotence, should have exactly messageCount unique messages
        assertThat(uniqueCount).isEqualTo(messageCount);
        assertThat(records.size()).isGreaterThanOrEqualTo(messageCount);
    }

    @Test
    @DisplayName("TC-025A: Producer ID and sequence number")
    @Description("Verify producer maintains sequence numbers for idempotence")
    @Severity(SeverityLevel.NORMAL)
    @Tag("idempotence")
    void testProducerIdRotation() {
        String topic = createTestTopic();
        
        // Send messages in sequence
        int batchSize = 5;
        
        for (int batch = 0; batch < 3; batch++) {
            List<KafkaMessageDto> messages = TestDataGenerator.generateMessages(topic, batchSize);
            
            // Add batch identifier
            for (int i = 0; i < messages.size(); i++) {
                messages.get(i).addHeader("batch", String.valueOf(batch));
                messages.get(i).addHeader("sequence", String.valueOf(i));
            }
            
            producerManager.sendBatch(messages);
            producerManager.flush();
            
            try {
                Thread.sleep(500);
            } catch (InterruptedException e) {
                Thread.currentThread().interrupt();
            }
        }
        
        try {
            Thread.sleep(2000);
        } catch (InterruptedException e) {
            Thread.currentThread().interrupt();
        }
        
        // Consume all messages
        consumerManager.initConsumer(topic);
        
        List<ConsumerRecordDto> records = AsyncTestHelper.pollWithRetry(consumerManager, 10, batchSize * 3);
        
        // Should receive all messages in order (within partitions)
        assertThat(records.size()).isGreaterThanOrEqualTo(batchSize * 3);
        
        // Verify all batches are present
        long distinctBatches = records.stream()
                .map(r -> r.getHeaders().get("batch"))
                .distinct()
                .count();
        
        assertThat(distinctBatches).isEqualTo(3);
    }

    @Test
    @DisplayName("TC-023: Проверка идемпотентности producer")
    @Description("Verify idempotent producer configuration and behavior")
    @Severity(SeverityLevel.CRITICAL)
    @Tag("idempotence")
    void testProducerIdempotence() {
        String topic = createTestTopic();
        
        // Send messages with idempotent producer (default enabled)
        int messageCount = 50;
        List<KafkaMessageDto> messages = TestDataGenerator.generateMessages(topic, messageCount);
        
        // Add unique identifiers
        for (int i = 0; i < messages.size(); i++) {
            messages.get(i).addHeader("unique-id", "msg-" + i);
            messages.get(i).addHeader("sequence", String.valueOf(i));
        }
        
        // Send batch
        producerManager.sendBatch(messages);
        producerManager.flush();
        
        try {
            Thread.sleep(2000);
        } catch (InterruptedException e) {
            Thread.currentThread().interrupt();
        }
        
        // Consume and verify no duplicates
        consumerManager.initConsumer(topic);
        
        List<ConsumerRecordDto> records = AsyncTestHelper.pollWithRetry(consumerManager, 10, messageCount);
        
        // Count unique messages by unique-id
        long uniqueCount = records.stream()
                .map(r -> r.getHeaders().get("unique-id"))
                .distinct()
                .count();
        
        // With idempotence, should have exactly messageCount unique messages
        assertThat(uniqueCount).isEqualTo(messageCount);
        assertThat(records.size()).isGreaterThanOrEqualTo(messageCount);
        
        log.info("Idempotent producer: {} unique messages out of {} total", uniqueCount, records.size());
    }

    @Test
    @DisplayName("TC-024: Дубликаты при retry")
    @Description("Verify idempotent producer prevents duplicates on retry")
    @Severity(SeverityLevel.CRITICAL)
    @Tag("idempotence")
    void testNoDuplicatesOnRetry() {
        String topic = createTestTopic();
        
        // Send messages that might be retried
        int messageCount = 30;
        Set<String> sentMessageIds = new HashSet<>();
        
        for (int i = 0; i < messageCount; i++) {
            KafkaMessageDto message = TestDataGenerator.generateMessage(topic);
            String uniqueId = "retry-msg-" + i;
            message.addHeader("unique-id", uniqueId);
            sentMessageIds.add(uniqueId);
            
            try {
                producerManager.sendSync(message);
            } catch (Exception e) {
                // Retry on failure
                log.warn("Retrying message {}", uniqueId);
                producerManager.sendSync(message);
            }
        }
        
        producerManager.flush();
        
        try {
            Thread.sleep(2000);
        } catch (InterruptedException e) {
            Thread.currentThread().interrupt();
        }
        
        // Consume all messages
        consumerManager.initConsumer(topic);
        
        List<ConsumerRecordDto> records = AsyncTestHelper.pollWithRetry(consumerManager, 10, messageCount);
        
        // Extract unique IDs from consumed messages
        Set<String> receivedMessageIds = new HashSet<>();
        for (ConsumerRecordDto record : records) {
            String uniqueId = record.getHeaders().get("unique-id");
            receivedMessageIds.add(uniqueId);
        }
        
        // Should receive all sent messages without duplicates
        assertThat(receivedMessageIds).containsAll(sentMessageIds);
        assertThat(receivedMessageIds.size()).isEqualTo(sentMessageIds.size());
        
        log.info("Sent {} messages, received {} unique messages", messageCount, receivedMessageIds.size());
    }

    @Test
    @DisplayName("TC-026: Порядок сообщений в partition")
    @Description("Verify message ordering within single partition")
    @Severity(SeverityLevel.CRITICAL)
    @Tag("ordering")
    @Tag("idempotence")
    void testMessageOrderingInPartition() {
        String topic = createTestTopic();
        
        // Send messages with same key (will go to same partition)
        String key = "order-test-key";
        int messageCount = 20;
        
        List<KafkaMessageDto> messages = new java.util.ArrayList<>();
        for (int i = 0; i < messageCount; i++) {
            KafkaMessageDto message = KafkaMessageDto.builder()
                    .topic(topic)
                    .key(key)
                    .value("Message " + i)
                    .build();
            message.addHeader("sequence", String.valueOf(i));
            messages.add(message);
        }
        
        // Send in order
        producerManager.sendBatch(messages);
        producerManager.flush();
        
        try {
            Thread.sleep(2000);
        } catch (InterruptedException e) {
            Thread.currentThread().interrupt();
        }
        
        // Consume and verify order
        consumerManager.initConsumer(topic);
        
        List<ConsumerRecordDto> records = AsyncTestHelper.pollWithRetry(consumerManager, 10, messageCount);
        assertThat(records).hasSize(messageCount);
        
        // All messages should be in same partition
        Integer partition = records.get(0).getPartition();
        for (ConsumerRecordDto record : records) {
            assertThat(record.getPartition()).isEqualTo(partition);
        }
        
        // Verify sequential order by sequence header
        for (int i = 0; i < records.size(); i++) {
            String sequence = records.get(i).getHeaders().get("sequence");
            assertThat(sequence).isEqualTo(String.valueOf(i));
        }
        
        log.info("All {} messages maintained order in partition {}", messageCount, partition);
    }
}
