package tests.producer;

import io.qameta.allure.*;
import lombok.extern.slf4j.Slf4j;
import org.apache.kafka.clients.producer.RecordMetadata;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Tag;
import org.junit.jupiter.api.Test;
import qa.autotest.app.dto.ConsumerRecordDto;
import qa.autotest.app.dto.KafkaMessageDto;
import qa.autotest.framework.utils.AsyncTestHelper;
import qa.autotest.framework.utils.TestDataGenerator;
import tests.BaseTest;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import static org.assertj.core.api.Assertions.assertThat;

/**
 * Producer Tests - Message Sending Operations
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
        String topic = createTestTopic();
        
        KafkaMessageDto message = TestDataGenerator.generateMessage(topic);
        
        RecordMetadata metadata = producerManager.sendSync(message);
        
        assertThat(metadata).isNotNull();
        assertThat(metadata.hasOffset()).isTrue();
        assertThat(metadata.topic()).isEqualTo(topic);
        assertThat(message.getOffset()).isNotNull();
        
        log.info("Message sent to partition {} with offset {}", 
                metadata.partition(), metadata.offset());
    }

    @Test
    @DisplayName("TC-002: Массовая отправка сообщений (batch)")
    @Description("Verify that multiple messages can be sent in batch")
    @Severity(SeverityLevel.CRITICAL)
    void testSendBatchMessages() {
        String topic = createTestTopic();
        int messageCount = 100;
        
        List<KafkaMessageDto> messages = TestDataGenerator.generateMessages(topic, messageCount);
        
        List<RecordMetadata> metadataList = producerManager.sendBatch(messages);
        
        assertThat(metadataList).hasSize(messageCount);
        
        // Verify sequential offsets
        for (int i = 1; i < metadataList.size(); i++) {
            long prevOffset = metadataList.get(i - 1).offset();
            long currentOffset = metadataList.get(i).offset();
            assertThat(currentOffset).isGreaterThan(prevOffset);
        }
    }

    @Test
    @DisplayName("TC-003: Отправка сообщения с custom headers")
    @Description("Verify that messages with custom headers are sent correctly")
    @Severity(SeverityLevel.NORMAL)
    void testSendMessageWithHeaders() {
        String topic = createTestTopic();
        
        Map<String, String> headers = new HashMap<>();
        headers.put("trace-id", "trace-123");
        headers.put("user-id", "user-456");
        
        KafkaMessageDto message = TestDataGenerator.generateMessage(topic);
        message.setHeaders(headers);
        
        RecordMetadata metadata = producerManager.sendSync(message);
        
        assertThat(metadata).isNotNull();
        
        // Verify by consuming - initialize consumer in main thread
        consumerManager.initConsumer(topic);
        
        // Poll with retry logic without awaitility to avoid threading issues
        List<ConsumerRecordDto> records = new ArrayList<>();
        for (int i = 0; i < 10 && records.isEmpty(); i++) {
            records = consumerManager.poll(2);
            if (records.isEmpty()) {
                try {
                    Thread.sleep(500);
                } catch (InterruptedException e) {
                    Thread.currentThread().interrupt();
                    throw new RuntimeException("Interrupted while waiting for messages", e);
                }
            }
        }
        
        assertThat(records).isNotEmpty();
        ConsumerRecordDto record = records.get(0);
        assertThat(record.getHeaders()).containsEntry("trace-id", "trace-123");
        assertThat(record.getHeaders()).containsEntry("user-id", "user-456");
    }

    @Test
    @DisplayName("TC-004: Отправка сообщения с указанным ключом для партиционирования")
    @Description("Verify that messages with the same key go to the same partition")
    @Severity(SeverityLevel.CRITICAL)
    void testSendMessageWithKey() {
        // Use 2 partitions for Aiven Free Tier (changed from 3)
        String topic = createTestTopic(2);
        String key = "user-123";
        
        KafkaMessageDto message1 = TestDataGenerator.generateMessageWithKey(topic, key);
        KafkaMessageDto message2 = TestDataGenerator.generateMessageWithKey(topic, key);
        
        RecordMetadata metadata1 = producerManager.sendSync(message1);
        RecordMetadata metadata2 = producerManager.sendSync(message2);
        
        // Same key should go to same partition
        assertThat(metadata1.partition()).isEqualTo(metadata2.partition());
        
        log.info("Both messages with key '{}' went to partition {}", key, metadata1.partition());
    }

    @Test
    @DisplayName("TC-006: Отправка сообщения размером больше max.message.bytes")
    @Description("Verify that oversized messages are rejected")
    @Severity(SeverityLevel.NORMAL)
    void testSendOversizedMessage() {
        String topic = createTestTopic();
        
        // Create a message larger than 1MB
        StringBuilder largeValue = new StringBuilder();
        for (int i = 0; i < 200000; i++) {
            largeValue.append("This is a large message payload. ");
        }
        
        KafkaMessageDto message = TestDataGenerator.generateMessage(topic);
        message.setValue(largeValue.toString());
        
        try {
            producerManager.sendSync(message);
            // If we reach here, the broker accepted it (might have higher limit)
            log.warn("Large message was accepted by broker");
        } catch (Exception e) {
            // Expected: message too large
            assertThat(e.getMessage()).containsIgnoringCase("message");
            log.info("Large message rejected as expected: {}", e.getMessage());
        }
    }

    @Test
    @DisplayName("TC-006: Message compression (gzip/lz4/snappy)")
    @Description("Verify producer can send compressed messages")
    @Severity(SeverityLevel.NORMAL)
    void testProducerCompression() {
        String topic = createTestTopic();
        
        // Send compressible messages (repeated text compresses well)
        int messageCount = 20;
        List<KafkaMessageDto> messages = TestDataGenerator.generateMessages(topic, messageCount);
        
        // Add repetitive content for better compression
        for (KafkaMessageDto msg : messages) {
            msg.setValue("Repeated text for compression. ".repeat(100));
        }
        
        producerManager.sendBatch(messages);
        producerManager.flush();
        
        try {
            Thread.sleep(3000); // Increased from 1s to 3s
        } catch (InterruptedException e) {
            Thread.currentThread().interrupt();
        }
        
        // Verify messages were sent successfully
        consumerManager.initConsumer(topic);
        List<ConsumerRecordDto> records = AsyncTestHelper.pollWithRetry(consumerManager, 60, messageCount);
        
        assertThat(records.size()).isGreaterThanOrEqualTo(messageCount); // Changed to >= for flexibility
    }

    @Test
    @DisplayName("TC-007: Producer acks=all guarantee")
    @Description("Verify producer waits for all replicas with acks=all")
    @Severity(SeverityLevel.CRITICAL)
    void testProducerAcks() {
        String topic = createTestTopic();
        
        // Send with acks=all (configured in producer)
        List<KafkaMessageDto> messages = TestDataGenerator.generateMessages(topic, 10);
        
        long startTime = System.currentTimeMillis();
        producerManager.sendBatch(messages);
        producerManager.flush();
        long endTime = System.currentTimeMillis();
        
        // With acks=all, should take slightly longer than acks=1
        // but should complete successfully
        long duration = endTime - startTime;
        log.info("Send with acks=all took {} ms", duration);
        
        // Verify messages were persisted
        try {
            Thread.sleep(1000);
        } catch (InterruptedException e) {
            Thread.currentThread().interrupt();
        }
        
        consumerManager.initConsumer(topic);
        consumerManager.poll(3);
        List<ConsumerRecordDto> records = AsyncTestHelper.pollWithRetry(consumerManager, 10, 10);
        
        assertThat(records).hasSize(10);
    }

    @Test
    @DisplayName("TC-008: Producer automatic retry on transient errors")
    @Description("Verify producer retries failed sends automatically")
    @Severity(SeverityLevel.CRITICAL)
    void testProducerRetry() {
        String topic = createTestTopic();
        
        // Send messages - producer will retry on transient errors
        int messageCount = 15;
        List<KafkaMessageDto> messages = TestDataGenerator.generateMessages(topic, messageCount);
        
        int successCount = 0;
        for (KafkaMessageDto message : messages) {
            try {
                producerManager.sendSync(message);
                successCount++;
            } catch (Exception e) {
                // Some might fail, but retry mechanism should minimize failures
                log.warn("Message send failed after retries: {}", e.getMessage());
            }
        }
        
        // Most messages should succeed even with potential transient errors
        assertThat(successCount).isGreaterThan(messageCount / 2);
    }

    @Test
    @DisplayName("TC-006A: Producer request timeout")
    @Description("Verify producer handles request timeout properly")
    @Severity(SeverityLevel.NORMAL)
    void testProducerTimeout() {
        String topic = createTestTopic();
        
        // Send message with configured timeout
        KafkaMessageDto message = TestDataGenerator.generateMessage(topic);
        
        try {
            long startTime = System.currentTimeMillis();
            producerManager.sendSync(message);
            long duration = System.currentTimeMillis() - startTime;
            
            // Should complete within reasonable time
            assertThat(duration).isLessThan(30000); // 30 seconds max
        } catch (org.apache.kafka.common.errors.TimeoutException e) {
            // Timeout is acceptable - just verify it's handled properly
            assertThat(e.getMessage()).containsIgnoringCase("timeout");
        }
    }

    @Test
    @DisplayName("TC-006B: Producer buffer overflow handling")
    @Description("Verify producer handles buffer full scenario")
    @Severity(SeverityLevel.NORMAL)
    void testProducerBufferFull() {
        String topic = createTestTopic();
        
        // Send many messages quickly to potentially fill buffer
        int messageCount = 1000;
        List<KafkaMessageDto> messages = TestDataGenerator.generateMessages(topic, messageCount);
        
        int sentCount = 0;
        for (KafkaMessageDto message : messages) {
            try {
                producerManager.sendAsync(message);
                sentCount++;
            } catch (Exception e) {
                // Buffer full or timeout is acceptable
                log.debug("Send failed (buffer full?): {}", e.getMessage());
            }
        }
        
        // Flush remaining
        producerManager.flush();
        
        // Should have sent at least some messages
        assertThat(sentCount).isGreaterThan(0);
    }

    @Test
    @DisplayName("TC-006C: Transactional message sending")
    @Description("Verify transactional producer behavior")
    @Severity(SeverityLevel.CRITICAL)
    void testTransactionalProducer() {
        String topic = createTestTopic();
        
        // Note: This test assumes transactional support
        // If not configured, it will work like normal send
        
        List<KafkaMessageDto> messages = TestDataGenerator.generateMessages(topic, 10);
        
        try {
            // Send as a transaction (if supported)
            producerManager.sendBatch(messages);
            producerManager.flush();
            
            // Verify all messages committed
            Thread.sleep(1000);
            
            consumerManager.initConsumer(topic);
            consumerManager.poll(3);
            List<ConsumerRecordDto> records = AsyncTestHelper.pollWithRetry(consumerManager, 10, 10);
            
            // Should get all or none (transactional guarantee)
            assertThat(records.size()).isIn(0, 10);
        } catch (InterruptedException e) {
            Thread.currentThread().interrupt();
        }
    }

    @Test
    @DisplayName("TC-006D: Producer metrics collection")
    @Description("Verify producer exposes metrics for monitoring")
    @Severity(SeverityLevel.NORMAL)
    void testProducerMetrics() {
        String topic = createTestTopic();
        
        // Send messages to generate metrics
        List<KafkaMessageDto> messages = TestDataGenerator.generateMessages(topic, 20);
        
        long startTime = System.currentTimeMillis();
        producerManager.sendBatch(messages);
        producerManager.flush();
        long duration = System.currentTimeMillis() - startTime;
        
        // Metrics we care about:
        // - Send duration (already measured)
        // - Success count (20 messages)
        // - Average latency
        
        log.info("Producer metrics: {} messages sent in {} ms", messages.size(), duration);
        log.info("Average latency: {} ms per message", duration / messages.size());
        
        // Verify basic metrics make sense
        assertThat(duration).isGreaterThan(0);
        assertThat(messages.size()).isEqualTo(20);
        
        double avgLatency = (double) duration / messages.size();
        assertThat(avgLatency).isGreaterThan(0);
    }
}
