package tests.ordering;

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
@Feature("Message Ordering")
@Tag("ordering")
@Tag("critical")
public class OrderingTests extends BaseTest {

    @Test
    @DisplayName("TC-021: Гарантия порядка в одной партиции")
    @Description("Verify messages with same key maintain order within partition")
    @Severity(SeverityLevel.BLOCKER)
    @Tag("smoke")
    void testOrderingWithinPartition() {
        // Use 1 partition for Aiven Free Tier (changed from 3)
        String topic = createTestTopic();
        String key = "order-test";
        int messageCount = 100;
        
        // Send messages with same key
        for (int i = 0; i < messageCount; i++) {
            KafkaMessageDto message = TestDataGenerator.generateMessageWithKey(topic, key);
            message.setValue("message-" + i);
            
            // Retry sending if fails (Kafka может быть медленный)
            boolean sent = false;
            for (int attempt = 1; attempt <= 3 && !sent; attempt++) {
                try {
                    producerManager.sendSync(message);
                    sent = true;
                } catch (RuntimeException e) {
                    if (attempt < 3) {
                        log.warn("Failed to send message {} (attempt {}), retrying: {}", i, attempt, e.getMessage());
                        AsyncTestHelper.waitFor(1);
                    } else {
                        throw e; // Последняя попытка - пробрасываем ошибку
                    }
                }
            }
        }
        producerManager.flush();
        
        // Wait for messages to be available in Kafka
        AsyncTestHelper.waitFor(2);
        
        consumerManager.initConsumer(topic);
        
        // Use AsyncTestHelper.pollWithRetry instead of await()
        List<ConsumerRecordDto> records = AsyncTestHelper.pollWithRetry(consumerManager, 30, messageCount);
        assertThat(records).hasSize(messageCount);
        
        // Verify sequential offsets (same partition)
        Integer partition = records.get(0).getPartition();
        long prevOffset = records.get(0).getOffset();
        
        for (int i = 1; i < records.size(); i++) {
            assertThat(records.get(i).getPartition()).isEqualTo(partition);
            assertThat(records.get(i).getOffset()).isEqualTo(prevOffset + 1);
            prevOffset = records.get(i).getOffset();
        }
    }

    @Test
    @DisplayName("TC-027: Message ordering across partitions")
    @Description("Verify ordering behavior with multiple partitions")
    @Severity(SeverityLevel.NORMAL)
    @Tag("ordering")
    void testOrderingMultiplePartitions() {
        String topic = createTestTopic(2); // 2 partitions (Aiven limit)
        
        // Send messages without key (will distribute across partitions)
        int messageCount = 15;
        List<KafkaMessageDto> messages = TestDataGenerator.generateMessages(topic, messageCount);
        
        // Add sequence numbers
        for (int i = 0; i < messages.size(); i++) {
            messages.get(i).addHeader("sequence", String.valueOf(i));
        }
        
        producerManager.sendBatch(messages);
        producerManager.flush();
        AsyncTestHelper.waitFor(2);
        
        // Consume
        consumerManager.initConsumer(topic);
        consumerManager.poll(3);
        
        List<ConsumerRecordDto> records = AsyncTestHelper.pollWithRetry(consumerManager, 30, messageCount);
        assertThat(records).hasSize(messageCount);
        
        // Group by partition
        var byPartition = records.stream()
                .collect(java.util.stream.Collectors.groupingBy(ConsumerRecordDto::getPartition));
        
        log.info("Messages distributed across {} partitions", byPartition.size());
        
        // Within each partition, ordering should be maintained
        for (var entry : byPartition.entrySet()) {
            List<ConsumerRecordDto> partitionRecords = entry.getValue();
            
            if (partitionRecords.size() > 1) {
                long prevOffset = partitionRecords.get(0).getOffset();
                
                for (int i = 1; i < partitionRecords.size(); i++) {
                    // Offsets should be sequential within partition
                    assertThat(partitionRecords.get(i).getOffset()).isGreaterThan(prevOffset);
                    prevOffset = partitionRecords.get(i).getOffset();
                }
            }
        }
    }

    @Test
    @DisplayName("TC-028: Ordering guarantee for keyed messages")
    @Description("Verify strict ordering for messages with same key")
    @Severity(SeverityLevel.CRITICAL)
    @Tag("ordering")
    void testOrderingWithKey() {
        String topic = createTestTopic(2); // 2 partitions (Aiven limit)
        
        // Send messages with same key (should go to same partition)
        String key = "ordered-key";
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
        
        producerManager.sendBatch(messages);
        producerManager.flush();
        AsyncTestHelper.waitFor(2);
        
        // Consume
        consumerManager.initConsumer(topic);
        consumerManager.poll(3);
        
        List<ConsumerRecordDto> records = AsyncTestHelper.pollWithRetry(consumerManager, 30, messageCount);
        assertThat(records).hasSize(messageCount);
        
        // All messages should be in same partition (same key)
        Integer partition = records.get(0).getPartition();
        for (ConsumerRecordDto record : records) {
            assertThat(record.getPartition()).isEqualTo(partition);
        }
        
        // Verify sequential order
        for (int i = 0; i < records.size(); i++) {
            String sequence = records.get(i).getHeaders().get("sequence");
            assertThat(sequence).isEqualTo(String.valueOf(i));
        }
        
        log.info("All {} messages with key '{}' maintained strict order in partition {}", 
                messageCount, key, partition);
    }
}
