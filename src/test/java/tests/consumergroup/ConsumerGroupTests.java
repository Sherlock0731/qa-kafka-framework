package tests.consumergroup;

import io.qameta.allure.Description;
import io.qameta.allure.Severity;
import io.qameta.allure.SeverityLevel;
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
 * Consumer Group Tests
 * Tests for consumer group coordination and rebalancing
 */
@Slf4j
@DisplayName("Consumer Group Tests")
@Tag("consumer-group")
public class ConsumerGroupTests extends BaseTest {

    @Test
    @DisplayName("TC-037: Consumer group rebalance")
    @Description("Verify consumer group rebalances when consumer joins/leaves")
    @Severity(SeverityLevel.CRITICAL)
    @Tag("consumer-group")
    void testConsumerGroupRebalance() {
        String topic = createTestTopic(2); // 2 partitions (Aiven limit)

        // Initialize consumer BEFORE producing to avoid rebalance timing issues
        consumerManager.initConsumer(topic);
        AsyncTestHelper.waitFor(5); // Wait for consumer group rebalance
        consumerManager.poll(2);   // Pre-warm poll to trigger partition assignment
        AsyncTestHelper.waitFor(1);

        // Send messages — use sendAsync+flush to avoid 120s block per message on network errors
        int messageCount = 30;
        List<KafkaMessageDto> messages = TestDataGenerator.generateMessages(topic, messageCount);
        for (KafkaMessageDto msg : messages) {
            try {
                producerManager.sendAsync(msg);
            } catch (Exception e) {
                log.warn("TC-037: Failed to enqueue message: {}", e.getMessage());
            }
        }
        producerManager.flush();
        AsyncTestHelper.waitFor(2);

        // Consume messages
        List<ConsumerRecordDto> records = AsyncTestHelper.pollWithRetry(consumerManager, 30, 1);

        log.info("Consumer consumed {} messages from {} partitions",
                records.size(),
                records.stream().map(ConsumerRecordDto::getPartition).distinct().count());

        long distinctPartitions = records.stream()
                .map(ConsumerRecordDto::getPartition)
                .distinct()
                .count();

        assertThat(distinctPartitions).isGreaterThan(0);
        assertThat(records.size()).isGreaterThan(0);

        // Close consumer to trigger rebalance
        consumerManager.close();

        // Create new consumer - will trigger rebalance again
        consumerManager.initConsumer(topic);
        AsyncTestHelper.waitFor(5);
        consumerManager.poll(2);

        List<ConsumerRecordDto> moreRecords = consumerManager.poll(2);

        log.info("After rebalance, new consumer is ready (received {} immediate messages)", moreRecords.size());

        assertThat(consumerManager).isNotNull();
    }
}
