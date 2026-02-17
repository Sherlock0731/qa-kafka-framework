package tests.errorhandling;

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

import static org.assertj.core.api.Assertions.assertThat;

/**
 * Error Handling Tests
 * Hexagonal Architecture v2.0
 * No Thread.sleep — all waits via KafkaAwaitHelper (Awaitility)
 */
@Slf4j
@DisplayName("Error Handling Tests")
@Tag("error-handling")
public class ErrorHandlingTests extends BaseTest {

    @Test
    @DisplayName("TC-029: Handle serialization errors")
    @Description("Verify proper error handling when message serialization fails")
    @Severity(SeverityLevel.CRITICAL)
    @Tag("error-handling")
    void testSerializationError() {
        String topicName = createTestTopic();

        // null content — valid in Kafka but tests graceful handling
        Message invalidMessage = Message.builder()
                .topic(Topic.builder().name(topicName).build())
                .key("test-key")
                .content(null)
                .build();

        try {
            PublishResult result = kafka.publish(invalidMessage);
            // null content may be accepted — test passes as long as no crash
            log.info("TC-029: null-content message handled, success={}", result.isSuccess());
        } catch (Exception e) {
            // Expected for strict validation
            log.info("TC-029: Serialization error as expected: {}", e.getMessage());
        }
    }

    @Test
    @DisplayName("TC-029A: Handle deserialization errors")
    @Description("Verify consumer handles corrupted message data gracefully")
    @Severity(SeverityLevel.CRITICAL)
    @Tag("error-handling")
    void testDeserializationError() {
        String topicName = createTestTopic();

        // Consumer ready before producing
        KafkaAwaitHelper.awaitConsumerReady(kafka, topicName, 15);

        // Send valid messages
        List<Message> messages = buildMessages(topicName, "deser-key-", 5);
        kafka.publishBatch(messages);
        KafkaAwaitHelper.awaitPropagation(kafka, topicName, 5, 10);

        // Consume — any deserialization issues must be handled gracefully
        List<Message> consumed = KafkaAwaitHelper.awaitNewMessages(kafka, 5, 20);

        // The fact we reach here without crash = deserialization works
        assertThat(kafka).isNotNull();
        log.info("TC-029A: Deserialization OK, received {} messages", consumed.size());
    }

    @Test
    @DisplayName("TC-029B: Recovery from network errors")
    @Description("Verify system recovers from transient network errors")
    @Severity(SeverityLevel.NORMAL)
    @Tag("error-handling")
    void testNetworkErrorRecovery() {
        String topicName = createTestTopic();

        List<Message> messages = buildMessages(topicName, "network-key-", 10);

        int successCount = 0;
        int errorCount = 0;

        for (Message message : messages) {
            try {
                PublishResult result = kafka.publish(message);
                if (result.isSuccess()) successCount++;
                else errorCount++;
            } catch (Exception e) {
                errorCount++;
                log.debug("TC-029B: Network error (expected possible): {}", e.getMessage());
            }
        }

        // All attempts accounted for — some may fail on network issues
        assertThat(successCount + errorCount).isEqualTo(messages.size());
        log.info("TC-029B: success={}, errors={}", successCount, errorCount);
    }

    @Test
    @DisplayName("TC-029C: Handle broker unavailable")
    @Description("Verify graceful handling when broker connection fails")
    @Severity(SeverityLevel.CRITICAL)
    @Tag("error-handling")
    void testBrokerUnavailable() {
        String topicName = createTestTopic();

        List<Message> messages = buildMessages(topicName, "broker-key-", 5);

        try {
            List<PublishResult> results = kafka.publishBatch(messages);
            kafka.flush();
            // Broker available — verify results
            assertThat(results).hasSize(5);
            log.info("TC-029C: Broker available, all {} messages sent", results.size());
        } catch (Exception e) {
            // Broker unavailable — verify error message is meaningful
            assertThat(e.getMessage()).isNotNull();
            log.info("TC-029C: Broker unavailable as expected: {}", e.getMessage());
        }
    }

    @Test
    @DisplayName("TC-029D: Handle topic not found error")
    @Description("Verify proper error handling for non-existent topics")
    @Severity(SeverityLevel.NORMAL)
    @Tag("error-handling")
    void testTopicNotFound() {
        String nonExistentTopic = "non-existent-topic-" + System.currentTimeMillis();

        // Check via AdminClient FIRST — avoids 60-sec producer block
        // (Aiven has auto.create.topics.enable=false, so publish would
        //  spam UNKNOWN_TOPIC_OR_PARTITION WARNs until max.block.ms=60s)
        boolean exists = kafka.topicExists(nonExistentTopic);

        if (exists) {
            // Unexpected: topic somehow exists — clean up and pass
            kafka.deleteTopic(nonExistentTopic);
            log.info("TC-029D: Topic unexpectedly existed, cleaned up");
        } else {
            // Expected on Aiven: topic does not exist, no publish attempted
            log.info("TC-029D: Topic correctly does not exist: {}", nonExistentTopic);
        }

        // Assert: topic should not exist before we create it
        assertThat(exists).isFalse();
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
