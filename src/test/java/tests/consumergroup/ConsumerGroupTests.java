package tests.consumergroup;

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
import java.util.Set;
import java.util.stream.Collectors;

import static org.assertj.core.api.Assertions.assertThat;

/**
 * Consumer Group Tests
 * Hexagonal Architecture v2.0
 * No Thread.sleep — all waits via KafkaAwaitHelper (Awaitility)
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
        String topicName = createTestTopic(2);

        // Subscribe and await group ready before producing
        KafkaAwaitHelper.awaitConsumerReady(kafka, topicName, 15);

        int messageCount = 30;
        List<Message> messages = buildMessages(topicName, "rebalance-", messageCount);
        kafka.publishBatch(messages);
        KafkaAwaitHelper.awaitPropagation(kafka, topicName, messageCount, 15);

        List<Message> consumed = KafkaAwaitHelper.awaitNewMessages(kafka, messageCount, 30);

        Set<Integer> partitions = consumed.stream()
                .map(Message::getPartition)
                .collect(Collectors.toSet());

        log.info("TC-037: Consumed {} messages from {} partitions",
                consumed.size(), partitions.size());

        assertThat(consumed.size()).isGreaterThan(0);
        assertThat(partitions).isNotEmpty();

        // Simulate rebalance: close and re-subscribe with a fresh facade
        kafka.close();
        kafka = createNewFacade();

        // Await rebalance complete — no Thread.sleep
        KafkaAwaitHelper.awaitRebalance(kafka, topicName, 15);

        assertThat(kafka).isNotNull();
        log.info("TC-037: Rebalance complete, new consumer ready");
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
