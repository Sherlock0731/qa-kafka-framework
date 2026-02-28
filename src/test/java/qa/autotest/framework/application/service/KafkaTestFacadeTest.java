package qa.autotest.framework.application.service;

import org.junit.jupiter.api.*;
import org.junit.jupiter.api.extension.ExtendWith;
import org.mockito.Mock;
import org.mockito.junit.jupiter.MockitoExtension;
import qa.autotest.framework.config.KafkaConfig;
import qa.autotest.framework.domain.model.*;
import qa.autotest.framework.domain.port.ConsumerGroupReader;
import qa.autotest.framework.domain.port.MessageConsumer;
import qa.autotest.framework.domain.port.MessagePublisher;
import qa.autotest.framework.domain.port.TopicRepository;

import java.time.Duration;
import java.util.List;
import java.util.Map;
import java.util.Optional;

import static org.assertj.core.api.Assertions.*;
import static org.mockito.ArgumentMatchers.*;
import static org.mockito.Mockito.*;

/**
 * Unit-тесты для KafkaTestFacade — unit-test конструктор с моками.
 * <p>
 * Покрываемые сценарии:
 * - делегирование publish → MessagePublishingService → MessagePublisher
 * - делегирование subscribe/poll → MessageConsumptionService → MessageConsumer
 * - делегирование createTopic/deleteTopic → TopicManagementService → TopicRepository
 * - close() закрывает все три сервиса
 * - getMetrics() в unit-test режиме возвращает "N/A"
 * - consumeUntil / consumeUntilOrThrow — делегирование и Optional/throw семантика
 */
@Tag("unit")
@DisplayName("KafkaTestFacade — unit-test constructor (port mocks)")
@ExtendWith(MockitoExtension.class)
class KafkaTestFacadeTest {

    @Mock
    MessagePublisher publisher;
    @Mock
    MessageConsumer consumer;
    @Mock
    TopicRepository topicRepository;
    @Mock
    ConsumerGroupReader consumerGroupReader;
    @Mock
    KafkaConfig config;

    private KafkaTestFacade facade;

    private static final String GROUP_ID = "unit-test-group";
    private static final String TOPIC_NAME = "qa-test-unit";

    private static Topic topic() {
        return Topic.builder().name(TOPIC_NAME).build();
    }

    private static Message message() {
        return Message.builder()
                .topic(topic())
                .key("k1")
                .content("{\"test\":true}")
                .build();
    }

    @BeforeEach
    void setUp() {
        facade = new KafkaTestFacade(publisher, consumer, topicRepository, consumerGroupReader, config, GROUP_ID);
    }

    // ═══════════════════════════════════════════════════════════════════════
    // Метаданные
    // ═══════════════════════════════════════════════════════════════════════

    @Test
    @DisplayName("getMetrics() в unit-test режиме возвращает строку 'N/A'")
    void shouldReturnNaMetricsInUnitTestMode() {
        assertThat(facade.getMetrics()).contains("N/A");
    }

    @Test
    @DisplayName("consumerGroupId возвращает значение из конструктора")
    void shouldExposeConsumerGroupId() {
        assertThat(facade.getConsumerGroupId()).isEqualTo(GROUP_ID);
    }

    // ═══════════════════════════════════════════════════════════════════════
    // Publishing — делегирование
    // ═══════════════════════════════════════════════════════════════════════

    @Test
    @DisplayName("publish(topic, key, content) делегирует в MessagePublisher")
    void shouldDelegateSimplePublish() {
        PublishResult expected = PublishResult.success(message(), 0, 42L, System.currentTimeMillis());
        when(publisher.publish(any(Message.class))).thenReturn(expected);

        PublishResult result = facade.publish(TOPIC_NAME, "k1", "{\"test\":true}");

        assertThat(result.isSuccess()).isTrue();
        assertThat(result.getOffset()).isEqualTo(42L);
        verify(publisher).publish(any(Message.class));
    }

    @Test
    @DisplayName("publish(topic, key, content, headers) передаёт headers в сообщение")
    void shouldDelegatePublishWithHeaders() {
        PublishResult expected = PublishResult.success(message(), 0, 1L, System.currentTimeMillis());
        when(publisher.publish(any(Message.class))).thenReturn(expected);

        Map<String, String> headers = Map.of("event-type", "ORDER_CREATED");
        facade.publish(TOPIC_NAME, "k1", "{\"id\":1}", headers);

        verify(publisher).publish(argThat(msg ->
                "ORDER_CREATED".equals(msg.getHeaders().get("event-type"))
        ));
    }

    @Test
    @DisplayName("publishBatch делегирует список сообщений")
    void shouldDelegateBatchPublish() {
        List<Message> messages = List.of(message(), message());
        List<PublishResult> results = List.of(
                PublishResult.success(message(), 0, 1L, 0L),
                PublishResult.success(message(), 0, 2L, 0L)
        );
        when(publisher.publishBatch(messages)).thenReturn(results);

        List<PublishResult> actual = facade.publishBatch(messages);

        assertThat(actual).hasSize(2);
        verify(publisher).publishBatch(messages);
    }

    @Test
    @DisplayName("flush() вызывает publisher.flush()")
    void shouldDelegateFlush() {
        facade.flush();
        verify(publisher).flush();
    }

    // ═══════════════════════════════════════════════════════════════════════
    // Consumption — делегирование
    // ═══════════════════════════════════════════════════════════════════════

    @Test
    @DisplayName("subscribe(topicName) вызывает consumer.subscribe(Topic)")
    void shouldDelegateSubscribe() {
        facade.subscribe(TOPIC_NAME);
        verify(consumer).subscribe(eq(topic()));
    }

    @Test
    @DisplayName("subscribe(vararg) вызывает consumer.subscribe(Set<Topic>)")
    void shouldDelegateSubscribeVararg() {
        facade.subscribe(TOPIC_NAME, "another-topic");
        verify(consumer).subscribe(argThat((java.util.Set<qa.autotest.framework.domain.model.Topic> set) -> set.size() == 2));
    }

    @Test
    @DisplayName("poll(timeout) возвращает результат от consumer")
    void shouldDelegatePoll() {
        ConsumeResult expected = ConsumeResult.success(List.of(message()));
        when(consumer.poll(any(Duration.class))).thenReturn(expected);

        ConsumeResult result = facade.poll(Duration.ofSeconds(5));

        assertThat(result.getMessageCount()).isEqualTo(1);
        verify(consumer).poll(Duration.ofSeconds(5));
    }

    @Test
    @DisplayName("seekToBeginning() вызывает consumer.seekToBeginning()")
    void shouldDelegateSeekToBeginning() {
        facade.seekToBeginning();
        verify(consumer).seekToBeginning();
    }

    @Test
    @DisplayName("seekToEnd() вызывает consumer.seekToEnd()")
    void shouldDelegateSeekToEnd() {
        facade.seekToEnd();
        verify(consumer).seekToEnd();
    }

    @Test
    @DisplayName("commitSync() вызывает consumer.commitSync()")
    void shouldDelegateCommitSync() {
        facade.commitSync();
        verify(consumer).commitSync();
    }

    // ═══════════════════════════════════════════════════════════════════════
    // Topic Management — делегирование
    // ═══════════════════════════════════════════════════════════════════════

    @Test
    @DisplayName("createTopic(name) возвращает true при успешном создании")
    void shouldDelegateCreateTopicByName() {
        when(topicRepository.exists(any())).thenReturn(false);
        when(topicRepository.createTopic(any())).thenReturn(true);

        boolean created = facade.createTopic(TOPIC_NAME);

        assertThat(created).isTrue();
        verify(topicRepository).createTopic(argThat(t -> TOPIC_NAME.equals(t.getName())));
    }

    @Test
    @DisplayName("createTopic(name, partitions) передаёт партиции в Topic")
    void shouldDelegateCreateTopicWithPartitions() {
        when(topicRepository.exists(any())).thenReturn(false);
        when(topicRepository.createTopic(any())).thenReturn(true);

        facade.createTopic(TOPIC_NAME, 4);

        verify(topicRepository).createTopic(argThat(t ->
                TOPIC_NAME.equals(t.getName()) && t.getPartitionCount() == 4
        ));
    }

    @Test
    @DisplayName("deleteTopic возвращает false если топик не существует")
    void shouldReturnFalseWhenDeletingNonExistentTopic() {
        when(topicRepository.exists(any())).thenReturn(false);

        boolean deleted = facade.deleteTopic(TOPIC_NAME);

        assertThat(deleted).isFalse();
        verify(topicRepository, never()).deleteTopic(any());
    }

    @Test
    @DisplayName("topicExists делегирует в TopicRepository")
    void shouldDelegateTopicExists() {
        when(topicRepository.exists(any())).thenReturn(true);

        boolean exists = facade.topicExists(TOPIC_NAME);

        assertThat(exists).isTrue();
    }

    // ═══════════════════════════════════════════════════════════════════════
    // Lifecycle
    // ═══════════════════════════════════════════════════════════════════════

    @Test
    @DisplayName("close() вызывает close на publisher, consumer и topicRepository")
    void shouldCloseAllResourcesOnClose() {
        facade.close();

        verify(publisher).close();
        verify(consumer).close();
        verify(topicRepository).close();
    }

    @Test
    @DisplayName("closeAll() в unit-test режиме — no-op, не бросает исключений")
    void shouldNotThrowOnCloseAllInUnitTestMode() {
        assertThatNoException().isThrownBy(facade::closeAll);
    }

    // ═══════════════════════════════════════════════════════════════════════
    // consumeUntil / consumeUntilOrThrow
    // ═══════════════════════════════════════════════════════════════════════

    @Test
    @DisplayName("consumeUntil возвращает Optional.empty() если условие не выполнено")
    void shouldReturnEmptyWhenConditionNotMet() {
        // consumer всегда возвращает пустой результат
        when(consumer.poll(any())).thenReturn(ConsumeResult.empty());

        Optional<Message> result = facade.consumeUntil(
                msg -> "never".equals(msg.getKey()),
                "key=never",
                1,
                Duration.ofMillis(100)
        );

        assertThat(result).isEmpty();
    }

    @Test
    @DisplayName("consumeUntil возвращает Optional с сообщением при совпадении")
    void shouldReturnMessageWhenConditionMet() {
        Message target = Message.builder()
                .topic(topic())
                .key("match-key")
                .content("{}")
                .build();
        when(consumer.poll(any()))
                .thenReturn(ConsumeResult.success(List.of(target)));

        Optional<Message> result = facade.consumeUntil(
                msg -> "match-key".equals(msg.getKey()),
                "key=match-key",
                3,
                Duration.ofMillis(100)
        );

        assertThat(result).isPresent();
        assertThat(result.get().getKey()).isEqualTo("match-key");
    }
}
