package qa.autotest.framework.application.service;

import org.junit.jupiter.api.*;
import org.junit.jupiter.api.extension.ExtendWith;
import org.mockito.Mock;
import org.mockito.junit.jupiter.MockitoExtension;
import qa.autotest.framework.exceptions.MessageNotFoundException;
import qa.autotest.framework.domain.model.*;
import qa.autotest.framework.domain.port.ConsumerGroupReader;
import qa.autotest.framework.domain.port.MessageConsumer;

import java.time.Duration;
import java.util.List;
import java.util.Optional;
import java.util.Set;

import static org.assertj.core.api.Assertions.*;
import static org.mockito.ArgumentMatchers.*;
import static org.mockito.Mockito.*;

/**
 * Unit-тесты для MessageConsumptionService.
 * <p>
 * Покрываемые сценарии:
 * - subscribeToTopic: domain-валидация Topic перед делегированием
 * - subscribeToTopics: валидация каждого топика в Set
 * - consumeMessages: делегирование poll, возврат результата
 * - consumeExpectedMessages: делегирование pollMessages
 * - consumeAllMessages: делегирование consumeAll
 * - consumeUntil: возвращает Optional.empty() при исчерпании попыток
 * - consumeUntil: возвращает Optional<Message> при совпадении
 * - consumeUntilOrThrow: бросает MessageNotFoundException если не найдено
 * - consumeUntilOrThrow: возвращает Message при совпадении
 * - seekToBeginning / seekToEnd / commitOffsets — делегирование
 * - close() делегирует
 */
@DisplayName("MessageConsumptionService")
@ExtendWith(MockitoExtension.class)
class MessageConsumptionServiceTest {

    @Mock
    MessageConsumer consumer;
    @Mock
    ConsumerGroupReader consumerGroupReader;

    private MessageConsumptionService service;

    // ── fixtures ──────────────────────────────────────────────────────────
    private static final String TOPIC_NAME = "qa-test-consume";

    private static Topic topic() {
        return Topic.builder().name(TOPIC_NAME).build();
    }

    private static Message message(String key) {
        return Message.builder()
                .topic(topic())
                .key(key)
                .content("{\"key\":\"" + key + "\"}")
                .build();
    }

    @BeforeEach
    void setUp() {
        service = new MessageConsumptionService(consumer, consumerGroupReader, "test-group");
    }

    // ═══════════════════════════════════════════════════════════════════════
    // subscribe — domain валидация
    // ═══════════════════════════════════════════════════════════════════════

    @Test
    @DisplayName("subscribeToTopic делегирует в consumer.subscribe(topic)")
    void shouldDelegateSubscribeToSingleTopic() {
        service.subscribeToTopic(topic());
        verify(consumer).subscribe(topic());
    }

    @Test
    @DisplayName("subscribeToTopic бросает NullPointerException при null имени топика (до вызова порта)")
    void shouldThrowWhenTopicNameNull() {
        Topic invalid = Topic.builder().build(); // name = null

        assertThatThrownBy(() -> service.subscribeToTopic(invalid))
                .isInstanceOf(NullPointerException.class);

        verifyNoInteractions(consumer);
    }

    @Test
    @DisplayName("subscribeToTopic бросает IllegalArgumentException при пустом имени (до вызова порта)")
    void shouldThrowWhenTopicNameBlank() {
        Topic invalid = Topic.builder().name("  ").build();

        assertThatThrownBy(() -> service.subscribeToTopic(invalid))
                .isInstanceOf(IllegalArgumentException.class);

        verifyNoInteractions(consumer);
    }

    @Test
    @DisplayName("subscribeToTopics делегирует Set топиков в consumer")
    void shouldDelegateSubscribeToMultipleTopics() {
        Set<Topic> topics = Set.of(topic(), Topic.builder().name("another-topic").build());

        service.subscribeToTopics(topics);

        verify(consumer).subscribe(topics);
    }

    @Test
    @DisplayName("subscribeToTopics бросает при невалидном топике в Set (до вызова порта)")
    void shouldThrowWhenAnyTopicInSetInvalid() {
        Set<Topic> topics = Set.of(
                topic(),
                Topic.builder().name("  ").build() // невалидный
        );

        assertThatThrownBy(() -> service.subscribeToTopics(topics))
                .isInstanceOf(IllegalArgumentException.class);

        verifyNoInteractions(consumer);
    }

    // ═══════════════════════════════════════════════════════════════════════
    // consume — делегирование и возврат результатов
    // ═══════════════════════════════════════════════════════════════════════

    @Test
    @DisplayName("consumeMessages делегирует poll и возвращает результат")
    void shouldDelegateConsumeMessages() {
        ConsumeResult expected = ConsumeResult.success(List.of(message("k1")));
        when(consumer.poll(Duration.ofSeconds(5))).thenReturn(expected);

        ConsumeResult result = service.consumeMessages(Duration.ofSeconds(5));

        assertThat(result.isSuccess()).isTrue();
        assertThat(result.getMessageCount()).isEqualTo(1);
        verify(consumer).poll(Duration.ofSeconds(5));
    }

    @Test
    @DisplayName("consumeExpectedMessages делегирует pollMessages")
    void shouldDelegateConsumeExpectedMessages() {
        List<Message> msgs = List.of(message("k1"), message("k2"));
        ConsumeResult expected = ConsumeResult.success(msgs);
        when(consumer.pollMessages(2, Duration.ofSeconds(10))).thenReturn(expected);

        ConsumeResult result = service.consumeExpectedMessages(2, Duration.ofSeconds(10));

        assertThat(result.getMessageCount()).isEqualTo(2);
        verify(consumer).pollMessages(2, Duration.ofSeconds(10));
    }

    @Test
    @DisplayName("consumeAllMessages делегирует consumeAll")
    void shouldDelegateConsumeAllMessages() {
        ConsumeResult expected = ConsumeResult.success(List.of(message("k1")));
        when(consumer.consumeAll(100, Duration.ofSeconds(10))).thenReturn(expected);

        ConsumeResult result = service.consumeAllMessages(100, Duration.ofSeconds(10));

        assertThat(result.getMessageCount()).isEqualTo(1);
        verify(consumer).consumeAll(100, Duration.ofSeconds(10));
    }

    // ═══════════════════════════════════════════════════════════════════════
    // consumeUntil — Optional семантика
    // ═══════════════════════════════════════════════════════════════════════

    @Test
    @DisplayName("consumeUntil возвращает Optional.empty() если условие не выполнено за maxAttempts")
    void shouldReturnEmptyWhenConditionNeverMet() {
        // каждый poll возвращает сообщение с другим ключом
        when(consumer.poll(any())).thenReturn(ConsumeResult.success(List.of(message("wrong-key"))));

        Optional<Message> result = service.consumeUntil(
                msg -> "target-key".equals(msg.getKey()),
                "key=target-key",
                3,
                Duration.ofMillis(50)
        );

        assertThat(result).isEmpty();
        verify(consumer, times(3)).poll(any());
    }

    @Test
    @DisplayName("consumeUntil возвращает Optional<Message> при совпадении условия")
    void shouldReturnMessageWhenConditionMet() {
        Message target = message("target-key");
        when(consumer.poll(any()))
                .thenReturn(ConsumeResult.success(List.of(message("wrong"))))  // 1-я попытка — мимо
                .thenReturn(ConsumeResult.success(List.of(target)));                // 2-я — совпадение

        Optional<Message> result = service.consumeUntil(
                msg -> "target-key".equals(msg.getKey()),
                "key=target-key",
                5,
                Duration.ofMillis(50)
        );

        assertThat(result).isPresent();
        assertThat(result.get().getKey()).isEqualTo("target-key");
    }

    @Test
    @DisplayName("consumeUntil возвращает Optional.empty() при пустых результатах от consumer")
    void shouldReturnEmptyWhenConsumerAlwaysReturnsEmpty() {
        when(consumer.poll(any())).thenReturn(ConsumeResult.empty());

        Optional<Message> result = service.consumeUntil(
                msg -> true,
                "any message",
                2,
                Duration.ofMillis(50)
        );

        assertThat(result).isEmpty();
    }

    // ═══════════════════════════════════════════════════════════════════════
    // consumeUntilOrThrow — исключение при неудаче
    // ═══════════════════════════════════════════════════════════════════════

    @Test
    @DisplayName("consumeUntilOrThrow бросает MessageNotFoundException если условие не выполнено")
    void shouldThrowMessageNotFoundExceptionWhenConditionNotMet() {
        when(consumer.poll(any())).thenReturn(ConsumeResult.empty());

        assertThatThrownBy(() -> service.consumeUntilOrThrow(
                msg -> true,
                "any message",
                2,
                Duration.ofMillis(50)
        )).isInstanceOf(MessageNotFoundException.class);
    }

    @Test
    @DisplayName("consumeUntilOrThrow возвращает сообщение при совпадении условия")
    void shouldReturnMessageWhenConditionMetOrThrow() {
        Message target = message("found");
        when(consumer.poll(any()))
                .thenReturn(ConsumeResult.success(List.of(target)));

        Message result = service.consumeUntilOrThrow(
                msg -> "found".equals(msg.getKey()),
                "key=found",
                3,
                Duration.ofMillis(50)
        );

        assertThat(result.getKey()).isEqualTo("found");
    }

    // ═══════════════════════════════════════════════════════════════════════
    // seek / commit / close
    // ═══════════════════════════════════════════════════════════════════════

    @Test
    @DisplayName("seekToBeginning() делегирует в consumer")
    void shouldDelegateSeekToBeginning() {
        service.seekToBeginning();
        verify(consumer).seekToBeginning();
    }

    @Test
    @DisplayName("seekToEnd() делегирует в consumer")
    void shouldDelegateSeekToEnd() {
        service.seekToEnd();
        verify(consumer).seekToEnd();
    }

    @Test
    @DisplayName("commitOffsets() вызывает consumer.commitSync()")
    void shouldDelegateCommitOffsets() {
        service.commitOffsets();
        verify(consumer).commitSync();
    }

    @Test
    @DisplayName("seekToOffset делегирует consumer.seek с корректными параметрами")
    void shouldDelegateSeekToOffset() {
        service.seekToOffset(topic(), 2, 100L);
        verify(consumer).seek(topic(), 2, 100L);
    }

    @Test
    @DisplayName("close() делегирует в consumer.close()")
    void shouldDelegateClose() {
        service.close();
        verify(consumer).close();
    }
}
