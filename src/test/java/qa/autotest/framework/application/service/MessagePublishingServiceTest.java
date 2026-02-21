package qa.autotest.framework.application.service;

import org.junit.jupiter.api.*;
import org.junit.jupiter.api.extension.ExtendWith;
import org.mockito.InOrder;
import org.mockito.Mock;
import org.mockito.junit.jupiter.MockitoExtension;
import qa.autotest.framework.domain.model.*;
import qa.autotest.framework.domain.port.MessagePublisher;

import java.util.List;
import java.util.concurrent.CompletableFuture;

import static org.assertj.core.api.Assertions.*;
import static org.mockito.ArgumentMatchers.*;
import static org.mockito.Mockito.*;

/**
 * Unit-тесты для MessagePublishingService.
 * <p>
 * Покрываемые сценарии:
 * - publishMessage: domain-валидация перед делегированием
 * - publishMessage: корректный возврат результата от порта
 * - publishBatch: валидация каждого сообщения, делегирование, агрегация
 * - publishWithRetry: успех на первой попытке
 * - publishWithRetry: успех после N ретраев (retryable error)
 * - publishWithRetry: немедленный возврат при non-retryable ошибке
 * - publishWithRetry: возврат последнего failure после исчерпания ретраев
 * - publishMessage бросает IllegalArgumentException при невалидном сообщении (до вызова порта)
 * - flush() делегирует
 * - close() делегирует
 */
@DisplayName("MessagePublishingService")
@ExtendWith(MockitoExtension.class)
class MessagePublishingServiceTest {

    @Mock
    MessagePublisher publisher;

    private MessagePublishingService service;

    // ── fixtures ──────────────────────────────────────────────────────────
    private static final String TOPIC_NAME = "qa-test-publish";

    private static Message validMessage() {
        return Message.builder()
                .topic(Topic.builder().name(TOPIC_NAME).build())
                .key("order-1")
                .content("{\"orderId\":1}")
                .build();
    }

    private static PublishResult successResult(Message msg) {
        return PublishResult.success(msg, 0, 10L, System.currentTimeMillis());
    }

    private static PublishResult retryableFailure(Message msg) {
        return PublishResult.failure(msg, "Timeout", KafkaErrorCategory.TIMEOUT_ERROR);
    }

    private static PublishResult nonRetryableFailure(Message msg) {
        return PublishResult.failure(msg, "Auth failed", KafkaErrorCategory.AUTHENTICATION_ERROR);
    }

    @BeforeEach
    void setUp() {
        service = new MessagePublishingService(publisher);
    }

    // ═══════════════════════════════════════════════════════════════════════
    // publishMessage — базовый сценарий
    // ═══════════════════════════════════════════════════════════════════════

    @Test
    @DisplayName("publishMessage делегирует в publisher и возвращает результат")
    void shouldDelegateToPublisherAndReturnResult() {
        Message msg = validMessage();
        PublishResult expected = successResult(msg);
        when(publisher.publish(msg)).thenReturn(expected);

        PublishResult result = service.publishMessage(msg);

        assertThat(result.isSuccess()).isTrue();
        assertThat(result.getOffset()).isEqualTo(10L);
        verify(publisher).publish(msg);
    }

    @Test
    @DisplayName("publishMessage бросает IllegalArgumentException при пустом content (до вызова порта)")
    void shouldThrowBeforeCallingPublisherWhenContentEmpty() {
        Message invalid = Message.builder()
                .topic(Topic.builder().name(TOPIC_NAME).build())
                .key("k")
                .content("")  // невалидное
                .build();

        assertThatThrownBy(() -> service.publishMessage(invalid))
                .isInstanceOf(IllegalArgumentException.class);

        verifyNoInteractions(publisher);
    }

    @Test
    @DisplayName("publishMessage бросает NullPointerException при null topic (до вызова порта)")
    void shouldThrowBeforeCallingPublisherWhenTopicNull() {
        Message invalid = Message.builder()
                .key("k")
                .content("data")
                // topic не выставлен
                .build();

        assertThatThrownBy(() -> service.publishMessage(invalid))
                .isInstanceOf(NullPointerException.class);

        verifyNoInteractions(publisher);
    }

    // ═══════════════════════════════════════════════════════════════════════
    // publishBatch
    // ═══════════════════════════════════════════════════════════════════════

    @Test
    @DisplayName("publishBatch делегирует список и возвращает все результаты")
    void shouldDelegateBatchAndReturnResults() {
        Message m1 = validMessage();
        Message m2 = validMessage();
        List<Message> messages = List.of(m1, m2);
        List<PublishResult> expected = List.of(successResult(m1), successResult(m2));
        when(publisher.publishBatch(messages)).thenReturn(expected);

        List<PublishResult> results = service.publishBatch(messages);

        assertThat(results).hasSize(2);
        assertThat(results).allMatch(PublishResult::isSuccess);
        verify(publisher).publishBatch(messages);
    }

    @Test
    @DisplayName("publishBatch бросает исключение при невалидном сообщении в списке (до вызова порта)")
    void shouldValidateEachMessageBeforePublishingBatch() {
        Message valid = validMessage();
        Message invalid = Message.builder()
                .topic(Topic.builder().name(TOPIC_NAME).build())
                .key("k")
                .content("")
                .build();

        assertThatThrownBy(() -> service.publishBatch(List.of(valid, invalid)))
                .isInstanceOf(IllegalArgumentException.class);

        verifyNoInteractions(publisher);
    }

    // ═══════════════════════════════════════════════════════════════════════
    // publishWithRetry
    // ═══════════════════════════════════════════════════════════════════════

    @Test
    @DisplayName("publishWithRetry возвращает успех сразу при первой попытке")
    void shouldReturnSuccessOnFirstAttempt() {
        Message msg = validMessage();
        when(publisher.publish(msg)).thenReturn(successResult(msg));

        PublishResult result = service.publishWithRetry(msg, 3);

        assertThat(result.isSuccess()).isTrue();
        // Вызван ровно один раз — ретраи не потребовались
        verify(publisher, times(1)).publish(msg);
    }

    @Test
    @DisplayName("publishWithRetry успешен на второй попытке после retryable-ошибки")
    void shouldRetryAndSucceedOnSecondAttempt() {
        Message msg = validMessage();
        // 1-я попытка — retryable failure, 2-я — success
        when(publisher.publish(msg))
                .thenReturn(retryableFailure(msg))
                .thenReturn(successResult(msg));

        // Используем spy на сервисе чтобы не ждать реальный Thread.sleep
        // Альтернативно: maxRetries=1 и убедиться что publisher вызван 2 раза
        // Мы не можем легко замокать sleep без рефакторинга, поэтому
        // проверяем поведение логики: 2 вызова publisher при maxRetries=1
        PublishResult result = service.publishWithRetry(msg, 1);

        assertThat(result.isSuccess()).isTrue();
        verify(publisher, times(2)).publish(msg);
    }

    @Test
    @DisplayName("publishWithRetry немедленно возвращает при non-retryable ошибке")
    void shouldNotRetryOnNonRetryableError() {
        Message msg = validMessage();
        when(publisher.publish(msg)).thenReturn(nonRetryableFailure(msg));

        PublishResult result = service.publishWithRetry(msg, 5);

        assertThat(result.isSuccess()).isFalse();
        assertThat(result.getErrorCategory()).isEqualTo(KafkaErrorCategory.AUTHENTICATION_ERROR);
        // Non-retryable: publisher вызван ровно 1 раз, без ретраев
        verify(publisher, times(1)).publish(msg);
    }

    @Test
    @DisplayName("publishWithRetry возвращает последний failure после исчерпания ретраев")
    void shouldReturnLastFailureAfterMaxRetries() {
        Message msg = validMessage();
        // Все попытки возвращают retryable failure
        when(publisher.publish(msg)).thenReturn(retryableFailure(msg));

        PublishResult result = service.publishWithRetry(msg, 2);

        assertThat(result.isSuccess()).isFalse();
        assertThat(result.isRetryable()).isTrue();
        // Ожидаем maxRetries+1 = 3 вызова (попытка 0, 1, 2)
        verify(publisher, times(3)).publish(msg);
    }

    @Test
    @DisplayName("publishWithRetry бросает при невалидном сообщении до первого вызова порта")
    void shouldValidateBeforeRetrying() {
        Message invalid = Message.builder()
                .topic(Topic.builder().name(TOPIC_NAME).build())
                .key("k")
                .content("")
                .build();

        assertThatThrownBy(() -> service.publishWithRetry(invalid, 3))
                .isInstanceOf(IllegalArgumentException.class);

        verifyNoInteractions(publisher);
    }

    // ═══════════════════════════════════════════════════════════════════════
    // flush / close
    // ═══════════════════════════════════════════════════════════════════════

    @Test
    @DisplayName("flush() делегирует в publisher.flush()")
    void shouldDelegateFlush() {
        service.flush();
        verify(publisher).flush();
    }

    @Test
    @DisplayName("close() делегирует в publisher.close()")
    void shouldDelegateClose() {
        service.close();
        verify(publisher).close();
    }

    // ═══════════════════════════════════════════════════════════════════════
    // publishMessageAsync
    // ═══════════════════════════════════════════════════════════════════════

    @Test
    @DisplayName("publishMessageAsync делегирует и возвращает завершённый Future при успехе")
    void shouldDelegateAsyncPublish() throws Exception {
        Message msg = validMessage();
        PublishResult expected = successResult(msg);
        when(publisher.publishAsync(msg))
                .thenReturn(CompletableFuture.completedFuture(expected));

        CompletableFuture<PublishResult> future = service.publishMessageAsync(msg);
        PublishResult result = future.get();

        assertThat(result.isSuccess()).isTrue();
        verify(publisher).publishAsync(msg);
    }
}
