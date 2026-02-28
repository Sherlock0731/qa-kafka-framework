package qa.autotest.framework.tests;

import org.junit.jupiter.api.*;
import qa.autotest.framework.domain.model.*;

import java.util.List;

import static org.assertj.core.api.Assertions.*;
import tests.KafkaAssertions;
import static tests.KafkaAssertions.*;

/**
 * Unit-тесты для {@link KafkaAssertions}.
 * <p>
 * Стратегия: каждый assertion-метод тестируется в двух сценариях —
 * <em>проходит</em> (green path) и <em>падает с AssertionError</em> (red path).
 * Это гарантирует, что методы не являются no-op и действительно проверяют условие.
 *
 * <h3>Покрываемые методы</h3>
 * <ul>
 *   <li>{@code assertPublishSuccess} / {@code assertPublishFailed} / {@code assertPublishRetryable}</li>
 *   <li>{@code assertConsumeSuccess}</li>
 *   <li>{@code assertMessageCount} / {@code assertMessageCountAtLeast} / {@code assertHasMessages}</li>
 *   <li>{@code assertMessageContent} / {@code assertMessageKey}</li>
 *   <li>{@code assertMessageHasHeader} / {@code assertMessageHeaderValue}</li>
 * </ul>
 */
@Tag("unit")
@DisplayName("KafkaAssertions")
class KafkaAssertionsTest {

    private static final Topic TOPIC = Topic.builder().name("qa-test-assert").build();

    private static Message message(String key, String content) {
        return Message.builder()
                .topic(TOPIC)
                .key(key)
                .content(content)
                .build();
    }

    private static PublishResult successResult() {
        return PublishResult.success(message("k", "v"), 0, 1L, System.currentTimeMillis());
    }

    private static PublishResult retryableFailure() {
        return PublishResult.failure(message("k", "v"), "Timeout", KafkaErrorCategory.TIMEOUT_ERROR);
    }

    private static PublishResult nonRetryableFailure() {
        return PublishResult.failure(message("k", "v"), "Auth", KafkaErrorCategory.AUTHENTICATION_ERROR);
    }

    private static ConsumeResult successConsumeResult(int messageCount) {
        List<Message> msgs = new java.util.ArrayList<>();
        for (int i = 0; i < messageCount; i++) {
            msgs.add(message("k" + i, "v" + i));
        }
        return ConsumeResult.success(msgs);
    }

    private static ConsumeResult failureConsumeResult() {
        return ConsumeResult.failure("Broker down", KafkaErrorCategory.NETWORK_ERROR);
    }

    // ═══════════════════════════════════════════════════════════════════════════
    // assertPublishSuccess
    // ═══════════════════════════════════════════════════════════════════════════

    @Nested
    @DisplayName("assertPublishSuccess")
    class AssertPublishSuccessTests {

        @Test
        @DisplayName("проходит при isSuccess()=true")
        void shouldPassForSuccessResult() {
            assertThatNoException().isThrownBy(() ->
                    assertPublishSuccess(successResult()));
        }

        @Test
        @DisplayName("падает с AssertionError при isSuccess()=false")
        void shouldFailForFailureResult() {
            assertThatThrownBy(() -> assertPublishSuccess(nonRetryableFailure()))
                    .isInstanceOf(AssertionError.class)
                    .hasMessageContaining("Publish failed");
        }

        @Test
        @DisplayName("сообщение об ошибке содержит errorMessage из результата")
        void shouldIncludeErrorMessageInFailure() {
            PublishResult failed = PublishResult.failure(
                    message("k", "v"), "Broker unavailable", KafkaErrorCategory.BROKER_NOT_AVAILABLE);

            assertThatThrownBy(() -> assertPublishSuccess(failed))
                    .hasMessageContaining("Broker unavailable");
        }
    }

    // ═══════════════════════════════════════════════════════════════════════════
    // assertPublishFailed
    // ═══════════════════════════════════════════════════════════════════════════

    @Nested
    @DisplayName("assertPublishFailed")
    class AssertPublishFailedTests {

        @Test
        @DisplayName("проходит при isSuccess()=false")
        void shouldPassForFailureResult() {
            assertThatNoException().isThrownBy(() ->
                    assertPublishFailed(nonRetryableFailure()));
        }

        @Test
        @DisplayName("падает с AssertionError при isSuccess()=true")
        void shouldFailForSuccessResult() {
            assertThatThrownBy(() -> assertPublishFailed(successResult()))
                    .isInstanceOf(AssertionError.class)
                    .hasMessageContaining("Expected publish to fail");
        }
    }

    // ═══════════════════════════════════════════════════════════════════════════
    // assertPublishRetryable
    // ═══════════════════════════════════════════════════════════════════════════

    @Nested
    @DisplayName("assertPublishRetryable")
    class AssertPublishRetryableTests {

        @Test
        @DisplayName("проходит для retryable failure (TIMEOUT_ERROR)")
        void shouldPassForRetryableFailure() {
            assertThatNoException().isThrownBy(() ->
                    assertPublishRetryable(retryableFailure()));
        }

        @Test
        @DisplayName("падает для non-retryable failure (AUTHENTICATION_ERROR)")
        void shouldFailForNonRetryableFailure() {
            assertThatThrownBy(() -> assertPublishRetryable(nonRetryableFailure()))
                    .isInstanceOf(AssertionError.class)
                    .hasMessageContaining("retryable");
        }

        @Test
        @DisplayName("падает для success-результата (нет ошибки для проверки)")
        void shouldFailForSuccessResult() {
            assertThatThrownBy(() -> assertPublishRetryable(successResult()))
                    .isInstanceOf(AssertionError.class);
        }
    }

    // ═══════════════════════════════════════════════════════════════════════════
    // assertConsumeSuccess
    // ═══════════════════════════════════════════════════════════════════════════

    @Nested
    @DisplayName("assertConsumeSuccess")
    class AssertConsumeSuccessTests {

        @Test
        @DisplayName("проходит при isSuccess()=true")
        void shouldPassForSuccessResult() {
            assertThatNoException().isThrownBy(() ->
                    assertConsumeSuccess(successConsumeResult(3)));
        }

        @Test
        @DisplayName("проходит для empty() — success=true, messageCount=0")
        void shouldPassForEmptyResult() {
            assertThatNoException().isThrownBy(() ->
                    assertConsumeSuccess(ConsumeResult.empty()));
        }

        @Test
        @DisplayName("падает с AssertionError при isSuccess()=false")
        void shouldFailForFailureResult() {
            assertThatThrownBy(() -> assertConsumeSuccess(failureConsumeResult()))
                    .isInstanceOf(AssertionError.class)
                    .hasMessageContaining("Consume failed");
        }
    }

    // ═══════════════════════════════════════════════════════════════════════════
    // assertMessageCount
    // ═══════════════════════════════════════════════════════════════════════════

    @Nested
    @DisplayName("assertMessageCount")
    class AssertMessageCountTests {

        @Test
        @DisplayName("проходит при точном совпадении числа сообщений")
        void shouldPassWhenCountMatches() {
            assertThatNoException().isThrownBy(() ->
                    assertMessageCount(successConsumeResult(5), 5));
        }

        @Test
        @DisplayName("падает при несовпадении числа сообщений")
        void shouldFailWhenCountMismatches() {
            assertThatThrownBy(() -> assertMessageCount(successConsumeResult(3), 5))
                    .isInstanceOf(AssertionError.class)
                    .hasMessageContaining("Expected 5 messages")
                    .hasMessageContaining("got 3");
        }

        @Test
        @DisplayName("проходит для 0 сообщений при expectedCount=0")
        void shouldPassForZeroMessages() {
            assertThatNoException().isThrownBy(() ->
                    assertMessageCount(ConsumeResult.empty(), 0));
        }
    }

    // ═══════════════════════════════════════════════════════════════════════════
    // assertMessageCountAtLeast
    // ═══════════════════════════════════════════════════════════════════════════

    @Nested
    @DisplayName("assertMessageCountAtLeast")
    class AssertMessageCountAtLeastTests {

        @Test
        @DisplayName("проходит когда count > minCount")
        void shouldPassWhenCountExceedsMin() {
            assertThatNoException().isThrownBy(() ->
                    assertMessageCountAtLeast(successConsumeResult(10), 5));
        }

        @Test
        @DisplayName("проходит когда count == minCount")
        void shouldPassWhenCountEqualsMin() {
            assertThatNoException().isThrownBy(() ->
                    assertMessageCountAtLeast(successConsumeResult(3), 3));
        }

        @Test
        @DisplayName("падает когда count < minCount")
        void shouldFailWhenCountBelowMin() {
            assertThatThrownBy(() -> assertMessageCountAtLeast(successConsumeResult(2), 5))
                    .isInstanceOf(AssertionError.class)
                    .hasMessageContaining("at least 5")
                    .hasMessageContaining("got 2");
        }
    }

    // ═══════════════════════════════════════════════════════════════════════════
    // assertHasMessages
    // ═══════════════════════════════════════════════════════════════════════════

    @Nested
    @DisplayName("assertHasMessages")
    class AssertHasMessagesTests {

        @Test
        @DisplayName("проходит при наличии хотя бы одного сообщения")
        void shouldPassWhenResultHasMessages() {
            assertThatNoException().isThrownBy(() ->
                    assertHasMessages(successConsumeResult(1)));
        }

        @Test
        @DisplayName("падает при пустом результате")
        void shouldFailForEmptyResult() {
            assertThatThrownBy(() -> assertHasMessages(ConsumeResult.empty()))
                    .isInstanceOf(AssertionError.class)
                    .hasMessageContaining("at least one message");
        }
    }

    // ═══════════════════════════════════════════════════════════════════════════
    // assertMessageContent
    // ═══════════════════════════════════════════════════════════════════════════

    @Nested
    @DisplayName("assertMessageContent")
    class AssertMessageContentTests {

        @Test
        @DisplayName("проходит при совпадении content")
        void shouldPassWhenContentMatches() {
            Message msg = message("key", "{\"id\":42}");
            assertThatNoException().isThrownBy(() ->
                    assertMessageContent(msg, "{\"id\":42}"));
        }

        @Test
        @DisplayName("падает при несовпадении content")
        void shouldFailWhenContentMismatches() {
            Message msg = message("key", "{\"id\":42}");
            assertThatThrownBy(() -> assertMessageContent(msg, "{\"id\":99}"))
                    .isInstanceOf(AssertionError.class)
                    .hasMessageContaining("Message content mismatch");
        }
    }

    // ═══════════════════════════════════════════════════════════════════════════
    // assertMessageKey
    // ═══════════════════════════════════════════════════════════════════════════

    @Nested
    @DisplayName("assertMessageKey")
    class AssertMessageKeyTests {

        @Test
        @DisplayName("проходит при совпадении key")
        void shouldPassWhenKeyMatches() {
            Message msg = message("order-123", "data");
            assertThatNoException().isThrownBy(() ->
                    assertMessageKey(msg, "order-123"));
        }

        @Test
        @DisplayName("падает при несовпадении key")
        void shouldFailWhenKeyMismatches() {
            Message msg = message("order-123", "data");
            assertThatThrownBy(() -> assertMessageKey(msg, "order-456"))
                    .isInstanceOf(AssertionError.class)
                    .hasMessageContaining("Message key mismatch");
        }
    }

    // ═══════════════════════════════════════════════════════════════════════════
    // assertMessageHasHeader
    // ═══════════════════════════════════════════════════════════════════════════

    @Nested
    @DisplayName("assertMessageHasHeader")
    class AssertMessageHasHeaderTests {

        @Test
        @DisplayName("проходит когда header присутствует в сообщении")
        void shouldPassWhenHeaderPresent() {
            Message msg = message("k", "v").withHeader("event-type", "ORDER_CREATED");
            assertThatNoException().isThrownBy(() ->
                    assertMessageHasHeader(msg, "event-type"));
        }

        @Test
        @DisplayName("падает когда header отсутствует")
        void shouldFailWhenHeaderAbsent() {
            Message msg = message("k", "v"); // без headers
            assertThatThrownBy(() -> assertMessageHasHeader(msg, "event-type"))
                    .isInstanceOf(AssertionError.class)
                    .hasMessageContaining("event-type")
                    .hasMessageContaining("not found");
        }

        @Test
        @DisplayName("падает когда headers null (сообщение без headers)")
        void shouldFailWhenHeadersNull() {
            Message msg = message("k", "v");
            assertThatThrownBy(() -> assertMessageHasHeader(msg, "trace-id"))
                    .isInstanceOf(AssertionError.class);
        }
    }

    // ═══════════════════════════════════════════════════════════════════════════
    // assertMessageHeaderValue
    // ═══════════════════════════════════════════════════════════════════════════

    @Nested
    @DisplayName("assertMessageHeaderValue")
    class AssertMessageHeaderValueTests {

        @Test
        @DisplayName("проходит при совпадении значения header")
        void shouldPassWhenHeaderValueMatches() {
            Message msg = message("k", "v").withHeader("event-type", "ORDER_CREATED");
            assertThatNoException().isThrownBy(() ->
                    assertMessageHeaderValue(msg, "event-type", "ORDER_CREATED"));
        }

        @Test
        @DisplayName("падает когда значение header не совпадает")
        void shouldFailWhenHeaderValueMismatches() {
            Message msg = message("k", "v").withHeader("event-type", "ORDER_CREATED");
            assertThatThrownBy(() -> assertMessageHeaderValue(msg, "event-type", "PAYMENT_PROCESSED"))
                    .isInstanceOf(AssertionError.class)
                    .hasMessageContaining("event-type");
        }

        @Test
        @DisplayName("падает когда header полностью отсутствует")
        void shouldFailWhenHeaderAbsent() {
            Message msg = message("k", "v");
            assertThatThrownBy(() -> assertMessageHeaderValue(msg, "trace-id", "abc"))
                    .isInstanceOf(AssertionError.class)
                    .hasMessageContaining("trace-id");
        }

        @Test
        @DisplayName("несколько headers — проверка выбирает правильный ключ")
        void shouldCheckCorrectHeaderAmongMultiple() {
            Message msg = message("k", "v")
                    .withHeader("trace-id", "xyz")
                    .withHeader("event-type", "ORDER_CREATED");

            assertThatNoException().isThrownBy(() ->
                    assertMessageHeaderValue(msg, "trace-id", "xyz"));
            assertThatNoException().isThrownBy(() ->
                    assertMessageHeaderValue(msg, "event-type", "ORDER_CREATED"));
        }
    }

    // ═══════════════════════════════════════════════════════════════════════════
    // Утилитный класс — невозможность инстанцирования
    // ═══════════════════════════════════════════════════════════════════════════

    @Test
    @DisplayName("KafkaAssertions — утилитный класс, конструктор бросает UnsupportedOperationException")
    void shouldBeNonInstantiable() throws Exception {
        var constructor = KafkaAssertions.class.getDeclaredConstructor();
        constructor.setAccessible(true);
        assertThatThrownBy(constructor::newInstance)
                .hasCauseInstanceOf(UnsupportedOperationException.class);
    }
}
