package tests;

import org.junit.jupiter.api.Assertions;
import qa.autotest.framework.domain.model.ConsumeResult;
import qa.autotest.framework.domain.model.Message;
import qa.autotest.framework.domain.model.PublishResult;

/**
 * KafkaAssertions — Ответственность: domain-ориентированные assertion-методы.
 * <p>
 * Единственная задача: предоставлять читаемые assertion-методы поверх
 * {@code PublishResult}, {@code ConsumeResult} и {@code Message},
 * делегируя фактические проверки в JUnit {@link Assertions}.
 *
 * <h3>Почему статический утильный класс, а не базовый класс</h3>
 * Java не поддерживает множественное наследование. Цепочка наследования
 * уже занята жизненным циклом:
 * <pre>
 *   KafkaTestBase → KafkaTestHelpers → BaseTest → ConcreteTests
 * </pre>
 * Встраивание ассертов в эту цепочку нарушало бы SRP у {@code KafkaTestHelpers}.
 * Статический утильный класс решает задачу без компромиссов:
 * <ul>
 *   <li>Не занимает слот наследования.</li>
 *   <li>Не несёт состояния — чистые функции.</li>
 *   <li>Тестируется независимо от lifecycle-инфраструктуры.</li>
 *   <li>Подключается одной строкой: {@code import static tests.KafkaAssertions.*}</li>
 * </ul>
 *
 * <h3>Использование в тест-классах</h3>
 * <pre>{@code
 * import static tests.KafkaAssertions.*;
 *
 * assertPublishSuccess(result);
 * assertConsumeSuccess(result);
 * assertMessageCount(result, 5);
 * assertMessageContent(message, "{\"key\":\"value\"}");
 * assertMessageKey(message, "order-123");
 * assertMessageHasHeader(message, "event-type");
 * assertResultIsRetryable(result);
 * }</pre>
 */
public final class KafkaAssertions {

    private KafkaAssertions() {
        throw new UnsupportedOperationException("Utility class — use static methods");
    }

    // ── PublishResult ─────────────────────────────────────────────────────────

    /**
     * Проверяет, что публикация завершилась успешно.
     *
     * @param result результат публикации
     * @throws AssertionError если {@code result.isSuccess() == false}
     */
    public static void assertPublishSuccess(PublishResult result) {
        Assertions.assertTrue(
                result.isSuccess(),
                "Publish failed: " + result.getErrorMessage());
    }

    /**
     * Проверяет, что публикация завершилась с ошибкой.
     * Используется в error-handling тестах.
     *
     * @param result результат публикации
     */
    public static void assertPublishFailed(PublishResult result) {
        Assertions.assertFalse(
                result.isSuccess(),
                "Expected publish to fail, but it succeeded");
    }

    /**
     * Проверяет, что ошибка публикации является повторяемой ({@code retryable = true}).
     */
    public static void assertPublishRetryable(PublishResult result) {
        Assertions.assertFalse(result.isSuccess(), "Expected a failed result to check retryable");
        Assertions.assertTrue(
                result.isRetryable(),
                "Expected retryable error, got: " + result.getErrorMessage());
    }

    // ── ConsumeResult ─────────────────────────────────────────────────────────

    /**
     * Проверяет, что потребление завершилось успешно.
     *
     * @param result результат потребления
     * @throws AssertionError если {@code result.isSuccess() == false}
     */
    public static void assertConsumeSuccess(ConsumeResult result) {
        Assertions.assertTrue(
                result.isSuccess(),
                "Consume failed: " + result.getErrorMessage());
    }

    /**
     * Проверяет точное совпадение числа полученных сообщений.
     *
     * @param result        результат потребления
     * @param expectedCount ожидаемое число сообщений
     */
    public static void assertMessageCount(ConsumeResult result, int expectedCount) {
        Assertions.assertEquals(
                expectedCount,
                result.getMessageCount(),
                "Expected " + expectedCount + " messages, got " + result.getMessageCount());
    }

    /**
     * Проверяет, что число полученных сообщений не менее {@code minCount}.
     */
    public static void assertMessageCountAtLeast(ConsumeResult result, int minCount) {
        Assertions.assertTrue(
                result.getMessageCount() >= minCount,
                "Expected at least " + minCount + " messages, got " + result.getMessageCount());
    }

    /**
     * Проверяет, что результат содержит хотя бы одно сообщение.
     */
    public static void assertHasMessages(ConsumeResult result) {
        Assertions.assertTrue(
                result.hasMessages(),
                "Expected at least one message, but result is empty");
    }

    // ── Message ───────────────────────────────────────────────────────────────

    /**
     * Проверяет содержимое (payload) сообщения.
     *
     * @param message         полученное сообщение
     * @param expectedContent ожидаемое содержимое
     */
    public static void assertMessageContent(Message message, String expectedContent) {
        Assertions.assertEquals(
                expectedContent,
                message.getContent(),
                "Message content mismatch");
    }

    /**
     * Проверяет ключ сообщения.
     *
     * @param message     полученное сообщение
     * @param expectedKey ожидаемый ключ
     */
    public static void assertMessageKey(Message message, String expectedKey) {
        Assertions.assertEquals(
                expectedKey,
                message.getKey(),
                "Message key mismatch");
    }

    /**
     * Проверяет наличие заданного header-ключа в сообщении.
     *
     * @param message    полученное сообщение
     * @param headerName имя header-ключа
     */
    public static void assertMessageHasHeader(Message message, String headerName) {
        Assertions.assertTrue(
                message.getHeaders() != null && message.getHeaders().containsKey(headerName),
                "Expected header '" + headerName + "' not found in message");
    }

    /**
     * Проверяет значение заданного header в сообщении.
     *
     * @param message       полученное сообщение
     * @param headerName    имя header-ключа
     * @param expectedValue ожидаемое значение
     */
    public static void assertMessageHeaderValue(Message message, String headerName, String expectedValue) {
        assertMessageHasHeader(message, headerName);
        Assertions.assertEquals(
                expectedValue,
                message.getHeaders().get(headerName),
                "Header '" + headerName + "' value mismatch");
    }
}
