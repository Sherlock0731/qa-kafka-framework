package tests.listeners;

/**
 * MDC key constants shared between {@link KafkaTestExecutionListener} and logback configuration.
 *
 * <h3>Назначение</h3>
 * Centralises all MDC key strings so that logback patterns and Java code stay
 * in sync.  A typo in a single constant produces a compile error; a typo in an
 * inline string literal silently produces empty log fields.
 *
 * <h3>Использование в logback.xml</h3>
 * <pre>{@code
 * %X{test.id}     — короткий UUID теста (последние 8 символов)
 * %X{test.class}  — простое имя класса (ConsumerTests)
 * %X{test.method} — имя метода (shouldConsumeMessage)
 * %X{test.thread} — имя потока JVM (pool-1-thread-2)
 * }</pre>
 *
 * <h3>Параллельный запуск</h3>
 * MDC в Logback хранится в {@link ThreadLocal} — каждый поток видит только
 * свой контекст.  При параллельном запуске тестов каждая запись лога несёт
 * идентификатор конкретного теста, выполняемого в данном потоке, что
 * позволяет однозначно атрибутировать лог без дополнительной корреляции.
 */
public final class MdcKeys {

    /**
     * Короткий идентификатор теста: последние 8 символов UUID.
     */
    public static final String TEST_ID = "test.id";

    /**
     * Простое имя тест-класса, например {@code ConsumerTests}.
     */
    public static final String TEST_CLASS = "test.class";

    /**
     * Имя тест-метода, например {@code shouldConsumeFromBeginning}.
     */
    public static final String TEST_METHOD = "test.method";

    /**
     * Имя потока JVM, например {@code pool-1-thread-2}.
     */
    public static final String TEST_THREAD = "test.thread";

    private MdcKeys() {
        throw new UnsupportedOperationException("constants class");
    }
}
