package tests.listeners;

import io.qameta.allure.Allure;
import lombok.extern.slf4j.Slf4j;
import org.junit.jupiter.api.extension.AfterEachCallback;
import org.junit.jupiter.api.extension.BeforeEachCallback;
import org.junit.jupiter.api.extension.ExtensionContext;
import org.slf4j.MDC;

import java.io.ByteArrayInputStream;
import java.nio.charset.StandardCharsets;
import java.time.Duration;
import java.time.Instant;
import java.util.UUID;

/**
 * JUnit 5 extension: per-test MDC population + Allure execution time.
 *
 * <h3>MDC lifecycle</h3>
 * <pre>
 *   beforeEach → MDC.put(test.id / test.class / test.method / test.thread)
 *   afterEach  → MDC.clear()   ← обязательно, иначе контекст утечёт в пул потоков
 * </pre>
 *
 * <h3>Параллельный запуск</h3>
 * SLF4J MDC хранит значения в {@link ThreadLocal}.  Каждый поток получает
 * независимый контекст, поэтому при параллельном запуске тестов каждая
 * запись лога несёт идентификатор именно того теста, что выполняется в
 * данном потоке — атрибуция работает без какой-либо синхронизации.
 *
 * <h3>Ключи</h3>
 * Все ключи определены в {@link MdcKeys} и продублированы в {@code logback.xml}
 * через {@code %X{...}}.  Изменение в одном месте автоматически ломает сборку,
 * если не обновлено второе.
 */
@Slf4j
public class KafkaTestExecutionListener implements BeforeEachCallback, AfterEachCallback {

    private static final String START_TIME_KEY = "startTime";

    @Override
    public void beforeEach(ExtensionContext context) {
        populateMdc(context);

        context.getStore(ExtensionContext.Namespace.GLOBAL)
               .put(START_TIME_KEY, Instant.now());

        log.info("▶ Starting test: {}", getTestName(context));
    }

    @Override
    public void afterEach(ExtensionContext context) {
        try {
            Instant startTime = context.getStore(ExtensionContext.Namespace.GLOBAL)
                                       .get(START_TIME_KEY, Instant.class);

            if (startTime != null) {
                Duration duration = Duration.between(startTime, Instant.now());
                log.info("◼ Finished test: {} in {} ms", getTestName(context), duration.toMillis());
                attachExecutionTime(duration);
            }
        } finally {
            // MDC MUST be cleared in finally — thread pool reuse would leak stale context
            MDC.clear();
        }
    }

    /**
     * Заполняет MDC четырьмя ключами перед запуском теста.
     *
     * <p>{@code test.id} — последние 8 символов случайного UUID, достаточно
     * уникальные для однозначной корреляции в рамках одного прогона и при
     * этом короткие, чтобы не засорять лог.
     */
    private void populateMdc(ExtensionContext context) {
        String shortId = UUID.randomUUID().toString().replace("-", "").substring(24); // 8 chars

        MDC.put(MdcKeys.TEST_ID,     shortId);
        MDC.put(MdcKeys.TEST_CLASS,  context.getTestClass()
                                            .map(Class::getSimpleName)
                                            .orElse("Unknown"));
        MDC.put(MdcKeys.TEST_METHOD, context.getTestMethod()
                                            .map(m -> m.getName())
                                            .orElse("unknown"));
        MDC.put(MdcKeys.TEST_THREAD, Thread.currentThread().getName());
    }

    private String getTestName(ExtensionContext context) {
        String cls = context.getTestClass().map(Class::getSimpleName).orElse("Unknown");
        String mtd = context.getTestMethod().map(m -> m.getName()).orElse("unknown");
        return cls + "." + mtd;
    }

    private void attachExecutionTime(Duration duration) {
        String timeInfo = String.format("Test Execution Time: %d ms (%d seconds)",
                duration.toMillis(), duration.getSeconds());
        Allure.addAttachment("Execution Time", "text/plain",
                new ByteArrayInputStream(timeInfo.getBytes(StandardCharsets.UTF_8)), "txt");
    }
}
