package tests.listeners;

import io.qameta.allure.Allure;
import lombok.extern.slf4j.Slf4j;
import org.junit.jupiter.api.extension.AfterEachCallback;
import org.junit.jupiter.api.extension.BeforeEachCallback;
import org.junit.jupiter.api.extension.ExtensionContext;

import java.io.ByteArrayInputStream;
import java.nio.charset.StandardCharsets;
import java.time.Duration;
import java.time.Instant;

/**
 * Kafka-specific test execution listener
 * Tracks test execution time and attaches Kafka-specific metadata to Allure
 */
@Slf4j
public class KafkaTestExecutionListener implements BeforeEachCallback, AfterEachCallback {

    private static final String START_TIME_KEY = "startTime";

    @Override
    public void beforeEach(ExtensionContext context) {
        Instant startTime = Instant.now();
        context.getStore(ExtensionContext.Namespace.GLOBAL).put(START_TIME_KEY, startTime);

        String testName = getTestName(context);
        log.info("▶ Starting test: {}", testName);
        log.debug("Thread: {}", Thread.currentThread().getName());
    }

    @Override
    public void afterEach(ExtensionContext context) {
        Instant startTime = context.getStore(ExtensionContext.Namespace.GLOBAL)
                .get(START_TIME_KEY, Instant.class);

        if (startTime != null) {
            Duration duration = Duration.between(startTime, Instant.now());
            String testName = getTestName(context);

            log.info("◼ Finished test: {} in {} ms", testName, duration.toMillis());

            attachExecutionTime(duration);
        }
    }

    /**
     * Gets formatted test name
     */
    private String getTestName(ExtensionContext context) {
        String className = context.getTestClass()
                .map(Class::getSimpleName)
                .orElse("Unknown");
        String methodName = context.getTestMethod()
                .map(m -> m.getName())
                .orElse("unknown");

        return className + "." + methodName;
    }

    /**
     * Attaches execution time to Allure report
     */
    private void attachExecutionTime(Duration duration) {
        String timeInfo = String.format("Test Execution Time: %d ms (%d seconds)",
                duration.toMillis(),
                duration.getSeconds());

        Allure.addAttachment("Execution Time", "text/plain",
                new ByteArrayInputStream(timeInfo.getBytes(StandardCharsets.UTF_8)), "txt");
    }
}
