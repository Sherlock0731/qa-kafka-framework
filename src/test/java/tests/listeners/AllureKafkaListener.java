package tests.listeners;

import io.qameta.allure.Allure;
import io.qameta.allure.model.Status;
import lombok.extern.slf4j.Slf4j;
import org.junit.jupiter.api.extension.ExtensionContext;
import org.junit.jupiter.api.extension.TestWatcher;
import qa.autotest.framework.exceptions.KafkaTestException;
import qa.autotest.framework.metrics.TestMetricsCollector;

import java.io.ByteArrayInputStream;
import java.nio.charset.StandardCharsets;
import java.time.LocalDateTime;
import java.time.format.DateTimeFormatter;
import java.util.Optional;

/**
 * Enhanced Allure listener for Kafka tests
 * Captures test execution details, categorizes failures, and collects metrics
 */
@Slf4j
public class AllureKafkaListener implements TestWatcher {

    private static final DateTimeFormatter FORMATTER = DateTimeFormatter.ofPattern("yyyy-MM-dd HH:mm:ss");

    @Override
    public void testSuccessful(ExtensionContext context) {
        String testName = context.getDisplayName();
        String className = context.getTestClass().map(Class::getSimpleName).orElse("Unknown");

        log.info("✓ Test PASSED: {}.{}", className, testName);

        attachTestInfo(context, Status.PASSED);
        TestMetricsCollector.recordTestResult(true);
    }

    @Override
    public void testFailed(ExtensionContext context, Throwable cause) {
        String testName = context.getDisplayName();
        String className = context.getTestClass().map(Class::getSimpleName).orElse("Unknown");

        log.error("✗ Test FAILED: {}.{}", className, testName);
        log.error("Failure reason: {}", cause.getMessage(), cause);

        // Categorize failure
        String category = categorizeFailure(cause);
        TestMetricsCollector.recordError(category);
        TestMetricsCollector.recordCategory(category);
        TestMetricsCollector.recordTestResult(false);

        attachTestInfo(context, Status.FAILED);
        attachFailureDetails(cause);
        attachErrorCategory(category);
    }

    @Override
    public void testAborted(ExtensionContext context, Throwable cause) {
        String testName = context.getDisplayName();
        String className = context.getTestClass().map(Class::getSimpleName).orElse("Unknown");

        log.warn("⊘ Test ABORTED: {}.{}", className, testName);

        String category = categorizeFailure(cause);
        TestMetricsCollector.recordError(category);
        TestMetricsCollector.recordTestResult(false);

        attachTestInfo(context, Status.BROKEN);
        attachFailureDetails(cause);
        attachErrorCategory(category);
    }

    @Override
    public void testDisabled(ExtensionContext context, Optional<String> reason) {
        String testName = context.getDisplayName();
        String className = context.getTestClass().map(Class::getSimpleName).orElse("Unknown");

        log.info("⊗ Test DISABLED: {}.{}", className, testName);
        reason.ifPresent(r -> log.info("Reason: {}", r));
    }

    /**
     * Categorize failure based on exception type and message
     */
    private String categorizeFailure(Throwable cause) {
        if (cause == null) {
            return "UNKNOWN";
        }

        // Check if it's our custom exception with category
        if (cause instanceof KafkaTestException) {
            KafkaTestException kte = (KafkaTestException) cause;
            return kte.getErrorCategory();
        }

        // Check root cause
        Throwable rootCause = getRootCause(cause);
        String exceptionName = rootCause.getClass().getSimpleName();
        String message = rootCause.getMessage() != null ? rootCause.getMessage().toLowerCase() : "";

        // Categorize based on exception type and message
        if (exceptionName.contains("Timeout") || message.contains("timeout") || message.contains("timed out")) {
            return "INFRASTRUCTURE_TIMEOUT";
        }

        if (exceptionName.contains("Connection") || message.contains("connection refused")) {
            return "INFRASTRUCTURE_CONNECTION";
        }

        if (exceptionName.contains("SSL") || message.contains("ssl") || message.contains("certificate")) {
            return "INFRASTRUCTURE_SSL";
        }

        if (message.contains("rebalance")) {
            return "KAFKA_REBALANCE";
        }

        if (exceptionName.contains("AssertionError") || exceptionName.contains("Assertion")) {
            return "TEST_ASSERTION_FAILURE";
        }

        if (message.contains("thread") || message.contains("threadlocal")) {
            return "TEST_THREAD_SYNC";
        }

        if (message.contains("serialization")) {
            return "KAFKA_SERIALIZATION";
        }

        return "UNKNOWN_" + exceptionName;
    }

    /**
     * Get root cause of exception
     */
    private Throwable getRootCause(Throwable throwable) {
        Throwable cause = throwable;
        while (cause.getCause() != null && cause.getCause() != cause) {
            cause = cause.getCause();
        }
        return cause;
    }

    /**
     * Attaches test execution information to Allure report
     */
    private void attachTestInfo(ExtensionContext context, Status status) {
        StringBuilder info = new StringBuilder();

        info.append("=== Test Execution Info ===\n\n");
        info.append("Test Class: ").append(context.getTestClass().map(Class::getName).orElse("Unknown")).append("\n");
        info.append("Test Method: ").append(context.getTestMethod().map(m -> m.getName()).orElse("Unknown")).append("\n");
        info.append("Display Name: ").append(context.getDisplayName()).append("\n");
        info.append("Status: ").append(status).append("\n");
        info.append("Execution Time: ").append(LocalDateTime.now().format(FORMATTER)).append("\n");
        info.append("Thread: ").append(Thread.currentThread().getName()).append("\n");

        context.getTags().forEach(tag -> info.append("Tag: ").append(tag).append("\n"));

        Allure.addAttachment("Test Info", "text/plain",
                new ByteArrayInputStream(info.toString().getBytes(StandardCharsets.UTF_8)), "txt");
    }

    /**
     * Attaches failure details to Allure report
     */
    private void attachFailureDetails(Throwable cause) {
        if (cause == null) {
            return;
        }

        StringBuilder details = new StringBuilder();
        details.append("=== Failure Details ===\n\n");
        details.append("Exception Type: ").append(cause.getClass().getName()).append("\n");
        details.append("Message: ").append(cause.getMessage()).append("\n\n");

        // Add custom exception context if available
        if (cause instanceof KafkaTestException) {
            KafkaTestException kte = (KafkaTestException) cause;
            details.append("Error Category: ").append(kte.getErrorCategory()).append("\n");
            details.append("Error Type: ").append(kte.getErrorType()).append("\n");
            if (!kte.getContext().isEmpty()) {
                details.append("\nContext:\n");
                kte.getContext().forEach((key, value) ->
                        details.append("  ").append(key).append(": ").append(value).append("\n")
                );
            }
            details.append("\n");
        }

        details.append("Stack Trace:\n");

        for (StackTraceElement element : cause.getStackTrace()) {
            details.append("  at ").append(element.toString()).append("\n");
        }

        // Add cause if present
        Throwable rootCause = cause.getCause();
        if (rootCause != null && rootCause != cause) {
            details.append("\nCaused by: ").append(rootCause.getClass().getName()).append("\n");
            details.append("Message: ").append(rootCause.getMessage()).append("\n");
        }

        Allure.addAttachment("Failure Details", "text/plain",
                new ByteArrayInputStream(details.toString().getBytes(StandardCharsets.UTF_8)), "txt");
    }

    /**
     * Attach error category as Allure label
     */
    private void attachErrorCategory(String category) {
        Allure.label("error_category", category);
        Allure.parameter("Error Category", category);

        log.debug("Error categorized as: {}", category);
    }
}
