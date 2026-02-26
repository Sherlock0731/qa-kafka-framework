package tests.listeners;

import io.qameta.allure.Allure;
import io.qameta.allure.model.Status;
import lombok.extern.slf4j.Slf4j;
import org.junit.jupiter.api.extension.ExtensionContext;
import org.junit.jupiter.api.extension.TestWatcher;
import qa.autotest.framework.domain.exceptions.KafkaTestException;
import qa.autotest.framework.metrics.TestMetricsCollector;
import qa.autotest.framework.metrics.TestMetricsExtension;

import java.io.ByteArrayInputStream;
import java.nio.charset.StandardCharsets;
import java.time.LocalDateTime;
import java.time.format.DateTimeFormatter;
import java.util.Optional;

/**
 * Allure listener for Kafka tests.
 *
 * <h3>Metrics integration</h3>
 * Obtains the per-suite {@link TestMetricsCollector} from
 * {@link TestMetricsExtension#getCollector(ExtensionContext)} instead of
 * calling static methods on a global singleton.  This eliminates cross-suite
 * metric contamination and the race condition on {@code reset()} that existed
 * with the old static implementation.
 *
 * <p>If {@code TestMetricsExtension} is not registered (e.g. in a standalone
 * test that doesn't extend {@code BaseTest}), {@code getCollector()} returns
 * {@code null} and all metric calls are safely skipped.
 */
@Slf4j
public class AllureKafkaListener implements TestWatcher {

    private static final DateTimeFormatter FORMATTER =
            DateTimeFormatter.ofPattern("yyyy-MM-dd HH:mm:ss");

    // ── TestWatcher callbacks ─────────────────────────────────────────────────

    @Override
    public void testSuccessful(ExtensionContext context) {
        String testName  = context.getDisplayName();
        String className = context.getTestClass().map(Class::getSimpleName).orElse("Unknown");
        log.info("✓ Test PASSED: {}.{}", className, testName);

        attachTestInfo(context, Status.PASSED);

        TestMetricsCollector collector = TestMetricsExtension.getCollector(context);
        if (collector != null) {
            collector.recordTestResult(true);
        }
    }

    @Override
    public void testFailed(ExtensionContext context, Throwable cause) {
        String testName  = context.getDisplayName();
        String className = context.getTestClass().map(Class::getSimpleName).orElse("Unknown");
        log.error("✗ Test FAILED: {}.{}", className, testName);
        log.error("Failure reason: {}", cause.getMessage(), cause);

        String category = categorizeFailure(cause);

        attachTestInfo(context, Status.FAILED);
        attachFailureDetails(cause);
        attachErrorCategory(category);

        TestMetricsCollector collector = TestMetricsExtension.getCollector(context);
        if (collector != null) {
            collector.recordError(category);
            collector.recordCategory(category);
            collector.recordTestResult(false);
        }
    }

    @Override
    public void testAborted(ExtensionContext context, Throwable cause) {
        String testName  = context.getDisplayName();
        String className = context.getTestClass().map(Class::getSimpleName).orElse("Unknown");
        log.warn("⊘ Test ABORTED: {}.{}", className, testName);

        String category = categorizeFailure(cause);

        attachTestInfo(context, Status.BROKEN);
        attachFailureDetails(cause);
        attachErrorCategory(category);

        TestMetricsCollector collector = TestMetricsExtension.getCollector(context);
        if (collector != null) {
            collector.recordError(category);
            collector.recordTestResult(false);
        }
    }

    @Override
    public void testDisabled(ExtensionContext context, Optional<String> reason) {
        String testName  = context.getDisplayName();
        String className = context.getTestClass().map(Class::getSimpleName).orElse("Unknown");
        log.info("⊗ Test DISABLED: {}.{}", className, testName);
        reason.ifPresent(r -> log.info("Reason: {}", r));
    }

    // ── Failure categorization ────────────────────────────────────────────────

    /**
     * Categorizes a failure by walking the exception type hierarchy.
     * Uses {@code instanceof} checks on exception types rather than fragile
     * string matching on lowercased messages.
     */
    private String categorizeFailure(Throwable cause) {
        if (cause == null) {
            return "UNKNOWN";
        }

        // Domain exception carries its own category
        if (cause instanceof KafkaTestException kte) {
            return kte.getErrorCategory();
        }

        Throwable root = rootCause(cause);

        // Walk the exception type hierarchy — more reliable than string matching
        if (isKafkaTimeoutException(root)) return "INFRASTRUCTURE_TIMEOUT";
        if (isConnectionException(root))  return "INFRASTRUCTURE_CONNECTION";
        if (isSslException(root))         return "INFRASTRUCTURE_SSL";
        if (isRebalanceException(root))   return "KAFKA_REBALANCE";
        if (isAssertionException(root))   return "TEST_ASSERTION_FAILURE";
        if (isSerializationException(root)) return "KAFKA_SERIALIZATION";
        if (isInterruptedException(root)) return "TEST_INTERRUPTED";

        return "UNKNOWN_" + root.getClass().getSimpleName();
    }

    private boolean isKafkaTimeoutException(Throwable t) {
        return t instanceof org.apache.kafka.common.errors.TimeoutException
                || t instanceof java.util.concurrent.TimeoutException
                || t.getClass().getSimpleName().contains("Timeout");
    }

    private boolean isConnectionException(Throwable t) {
        return t instanceof java.net.ConnectException
                || t instanceof org.apache.kafka.common.errors.NetworkException
                || t.getClass().getSimpleName().contains("Connection");
    }

    private boolean isSslException(Throwable t) {
        return t instanceof javax.net.ssl.SSLException
                || t.getClass().getName().contains("ssl")
                || t.getClass().getName().contains("SSL");
    }

    private boolean isRebalanceException(Throwable t) {
        return t instanceof org.apache.kafka.common.errors.RebalanceInProgressException
                || t instanceof org.apache.kafka.clients.consumer.CommitFailedException;
    }

    private boolean isAssertionException(Throwable t) {
        return t instanceof AssertionError
                || t.getClass().getSimpleName().contains("Assertion");
    }

    private boolean isSerializationException(Throwable t) {
        return t instanceof org.apache.kafka.common.errors.SerializationException;
    }

    private boolean isInterruptedException(Throwable t) {
        return t instanceof InterruptedException;
    }

    private Throwable rootCause(Throwable t) {
        Throwable cause = t;
        while (cause.getCause() != null && cause.getCause() != cause) {
            cause = cause.getCause();
        }
        return cause;
    }

    // ── Allure attachments ────────────────────────────────────────────────────

    private void attachTestInfo(ExtensionContext context, Status status) {
        StringBuilder info = new StringBuilder();
        info.append("=== Test Execution Info ===\n\n");
        info.append("Test Class:   ").append(context.getTestClass().map(Class::getName).orElse("Unknown")).append("\n");
        info.append("Test Method:  ").append(context.getTestMethod().map(m -> m.getName()).orElse("Unknown")).append("\n");
        info.append("Display Name: ").append(context.getDisplayName()).append("\n");
        info.append("Status:       ").append(status).append("\n");
        info.append("Time:         ").append(LocalDateTime.now().format(FORMATTER)).append("\n");
        info.append("Thread:       ").append(Thread.currentThread().getName()).append("\n");
        context.getTags().forEach(tag -> info.append("Tag: ").append(tag).append("\n"));

        Allure.addAttachment("Test Info", "text/plain",
                new ByteArrayInputStream(info.toString().getBytes(StandardCharsets.UTF_8)), "txt");
    }

    private void attachFailureDetails(Throwable cause) {
        if (cause == null) return;

        StringBuilder details = new StringBuilder();
        details.append("=== Failure Details ===\n\n");
        details.append("Exception Type: ").append(cause.getClass().getName()).append("\n");
        details.append("Message: ").append(cause.getMessage()).append("\n\n");

        if (cause instanceof KafkaTestException kte) {
            details.append("Error Category: ").append(kte.getErrorCategory()).append("\n");
            details.append("Error Type: ").append(kte.getErrorType()).append("\n");
            if (!kte.getContext().isEmpty()) {
                details.append("\nContext:\n");
                kte.getContext().forEach((k, v) ->
                        details.append("  ").append(k).append(": ").append(v).append("\n"));
            }
            details.append("\n");
        }

        details.append("Stack Trace:\n");
        for (StackTraceElement element : cause.getStackTrace()) {
            details.append("  at ").append(element).append("\n");
        }

        Throwable rootCause = cause.getCause();
        if (rootCause != null && rootCause != cause) {
            details.append("\nCaused by: ").append(rootCause.getClass().getName()).append("\n");
            details.append("Message: ").append(rootCause.getMessage()).append("\n");
        }

        Allure.addAttachment("Failure Details", "text/plain",
                new ByteArrayInputStream(details.toString().getBytes(StandardCharsets.UTF_8)), "txt");
    }

    private void attachErrorCategory(String category) {
        Allure.label("error_category", category);
        Allure.parameter("Error Category", category);
        log.debug("Error categorized as: {}", category);
    }
}
