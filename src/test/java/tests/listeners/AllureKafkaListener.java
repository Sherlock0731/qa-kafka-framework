package tests.listeners;

import io.qameta.allure.Allure;
import io.qameta.allure.model.Status;
import lombok.extern.slf4j.Slf4j;
import org.junit.jupiter.api.extension.ExtensionContext;
import org.junit.jupiter.api.extension.TestWatcher;

import java.io.ByteArrayInputStream;
import java.nio.charset.StandardCharsets;
import java.time.LocalDateTime;
import java.time.format.DateTimeFormatter;
import java.util.Optional;

/**
 * Allure listener for Kafka tests
 * Captures test execution details and attaches them to Allure report
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
    }

    @Override
    public void testFailed(ExtensionContext context, Throwable cause) {
        String testName = context.getDisplayName();
        String className = context.getTestClass().map(Class::getSimpleName).orElse("Unknown");
        
        log.error("✗ Test FAILED: {}.{}", className, testName);
        log.error("Failure reason: {}", cause.getMessage(), cause);
        
        attachTestInfo(context, Status.FAILED);
        attachFailureDetails(cause);
    }

    @Override
    public void testAborted(ExtensionContext context, Throwable cause) {
        String testName = context.getDisplayName();
        String className = context.getTestClass().map(Class::getSimpleName).orElse("Unknown");
        
        log.warn("⊘ Test ABORTED: {}.{}", className, testName);
        
        attachTestInfo(context, Status.BROKEN);
        attachFailureDetails(cause);
    }

    @Override
    public void testDisabled(ExtensionContext context, Optional<String> reason) {
        String testName = context.getDisplayName();
        String className = context.getTestClass().map(Class::getSimpleName).orElse("Unknown");
        
        log.info("⊗ Test DISABLED: {}.{}", className, testName);
        reason.ifPresent(r -> log.info("Reason: {}", r));
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
}
