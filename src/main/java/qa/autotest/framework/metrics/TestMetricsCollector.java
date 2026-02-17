package qa.autotest.framework.metrics;

import com.google.gson.Gson;
import com.google.gson.GsonBuilder;
import io.qameta.allure.Allure;
import lombok.extern.slf4j.Slf4j;

import java.io.ByteArrayInputStream;
import java.nio.charset.StandardCharsets;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicLong;

/**
 * Collects and tracks test execution metrics for analysis
 * Integrates with Allure for reporting
 */
@Slf4j
public class TestMetricsCollector {

    private static final Map<String, AtomicInteger> errorCounts = new ConcurrentHashMap<>();
    private static final Map<String, AtomicInteger> categoryCounts = new ConcurrentHashMap<>();
    private static final Map<String, AtomicLong> operationDurations = new ConcurrentHashMap<>();
    private static final AtomicInteger totalTests = new AtomicInteger(0);
    private static final AtomicInteger passedTests = new AtomicInteger(0);
    private static final AtomicInteger failedTests = new AtomicInteger(0);

    private static final Gson gson = new GsonBuilder().setPrettyPrinting().create();

    /**
     * Record an error occurrence by category
     */
    public static void recordError(String category) {
        errorCounts.computeIfAbsent(category, k -> new AtomicInteger()).incrementAndGet();
        log.debug("Error recorded: {}", category);
    }

    /**
     * Record a test categorization
     */
    public static void recordCategory(String category) {
        categoryCounts.computeIfAbsent(category, k -> new AtomicInteger()).incrementAndGet();
    }

    /**
     * Record operation duration in milliseconds
     */
    public static void recordDuration(String operation, long durationMs) {
        operationDurations.computeIfAbsent(operation, k -> new AtomicLong()).addAndGet(durationMs);
    }

    /**
     * Record test result
     */
    public static void recordTestResult(boolean passed) {
        totalTests.incrementAndGet();
        if (passed) {
            passedTests.incrementAndGet();
        } else {
            failedTests.incrementAndGet();
        }
    }

    /**
     * Get error count for a specific category
     */
    public static int getErrorCount(String category) {
        AtomicInteger count = errorCounts.get(category);
        return count != null ? count.get() : 0;
    }

    /**
     * Get all metrics as a map
     */
    public static Map<String, Object> getAllMetrics() {
        Map<String, Object> metrics = new ConcurrentHashMap<>();

        // Error counts
        Map<String, Integer> errors = new ConcurrentHashMap<>();
        errorCounts.forEach((key, value) -> errors.put(key, value.get()));
        metrics.put("error_counts", errors);

        // Category distribution
        Map<String, Integer> categories = new ConcurrentHashMap<>();
        categoryCounts.forEach((key, value) -> categories.put(key, value.get()));
        metrics.put("category_distribution", categories);

        // Operation durations
        Map<String, Long> durations = new ConcurrentHashMap<>();
        operationDurations.forEach((key, value) -> durations.put(key, value.get()));
        metrics.put("operation_durations_ms", durations);

        // Test summary
        Map<String, Integer> summary = new ConcurrentHashMap<>();
        summary.put("total", totalTests.get());
        summary.put("passed", passedTests.get());
        summary.put("failed", failedTests.get());
        metrics.put("test_summary", summary);

        return metrics;
    }

    /**
     * Attach metrics to Allure report
     */
    public static void attachMetricsToAllure() {
        Map<String, Object> metrics = getAllMetrics();
        String jsonMetrics = gson.toJson(metrics);

        Allure.addAttachment(
                "Test Execution Metrics",
                "application/json",
                new ByteArrayInputStream(jsonMetrics.getBytes(StandardCharsets.UTF_8)),
                ".json"
        );

        log.info("Metrics attached to Allure report");
    }

    /**
     * Get metrics summary as formatted string
     */
    public static String getMetricsSummary() {
        Map<String, Object> metrics = getAllMetrics();
        return gson.toJson(metrics);
    }

    /**
     * Reset all metrics (use for test isolation if needed)
     */
    public static void reset() {
        errorCounts.clear();
        categoryCounts.clear();
        operationDurations.clear();
        totalTests.set(0);
        passedTests.set(0);
        failedTests.set(0);
        log.info("Metrics reset");
    }

    /**
     * Log current metrics
     */
    public static void logMetrics() {
        log.info("=== Test Metrics Summary ===");
        log.info("Total Tests: {}", totalTests.get());
        log.info("Passed: {}, Failed: {}", passedTests.get(), failedTests.get());

        if (!errorCounts.isEmpty()) {
            log.info("Error Distribution:");
            errorCounts.forEach((category, count) ->
                    log.info("  {}: {}", category, count.get())
            );
        }

        if (!operationDurations.isEmpty()) {
            log.info("Operation Durations:");
            operationDurations.forEach((operation, duration) ->
                    log.info("  {}: {} ms", operation, duration.get())
            );
        }
    }
}
