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
 * Per-suite metrics collector for Kafka test execution.
 *
 * <h3>Threading model</h3>
 * One instance is created per test class and stored in
 * {@link org.junit.jupiter.api.extension.ExtensionContext.Store} with
 * class-level scope by {@link TestMetricsExtension}.  Parallel tests within
 * the same class share one instance — all counters use atomic types so
 * concurrent increments are safe.  Tests from <em>different</em> classes each
 * get their own instance, so metrics never bleed across suites.
 *
 * <h3>Migration from static singleton</h3>
 * Previously all fields were {@code static}, which caused two problems in
 * parallel execution:
 * <ul>
 *   <li>Metrics from unrelated suites mixed in the same maps.</li>
 *   <li>{@code reset()} called {@code ConcurrentHashMap.clear()} while other
 *       threads were writing — a non-atomic sequence that lost increments.</li>
 * </ul>
 * Now every field is an <em>instance</em> field.  No {@code static} data,
 * no {@code reset()}, no race on clear.
 *
 * <h3>Obtaining an instance</h3>
 * <pre>{@code
 * // In an extension or listener:
 * TestMetricsCollector metrics = TestMetricsExtension.getCollector(context);
 * metrics.recordTestResult(true);
 * }</pre>
 */
@Slf4j
public class TestMetricsCollector {

    private final Map<String, AtomicInteger> errorCounts      = new ConcurrentHashMap<>();
    private final Map<String, AtomicInteger> categoryCounts   = new ConcurrentHashMap<>();
    private final Map<String, AtomicLong>   operationDurations = new ConcurrentHashMap<>();
    private final AtomicInteger totalTests  = new AtomicInteger(0);
    private final AtomicInteger passedTests = new AtomicInteger(0);
    private final AtomicInteger failedTests = new AtomicInteger(0);

    /** Suite name — used as the Allure attachment label. */
    private final String suiteName;

    private static final Gson GSON = new GsonBuilder().setPrettyPrinting().create();

    public TestMetricsCollector(String suiteName) {
        this.suiteName = suiteName;
        log.debug("TestMetricsCollector created for suite: {}", suiteName);
    }

    /** Records a failed test's error category. */
    public void recordError(String category) {
        errorCounts.computeIfAbsent(category, k -> new AtomicInteger()).incrementAndGet();
        log.debug("Error recorded [suite={}]: {}", suiteName, category);
    }

    /** Records the failure category (for distribution charts). */
    public void recordCategory(String category) {
        categoryCounts.computeIfAbsent(category, k -> new AtomicInteger()).incrementAndGet();
    }

    /** Accumulates wall-clock duration for a named operation. */
    public void recordDuration(String operation, long durationMs) {
        operationDurations.computeIfAbsent(operation, k -> new AtomicLong()).addAndGet(durationMs);
    }

    /** Increments the total/passed/failed counters. */
    public void recordTestResult(boolean passed) {
        totalTests.incrementAndGet();
        if (passed) {
            passedTests.incrementAndGet();
        } else {
            failedTests.incrementAndGet();
        }
    }

    /** Returns the error count for {@code category}, or 0 if never recorded. */
    public int getErrorCount(String category) {
        AtomicInteger count = errorCounts.get(category);
        return count != null ? count.get() : 0;
    }

    /** Snapshot of all metrics as a plain-object map (safe to serialize). */
    public Map<String, Object> getAllMetrics() {
        Map<String, Object> metrics = new ConcurrentHashMap<>();

        Map<String, Integer> errors = new ConcurrentHashMap<>();
        errorCounts.forEach((k, v) -> errors.put(k, v.get()));
        metrics.put("error_counts", errors);

        Map<String, Integer> categories = new ConcurrentHashMap<>();
        categoryCounts.forEach((k, v) -> categories.put(k, v.get()));
        metrics.put("category_distribution", categories);

        Map<String, Long> durations = new ConcurrentHashMap<>();
        operationDurations.forEach((k, v) -> durations.put(k, v.get()));
        metrics.put("operation_durations_ms", durations);

        Map<String, Integer> summary = new ConcurrentHashMap<>();
        summary.put("total",  totalTests.get());
        summary.put("passed", passedTests.get());
        summary.put("failed", failedTests.get());
        metrics.put("test_summary", summary);

        metrics.put("suite", suiteName);

        return metrics;
    }

    /** Serialises all metrics to a pretty-printed JSON string. */
    public String getMetricsSummary() {
        return GSON.toJson(getAllMetrics());
    }

    /**
     * Attaches this suite's metrics snapshot to the current Allure report.
     * Called automatically by {@link TestMetricsExtension#afterAll} at
     * end of each test class.
     */
    public void attachMetricsToAllure() {
        String json = getMetricsSummary();
        Allure.addAttachment(
                "Test Execution Metrics — " + suiteName,
                "application/json",
                new ByteArrayInputStream(json.getBytes(StandardCharsets.UTF_8)),
                ".json"
        );
        log.info("Metrics attached to Allure for suite: {}", suiteName);
    }

    /** Logs a human-readable summary to the test output. */
    public void logMetrics() {
        log.info("=== Metrics [{}] ===", suiteName);
        log.info("Total: {}, Passed: {}, Failed: {}",
                totalTests.get(), passedTests.get(), failedTests.get());
        if (!errorCounts.isEmpty()) {
            errorCounts.forEach((cat, cnt) ->
                    log.info("  error/{}: {}", cat, cnt.get()));
        }
        if (!operationDurations.isEmpty()) {
            operationDurations.forEach((op, ms) ->
                    log.info("  duration/{}: {} ms", op, ms.get()));
        }
    }
}
