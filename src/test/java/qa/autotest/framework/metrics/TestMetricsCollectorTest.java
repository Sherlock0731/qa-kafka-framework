package qa.autotest.framework.metrics;

import com.google.gson.Gson;
import org.junit.jupiter.api.*;

import java.util.Map;
import java.util.concurrent.*;

import static org.assertj.core.api.Assertions.*;

/**
 * Unit-тесты для TestMetricsCollector.
 * <p>
 * Особый акцент: конкурентная корректность.
 * Все счётчики — AtomicInteger / AtomicLong — тесты подтверждают
 * отсутствие lost-updates при параллельных инкрементах.
 * <p>
 * Покрываемые сценарии:
 * - recordTestResult: счётчики total / passed / failed
 * - recordError: накапливает ошибки по категории
 * - recordDuration: суммирует продолжительности по операции
 * - getErrorCount: возвращает 0 для неизвестной категории
 * - getAllMetrics: структура возвращаемого Map
 * - getMetricsSummary: валидный JSON
 * - конкурентность: 10 потоков × 100 инкрементов без потерь
 * - изоляция экземпляров: два коллектора не влияют друг на друга
 */
@Tag("unit")
@DisplayName("TestMetricsCollector")
class TestMetricsCollectorTest {

    private TestMetricsCollector metrics;

    @BeforeEach
    void setUp() {
        metrics = new TestMetricsCollector("UnitTestSuite");
    }

    // ═══════════════════════════════════════════════════════════════════════
    // recordTestResult
    // ═══════════════════════════════════════════════════════════════════════

    @Test
    @DisplayName("recordTestResult(true) инкрементирует total и passed")
    void shouldIncrementTotalAndPassedOnSuccess() {
        metrics.recordTestResult(true);
        metrics.recordTestResult(true);

        Map<String, Object> m = metrics.getAllMetrics();
        @SuppressWarnings("unchecked")
        Map<String, Integer> summary = (Map<String, Integer>) m.get("test_summary");

        assertThat(summary.get("total")).isEqualTo(2);
        assertThat(summary.get("passed")).isEqualTo(2);
        assertThat(summary.get("failed")).isEqualTo(0);
    }

    @Test
    @DisplayName("recordTestResult(false) инкрементирует total и failed")
    void shouldIncrementTotalAndFailedOnFailure() {
        metrics.recordTestResult(false);

        Map<String, Object> m = metrics.getAllMetrics();
        @SuppressWarnings("unchecked")
        Map<String, Integer> summary = (Map<String, Integer>) m.get("test_summary");

        assertThat(summary.get("total")).isEqualTo(1);
        assertThat(summary.get("passed")).isEqualTo(0);
        assertThat(summary.get("failed")).isEqualTo(1);
    }

    @Test
    @DisplayName("total = passed + failed после смешанных результатов")
    void shouldMaintainConsistentCounters() {
        metrics.recordTestResult(true);
        metrics.recordTestResult(true);
        metrics.recordTestResult(false);

        Map<String, Object> m = metrics.getAllMetrics();
        @SuppressWarnings("unchecked")
        Map<String, Integer> summary = (Map<String, Integer>) m.get("test_summary");

        assertThat(summary.get("total")).isEqualTo(3);
        assertThat(summary.get("passed") + summary.get("failed"))
                .isEqualTo(summary.get("total"));
    }

    // ═══════════════════════════════════════════════════════════════════════
    // recordError
    // ═══════════════════════════════════════════════════════════════════════

    @Test
    @DisplayName("recordError накапливает ошибки по категории")
    void shouldAccumulateErrorsByCategory() {
        metrics.recordError("TIMEOUT");
        metrics.recordError("TIMEOUT");
        metrics.recordError("AUTH");

        assertThat(metrics.getErrorCount("TIMEOUT")).isEqualTo(2);
        assertThat(metrics.getErrorCount("AUTH")).isEqualTo(1);
    }

    @Test
    @DisplayName("getErrorCount возвращает 0 для неизвестной категории")
    void shouldReturnZeroForUnknownCategory() {
        assertThat(metrics.getErrorCount("NEVER_SEEN")).isZero();
    }

    @Test
    @DisplayName("getAllMetrics содержит error_counts с корректными значениями")
    void shouldIncludeErrorCountsInAllMetrics() {
        metrics.recordError("NETWORK");
        metrics.recordError("NETWORK");
        metrics.recordError("TIMEOUT");

        Map<String, Object> m = metrics.getAllMetrics();
        @SuppressWarnings("unchecked")
        Map<String, Integer> errors = (Map<String, Integer>) m.get("error_counts");

        assertThat(errors.get("NETWORK")).isEqualTo(2);
        assertThat(errors.get("TIMEOUT")).isEqualTo(1);
    }

    // ═══════════════════════════════════════════════════════════════════════
    // recordDuration
    // ═══════════════════════════════════════════════════════════════════════

    @Test
    @DisplayName("recordDuration суммирует продолжительности для одной операции")
    void shouldAccumulateDurationForSameOperation() {
        metrics.recordDuration("publish", 100L);
        metrics.recordDuration("publish", 200L);
        metrics.recordDuration("publish", 50L);

        Map<String, Object> m = metrics.getAllMetrics();
        @SuppressWarnings("unchecked")
        Map<String, Long> durations = (Map<String, Long>) m.get("operation_durations_ms");

        assertThat(durations.get("publish")).isEqualTo(350L);
    }

    @Test
    @DisplayName("recordDuration разделяет накопление для разных операций")
    void shouldAccumulateSeparatelyForDifferentOperations() {
        metrics.recordDuration("publish", 100L);
        metrics.recordDuration("consume", 200L);

        Map<String, Object> m = metrics.getAllMetrics();
        @SuppressWarnings("unchecked")
        Map<String, Long> durations = (Map<String, Long>) m.get("operation_durations_ms");

        assertThat(durations.get("publish")).isEqualTo(100L);
        assertThat(durations.get("consume")).isEqualTo(200L);
    }

    // ═══════════════════════════════════════════════════════════════════════
    // getAllMetrics — структура
    // ═══════════════════════════════════════════════════════════════════════

    @Test
    @DisplayName("getAllMetrics содержит все обязательные ключи")
    void shouldContainAllRequiredKeys() {
        Map<String, Object> m = metrics.getAllMetrics();

        assertThat(m).containsKeys(
                "error_counts",
                "category_distribution",
                "operation_durations_ms",
                "test_summary",
                "suite"
        );
    }

    @Test
    @DisplayName("getAllMetrics содержит имя suite")
    void shouldContainSuiteName() {
        assertThat(metrics.getAllMetrics().get("suite")).isEqualTo("UnitTestSuite");
    }

    // ═══════════════════════════════════════════════════════════════════════
    // getMetricsSummary — валидный JSON
    // ═══════════════════════════════════════════════════════════════════════

    @Test
    @DisplayName("getMetricsSummary возвращает валидный JSON")
    void shouldReturnValidJson() {
        metrics.recordTestResult(true);
        metrics.recordError("TIMEOUT");

        String json = metrics.getMetricsSummary();

        assertThatNoException().isThrownBy(() -> new Gson().fromJson(json, Map.class));
    }

    @Test
    @DisplayName("getMetricsSummary содержит ключ test_summary в JSON")
    void shouldContainTestSummaryInJson() {
        metrics.recordTestResult(false);

        String json = metrics.getMetricsSummary();

        assertThat(json).contains("test_summary");
        assertThat(json).contains("\"total\"");
    }

    // ═══════════════════════════════════════════════════════════════════════
    // Конкурентность — ключевой раздел
    // ═══════════════════════════════════════════════════════════════════════

    @Test
    @DisplayName("10 потоков × 100 recordTestResult(true) — итог ровно 1000 без lost-updates")
    void shouldHandleConcurrentTestResultUpdatesWithoutLoss() throws InterruptedException {
        int threadCount = 10;
        int iterationsEach = 100;
        ExecutorService pool = Executors.newFixedThreadPool(threadCount);
        CountDownLatch latch = new CountDownLatch(threadCount);

        for (int t = 0; t < threadCount; t++) {
            pool.submit(() -> {
                try {
                    for (int i = 0; i < iterationsEach; i++) {
                        metrics.recordTestResult(true);
                    }
                } finally {
                    latch.countDown();
                }
            });
        }

        latch.await(10, TimeUnit.SECONDS);
        pool.shutdown();

        Map<String, Object> m = metrics.getAllMetrics();
        @SuppressWarnings("unchecked")
        Map<String, Integer> summary = (Map<String, Integer>) m.get("test_summary");

        assertThat(summary.get("total")).isEqualTo(threadCount * iterationsEach);
        assertThat(summary.get("passed")).isEqualTo(threadCount * iterationsEach);
    }

    @Test
    @DisplayName("10 потоков × 50 passed + 50 failed — суммы без lost-updates")
    void shouldHandleConcurrentMixedResultsWithoutLoss() throws InterruptedException {
        int threadCount = 10;
        int passEach = 50;
        int failEach = 50;
        ExecutorService pool = Executors.newFixedThreadPool(threadCount);
        CountDownLatch latch = new CountDownLatch(threadCount);

        for (int t = 0; t < threadCount; t++) {
            pool.submit(() -> {
                try {
                    for (int i = 0; i < passEach; i++) metrics.recordTestResult(true);
                    for (int i = 0; i < failEach; i++) metrics.recordTestResult(false);
                } finally {
                    latch.countDown();
                }
            });
        }

        latch.await(10, TimeUnit.SECONDS);
        pool.shutdown();

        Map<String, Object> m = metrics.getAllMetrics();
        @SuppressWarnings("unchecked")
        Map<String, Integer> summary = (Map<String, Integer>) m.get("test_summary");

        assertThat(summary.get("total")).isEqualTo(threadCount * (passEach + failEach));
        assertThat(summary.get("passed")).isEqualTo(threadCount * passEach);
        assertThat(summary.get("failed")).isEqualTo(threadCount * failEach);
    }

    @Test
    @DisplayName("10 потоков × 100 recordDuration — суммарная длительность без потерь")
    void shouldHandleConcurrentDurationRecordingWithoutLoss() throws InterruptedException {
        int threadCount = 10;
        int iterationsEach = 100;
        long durationPerCall = 5L;
        ExecutorService pool = Executors.newFixedThreadPool(threadCount);
        CountDownLatch latch = new CountDownLatch(threadCount);

        for (int t = 0; t < threadCount; t++) {
            pool.submit(() -> {
                try {
                    for (int i = 0; i < iterationsEach; i++) {
                        metrics.recordDuration("publish", durationPerCall);
                    }
                } finally {
                    latch.countDown();
                }
            });
        }

        latch.await(10, TimeUnit.SECONDS);
        pool.shutdown();

        Map<String, Object> m = metrics.getAllMetrics();
        @SuppressWarnings("unchecked")
        Map<String, Long> durations = (Map<String, Long>) m.get("operation_durations_ms");

        long expected = (long) threadCount * iterationsEach * durationPerCall;
        assertThat(durations.get("publish")).isEqualTo(expected);
    }

    // ═══════════════════════════════════════════════════════════════════════
    // Изоляция экземпляров
    // ═══════════════════════════════════════════════════════════════════════

    @Test
    @DisplayName("Два экземпляра коллектора не влияют друг на друга (нет static state)")
    void shouldBeIsolatedFromOtherInstances() {
        TestMetricsCollector collector1 = new TestMetricsCollector("Suite-A");
        TestMetricsCollector collector2 = new TestMetricsCollector("Suite-B");

        collector1.recordTestResult(true);
        collector1.recordTestResult(true);
        collector1.recordError("TIMEOUT");

        // collector2 должен оставаться чистым
        Map<String, Object> m2 = collector2.getAllMetrics();
        @SuppressWarnings("unchecked")
        Map<String, Integer> summary = (Map<String, Integer>) m2.get("test_summary");

        assertThat(summary.get("total")).isZero();
        assertThat(collector2.getErrorCount("TIMEOUT")).isZero();
    }

    @Test
    @DisplayName("suiteName доступно в метриках и не смешивается между экземплярами")
    void shouldStoreSuiteNamePerInstance() {
        TestMetricsCollector c1 = new TestMetricsCollector("ConsumerTests");
        TestMetricsCollector c2 = new TestMetricsCollector("ProducerTests");

        assertThat(c1.getAllMetrics().get("suite")).isEqualTo("ConsumerTests");
        assertThat(c2.getAllMetrics().get("suite")).isEqualTo("ProducerTests");
    }
}
