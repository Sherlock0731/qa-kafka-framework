package qa.autotest.framework.metrics;

import lombok.extern.slf4j.Slf4j;
import org.junit.jupiter.api.extension.*;

/**
 * JUnit 5 Extension: TestMetricsExtension
 *
 * <h3>Responsibility</h3>
 * Creates one {@link TestMetricsCollector} per test class, stores it in
 * {@link ExtensionContext.Store} with class-level scope, and attaches the
 * final metrics snapshot to the Allure report after the suite completes.
 *
 * <h3>Why ExtensionContext.Store</h3>
 * JUnit 5's {@code Store} is the standard DI mechanism for extensions.
 * Using {@code Namespace.create(TestMetricsExtension.class, testClass)}
 * guarantees:
 * <ul>
 *   <li>One collector per test class — no cross-suite mixing.</li>
 *   <li>Thread-safe access — JUnit's Store implementation is concurrent.</li>
 *   <li>Automatic cleanup — Store entries are closed at scope boundary.</li>
 * </ul>
 *
 * <h3>How to register</h3>
 * Add to {@code BaseTest} (replaces the old static {@code TestMetricsCollector} calls):
 * <pre>{@code
 * @ExtendWith(TestMetricsExtension.class)
 * public abstract class BaseTest { ... }
 * }</pre>
 *
 * <h3>How to read from another extension (e.g. AllureKafkaListener)</h3>
 * <pre>{@code
 * TestMetricsCollector collector = TestMetricsExtension.getCollector(context);
 * collector.recordTestResult(true);
 * }</pre>
 *
 * <h3>Race-condition fix</h3>
 * The old static singleton called {@code ConcurrentHashMap.clear()} inside
 * {@code reset()} — a non-atomic operation that dropped increments written
 * concurrently by other threads.  With per-class instances there is no
 * {@code reset()} at all: each suite starts with a fresh, zeroed instance.
 */
@Slf4j
public class TestMetricsExtension
        implements BeforeAllCallback, AfterAllCallback {

    /**
     * Namespace key — isolates this extension's Store entries from
     * other extensions using the same context.
     */
    private static final ExtensionContext.Namespace NAMESPACE =
            ExtensionContext.Namespace.create(TestMetricsExtension.class);

    /**
     * Key under which the collector is stored in the Store.
     */
    private static final String COLLECTOR_KEY = "metricsCollector";

    /**
     * Creates a fresh {@link TestMetricsCollector} for this test class and
     * puts it into the class-level Store before any test method runs.
     */
    @Override
    public void beforeAll(ExtensionContext context) {
        String suiteName = context.getTestClass()
                .map(Class::getSimpleName)
                .orElse("UnknownSuite");

        TestMetricsCollector collector = new TestMetricsCollector(suiteName);
        getStore(context).put(COLLECTOR_KEY, collector);

        log.debug("TestMetricsCollector registered for suite: {}", suiteName);
    }

    /**
     * After all tests in the class have finished, logs a summary and attaches
     * the metrics JSON to the Allure report.
     */
    @Override
    public void afterAll(ExtensionContext context) {
        TestMetricsCollector collector = getCollector(context);
        if (collector != null) {
            collector.logMetrics();
            collector.attachMetricsToAllure();
        }
    }

    /**
     * Retrieves the {@link TestMetricsCollector} associated with the given
     * extension context (looks up the class-level Store).
     *
     * <p>Returns {@code null} if the extension was not registered for this
     * context — callers should guard with a null-check.
     *
     * @param context the current test's {@link ExtensionContext}
     * @return the suite's collector, or {@code null}
     */
    public static TestMetricsCollector getCollector(ExtensionContext context) {
        // Walk up to the class-level context where beforeAll stored the collector
        ExtensionContext target = classLevelContext(context);
        if (target == null) {
            return null;
        }
        return target.getStore(NAMESPACE).get(COLLECTOR_KEY, TestMetricsCollector.class);
    }

    private ExtensionContext.Store getStore(ExtensionContext context) {
        return context.getStore(NAMESPACE);
    }

    /**
     * Traverses parent contexts to find the class-level context.
     * Needed when {@code getCollector} is called from a method-level context
     * (e.g. inside {@code testFailed()}).
     */
    private static ExtensionContext classLevelContext(ExtensionContext context) {
        ExtensionContext current = context;
        while (current != null) {
            if (current.getTestClass().isPresent() &&
                    current.getTestMethod().isEmpty()) {
                return current;
            }
            current = current.getParent().orElse(null);
        }
        // Fallback: use the provided context directly (handles class-level calls)
        return context;
    }
}
