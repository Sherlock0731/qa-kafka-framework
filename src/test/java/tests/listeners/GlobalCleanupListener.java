package tests.listeners;

import lombok.extern.slf4j.Slf4j;
import org.junit.platform.launcher.TestExecutionListener;
import org.junit.platform.launcher.TestPlan;
import qa.autotest.framework.config.ConfigFactory;
import qa.autotest.framework.config.KafkaConfig;
import qa.autotest.framework.infrastructure.KafkaTopicCleanupManager;

/**
 * Global Test Suite Cleanup Listener
 * <p>
 * Executes before and after the full test suite to log diagnostics and delete
 * any remaining test topics.
 * <p>
 * Registered via the ServiceLoader mechanism:
 * {@code src/test/resources/META-INF/services/org.junit.platform.launcher.TestExecutionListener}
 *
 * <h3>DIP fix</h3>
 * Previously this class relied on {@code KafkaTopicCleanupManager}'s internal
 * {@code new AivenApiController(config)} call, which chained two concrete
 * dependencies.  Now {@link KafkaTopicCleanupManager#create(KafkaConfig)} acts
 * as the single composition root: this listener calls the factory and receives
 * a manager whose {@code CleanupPort} is already wired — with no knowledge of
 * {@code AivenApiController} here.
 */
@Slf4j
public class GlobalCleanupListener implements TestExecutionListener {

    private static final KafkaConfig CONFIG = ConfigFactory.getConfig();
    private static final KafkaTopicCleanupManager CLEANUP_MANAGER = KafkaTopicCleanupManager.create(CONFIG);

    // ── TestExecutionListener ─────────────────────────────────────────────

    /**
     * Called before any tests are executed.
     */
    @Override
    public void testPlanExecutionStarted(TestPlan testPlan) {
        log.info("╔════════════════════════════════════════════════════════════════════════════╗");
        log.info("║                    KAFKA TEST SUITE EXECUTION STARTED                      ║");
        log.info("╚════════════════════════════════════════════════════════════════════════════╝");
        log.info("Total tests discovered: {}", testPlan.countTestIdentifiers(t -> t.isTest()));
        log.info("Environment: {}", CONFIG.environment());
        log.info("Kafka SSL enabled");
        log.info("Global Aiven API cleanup: {}",
                CONFIG.cleanupViaAivenApiEnabled() ? "ENABLED" : "DISABLED (test.cleanup.aiven.api.enabled=false)");

        log.info("Aiven API Configuration:\n{}", CLEANUP_MANAGER.getConfigurationStatus());
    }

    /**
     * Called after all tests are executed.
     * Performs global cleanup of all test topics via the injected {@link qa.autotest.framework.domain.port.CleanupPort}.
     */
    @Override
    public void testPlanExecutionFinished(TestPlan testPlan) {
        log.info("╔════════════════════════════════════════════════════════════════════════════╗");
        log.info("║                    KAFKA TEST SUITE EXECUTION FINISHED                     ║");
        log.info("╚════════════════════════════════════════════════════════════════════════════╝");

        try {
            log.info("Starting global cleanup of test topics via Aiven API...");

            int deletedCount = CLEANUP_MANAGER.cleanupAllTestTopics();

            if (deletedCount > 0) {
                log.info("✓ Global cleanup completed successfully! Deleted {} test topics", deletedCount);
            } else {
                log.info("No test topics found for cleanup");
            }

        } catch (Exception e) {
            log.error("Error during global cleanup: {}", e.getMessage(), e);
        }

        log.info("╔════════════════════════════════════════════════════════════════════════════╗");
        log.info("║                         TEST SUITE COMPLETE                                ║");
        log.info("╚════════════════════════════════════════════════════════════════════════════╝");
    }
}
