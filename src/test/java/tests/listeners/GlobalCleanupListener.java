package tests.listeners;

import lombok.extern.slf4j.Slf4j;
import org.junit.platform.launcher.TestExecutionListener;
import org.junit.platform.launcher.TestPlan;
import qa.autotest.framework.config.ConfigFactory;
import qa.autotest.framework.config.KafkaConfig;
import qa.autotest.framework.kafka.KafkaTopicCleanupManager;

/**
 * Global Test Suite Cleanup Listener
 * Executes after all tests are completed to cleanup remaining test topics
 * 
 * This listener is registered via ServiceLoader mechanism:
 * - Create file: src/test/resources/META-INF/services/org.junit.platform.launcher.TestExecutionListener
 * - Add this class name to the file
 */
@Slf4j
public class GlobalCleanupListener implements TestExecutionListener {
    
    private static final KafkaConfig CONFIG = ConfigFactory.getConfig();
    private static final KafkaTopicCleanupManager CLEANUP_MANAGER = new KafkaTopicCleanupManager(CONFIG);
    
    /**
     * Called before any tests are executed
     */
    @Override
    public void testPlanExecutionStarted(TestPlan testPlan) {
        log.info("╔════════════════════════════════════════════════════════════════════════════╗");
        log.info("║                    KAFKA TEST SUITE EXECUTION STARTED                      ║");
        log.info("╚════════════════════════════════════════════════════════════════════════════╝");
        log.info("Total tests discovered: {}", testPlan.countTestIdentifiers(t -> t.isTest()));
        log.info("Environment: {}", CONFIG.environment());
        log.info("Kafka SSL enabled");
        
        // Log Aiven API configuration status
        String configStatus = CLEANUP_MANAGER.getConfigurationStatus();
        log.info("Aiven API Configuration:\n{}", configStatus);
    }
    
    /**
     * Called after all tests are executed
     * This is where we perform global cleanup of all test topics
     */
    @Override
    public void testPlanExecutionFinished(TestPlan testPlan) {
        log.info("╔════════════════════════════════════════════════════════════════════════════╗");
        log.info("║                    KAFKA TEST SUITE EXECUTION FINISHED                     ║");
        log.info("╚════════════════════════════════════════════════════════════════════════════╝");
        
        try {
            log.info("Starting global cleanup of test topics via Aiven API...");
            
            // Cleanup all test topics using Aiven API
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
