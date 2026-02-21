package qa.autotest.framework.infrastructure;

import lombok.extern.slf4j.Slf4j;
import qa.autotest.framework.infrastructure.api.aiven.AivenApiController;
import qa.autotest.framework.config.KafkaConfig;

import java.util.List;

/**
 * Kafka Topic Cleanup Manager
 * Handles cleanup of test topics after test execution
 * <p>
 * Uses Aiven API to delete topics created during test runs
 */
@Slf4j
public class KafkaTopicCleanupManager {

    private final KafkaConfig config;
    private final AivenApiController aivenApiController;

    public KafkaTopicCleanupManager(KafkaConfig config) {
        this.config = config;
        this.aivenApiController = new AivenApiController(config);
    }

    /**
     * Cleanup all test topics created during test execution
     * This method should be called after all tests are completed
     *
     * @return Number of successfully deleted topics
     */
    public int cleanupAllTestTopics() {
        if (!config.cleanupTopics()) {
            log.info("Topic cleanup is disabled in configuration (test.cleanup.topics=false)");
            return 0;
        }

        if (!isAivenApiConfigured()) {
            log.warn("Aiven API is not configured. Skipping topic cleanup. " +
                    "Please configure: aiven.api.token, aiven.project.name, aiven.service.name");
            return 0;
        }

        log.info("Starting automated cleanup of test topics via Aiven API");

        try {
            // Verify API connection first
            if (!aivenApiController.verifyApiConnection()) {
                log.error("Failed to connect to Aiven API. Skipping cleanup.");
                return 0;
            }

            // Get all topics and delete test topics
            int deletedCount = aivenApiController.deleteAllTestTopics();

            if (deletedCount > 0) {
                log.info("Successfully cleaned up {} test topics", deletedCount);
            } else {
                log.info("No test topics found to cleanup");
            }

            return deletedCount;

        } catch (Exception e) {
            log.error("Error during topic cleanup: {}", e.getMessage(), e);
            return 0;
        }
    }

    /**
     * Cleanup specific topics
     *
     * @param topicNames List of topic names to delete
     * @return Number of successfully deleted topics
     */
    public int cleanupSpecificTopics(List<String> topicNames) {
        if (!config.cleanupTopics()) {
            log.info("Topic cleanup is disabled in configuration");
            return 0;
        }

        if (!isAivenApiConfigured()) {
            log.warn("Aiven API is not configured. Skipping topic cleanup.");
            return 0;
        }

        log.info("Cleaning up {} specific topics", topicNames.size());
        return aivenApiController.deleteTopics(topicNames);
    }

    /**
     * Check if Aiven API is properly configured
     *
     * @return True if all required Aiven API properties are set
     */
    private boolean isAivenApiConfigured() {
        return config.aivenApiToken() != null && !config.aivenApiToken().isEmpty()
                && config.aivenProjectName() != null && !config.aivenProjectName().isEmpty()
                && config.aivenServiceName() != null && !config.aivenServiceName().isEmpty();
    }

    /**
     * Get current Aiven API configuration status
     *
     * @return Configuration status message
     */
    public String getConfigurationStatus() {
        StringBuilder status = new StringBuilder();
        status.append("Aiven API Configuration Status:\n");
        status.append("- API URL: ").append(config.aivenApiUrl()).append("\n");
        status.append("- API Token: ").append(config.aivenApiToken() != null && !config.aivenApiToken().isEmpty()
                ? "Configured" : "NOT CONFIGURED").append("\n");
        status.append("- Project Name: ").append(config.aivenProjectName() != null && !config.aivenProjectName().isEmpty()
                ? config.aivenProjectName() : "NOT CONFIGURED").append("\n");
        status.append("- Service Name: ").append(config.aivenServiceName() != null && !config.aivenServiceName().isEmpty()
                ? config.aivenServiceName() : "NOT CONFIGURED").append("\n");
        status.append("- Cleanup Enabled: ").append(config.cleanupTopics()).append("\n");

        return status.toString();
    }
}
