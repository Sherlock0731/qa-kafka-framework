package qa.autotest.framework.infrastructure;

import lombok.extern.slf4j.Slf4j;
import qa.autotest.framework.config.KafkaConfig;
import qa.autotest.framework.domain.port.CleanupPort;
import qa.autotest.framework.infrastructure.api.aiven.AivenApiController;

import java.util.List;
import java.util.stream.Collectors;

/**
 * Kafka Topic Cleanup Manager
 * <p>
 * Orchestrates cleanup of test topics after suite execution.
 * <p>
 * <h3>DIP fix</h3>
 * Previously this class constructed {@code new AivenApiController(config)}
 * directly, coupling the orchestration logic to a specific REST-client
 * implementation.  It now receives a {@link CleanupPort} via constructor
 * injection, so the concrete adapter is chosen by the caller (the factory
 * method {@link #create(KafkaConfig)}) rather than hard-coded here.
 * <p>
 * This makes the manager fully unit-testable with a mock {@link CleanupPort}
 * — no HTTP calls required.
 *
 * <h3>Usage</h3>
 * <pre>{@code
 * // production wiring (GlobalCleanupListener):
 * KafkaTopicCleanupManager manager = KafkaTopicCleanupManager.create(config);
 *
 * // test wiring:
 * CleanupPort mockPort = mock(CleanupPort.class);
 * KafkaTopicCleanupManager manager = new KafkaTopicCleanupManager(config, mockPort);
 * }</pre>
 */
@Slf4j
public class KafkaTopicCleanupManager {

    private final KafkaConfig config;
    private final CleanupPort cleanupPort;

    /**
     * Primary constructor — accepts an externally provided {@link CleanupPort}.
     * Use this constructor in tests to inject a mock.
     *
     * @param config      validated {@link KafkaConfig}
     * @param cleanupPort outbound port for topic-deletion operations
     */
    public KafkaTopicCleanupManager(KafkaConfig config, CleanupPort cleanupPort) {
        this.config      = config;
        this.cleanupPort = cleanupPort;
    }

    /**
     * Production factory method — wires {@link AivenApiController} as the
     * {@link CleanupPort} implementation.
     * <p>
     * Keeps {@code GlobalCleanupListener} and other callers free from knowing
     * which concrete adapter is in use.
     *
     * @param config validated {@link KafkaConfig}
     * @return fully-wired {@code KafkaTopicCleanupManager}
     */
    public static KafkaTopicCleanupManager create(KafkaConfig config) {
        return new KafkaTopicCleanupManager(config, new AivenApiController(config));
    }

    /**
     * Cleans up all test topics created during test execution.
     * <p>
     * Guarded by two independent configuration flags:
     * <ol>
     *   <li>{@code test.cleanup.topics} — master switch for all cleanup</li>
     *   <li>{@code test.cleanup.aiven.api.enabled} — specifically controls
     *       whether the global Aiven REST API sweep runs at suite end</li>
     * </ol>
     * Both must be {@code true} for the {@link CleanupPort} call to proceed.
     *
     * @return number of successfully deleted topics
     */
    public int cleanupAllTestTopics() {
        if (!config.cleanupTopics()) {
            log.info("Topic cleanup is disabled (test.cleanup.topics=false). Skipping global Aiven API cleanup.");
            return 0;
        }

        if (!config.cleanupViaAivenApiEnabled()) {
            log.info("Global Aiven API cleanup is disabled (test.cleanup.aiven.api.enabled=false). Skipping.");
            return 0;
        }

        if (!isAivenApiConfigured()) {
            log.warn("Aiven API is not configured. Skipping topic cleanup. " +
                    "Please configure: aiven.api.token, aiven.project.name, aiven.service.name");
            return 0;
        }

        log.info("Starting automated cleanup of test topics via Aiven API");

        try {
            if (!cleanupPort.verifyConnection()) {
                log.error("Failed to connect to Aiven API. Skipping cleanup.");
                return 0;
            }

            List<String> allTopics   = cleanupPort.getTopicList();
            List<String> testTopics  = filterTestTopics(allTopics);

            if (testTopics.isEmpty()) {
                log.info("No test topics found to cleanup");
                return 0;
            }

            log.info("Found {} test topics to delete: {}", testTopics.size(), testTopics);
            int deletedCount = cleanupPort.deleteTopics(testTopics);
            log.info("Successfully cleaned up {} test topics", deletedCount);
            return deletedCount;

        } catch (Exception e) {
            log.error("Error during topic cleanup: {}", e.getMessage(), e);
            return 0;
        }
    }

    /**
     * Cleans up a specific list of topics.
     *
     * @param topicNames list of topic names to delete
     * @return number of successfully deleted topics
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
        return cleanupPort.deleteTopics(topicNames);
    }

    /**
     * Returns a human-readable summary of the current Aiven API configuration.
     * Used by {@code GlobalCleanupListener} for startup diagnostics.
     *
     * @return multi-line configuration status string
     */
    public String getConfigurationStatus() {
        return "Aiven API Configuration Status:\n" +
                "- API URL: "              + config.aivenApiUrl() + "\n" +
                "- API Token: "            + (isTokenSet()        ? "Configured"         : "NOT CONFIGURED") + "\n" +
                "- Project Name: "         + (isProjectSet()      ? config.aivenProjectName() : "NOT CONFIGURED") + "\n" +
                "- Service Name: "         + (isServiceSet()      ? config.aivenServiceName() : "NOT CONFIGURED") + "\n" +
                "- Cleanup Enabled: "      + config.cleanupTopics() + "\n" +
                "- Aiven API Cleanup Enabled: " + config.cleanupViaAivenApiEnabled() + "\n";
    }

    private List<String> filterTestTopics(List<String> allTopics) {
        String prefix = config.testTopicPrefix();
        return allTopics.stream()
                .filter(t -> t.startsWith(prefix))
                .collect(Collectors.toList());
    }

    private boolean isAivenApiConfigured() {
        return isTokenSet() && isProjectSet() && isServiceSet();
    }

    private boolean isTokenSet() {
        return config.aivenApiToken() != null && !config.aivenApiToken().isEmpty();
    }

    private boolean isProjectSet() {
        return config.aivenProjectName() != null && !config.aivenProjectName().isEmpty();
    }

    private boolean isServiceSet() {
        return config.aivenServiceName() != null && !config.aivenServiceName().isEmpty();
    }
}
