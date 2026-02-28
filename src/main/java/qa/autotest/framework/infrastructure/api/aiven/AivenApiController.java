package qa.autotest.framework.infrastructure.api.aiven;

import io.restassured.builder.RequestSpecBuilder;
import io.restassured.filter.log.LogDetail;
import io.restassured.filter.log.RequestLoggingFilter;
import io.restassured.filter.log.ResponseLoggingFilter;
import io.restassured.http.ContentType;
import io.restassured.response.Response;
import io.restassured.specification.RequestSpecification;
import lombok.extern.slf4j.Slf4j;
import qa.autotest.framework.config.KafkaConfig;
import qa.autotest.framework.domain.port.CleanupPort;
import qa.autotest.framework.infrastructure.api.aiven.dto.AivenTopicListResponseDto;

import java.util.List;
import java.util.stream.Collectors;

import static io.restassured.RestAssured.given;

/**
 * Infrastructure Adapter: AivenApiController
 * <p>
 * Implements {@link CleanupPort} — the outbound port for remote topic cleanup —
 * by communicating with the Aiven REST API.
 * <p>
 * <h3>DIP fix</h3>
 * Previously this class had no port abstraction: callers depended directly on
 * the concrete class.  It now implements {@link CleanupPort} so that
 * {@link qa.autotest.framework.infrastructure.KafkaTopicCleanupManager} and
 * any future caller can depend on the interface rather than the implementation.
 * <p>
 * <h3>Thread-safety note</h3>
 * The {@code RequestSpecification} is built once in the constructor and stored
 * in a final field.  The static field {@code RestAssured.baseURI} is
 * intentionally <em>not</em> mutated here: doing so would introduce a race
 * condition when multiple instances are constructed concurrently in parallel
 * test threads.
 * <p>
 * API Documentation: https://api.aiven.io/doc/
 */
@Slf4j
public class AivenApiController implements CleanupPort {

    private final KafkaConfig config;
    private final RequestSpecification requestSpec;

    public AivenApiController(KafkaConfig config) {
        this.config = config;
        this.requestSpec = createRequestSpecification();
    }

    private RequestSpecification createRequestSpecification() {
        return new RequestSpecBuilder()
                .setBaseUri(config.aivenApiUrl())
                .setContentType(ContentType.JSON)
                .addHeader("Authorization", "Bearer " + config.aivenApiToken())
                .addFilter(new RequestLoggingFilter(LogDetail.ALL))
                .addFilter(new ResponseLoggingFilter(LogDetail.ALL))
                .build();
    }

    /**
     * {@inheritDoc}
     * <p>
     * GET /v1/project/{project}/service/{service}/topic
     */
    @Override
    public List<String> getTopicList() {
        log.info("Getting topic list from Aiven API for project: {}, service: {}",
                config.aivenProjectName(), config.aivenServiceName());

        try {
            Response response = given()
                    .spec(requestSpec)
                    .when()
                    .get("/project/{project}/service/{service}/topic",
                            config.aivenProjectName(),
                            config.aivenServiceName())
                    .then()
                    .extract()
                    .response();

            if (response.getStatusCode() == 200) {
                AivenTopicListResponseDto body = response.as(AivenTopicListResponseDto.class);

                if (body.getTopics() != null) {
                    List<String> topicNames = body.getTopics().stream()
                            .map(AivenTopicListResponseDto.AivenTopicDto::getTopicName)
                            .collect(Collectors.toList());

                    log.info("Successfully retrieved {} topics from Aiven", topicNames.size());
                    return topicNames;
                }
            }

            log.warn("Failed to get topic list from Aiven. Status: {}, Response: {}",
                    response.getStatusCode(), response.asString());
            return List.of();

        } catch (Exception e) {
            log.error("Error getting topic list from Aiven: {}", e.getMessage(), e);
            return List.of();
        }
    }

    /**
     * {@inheritDoc}
     * <p>
     * DELETE /v1/project/{project}/service/{service}/topic/{topic_name}
     * <p>
     * HTTP 404 is treated as success — topic was already absent.
     */
    @Override
    public boolean deleteTopic(String topicName) {
        log.info("Deleting topic '{}' via Aiven API", topicName);

        try {
            Response response = given()
                    .spec(requestSpec)
                    .when()
                    .delete("/project/{project}/service/{service}/topic/{topic}",
                            config.aivenProjectName(),
                            config.aivenServiceName(),
                            topicName)
                    .then()
                    .extract()
                    .response();

            if (response.getStatusCode() == 200) {
                log.info("Successfully deleted topic '{}' via Aiven API", topicName);
                return true;
            }

            if (response.getStatusCode() == 404) {
                log.info("Topic '{}' not found in Aiven (already deleted or doesn't exist)", topicName);
                return true;
            }

            log.warn("Failed to delete topic '{}'. Status: {}, Response: {}",
                    topicName, response.getStatusCode(), response.asString());
            return false;

        } catch (Exception e) {
            log.error("Error deleting topic '{}' via Aiven API: {}", topicName, e.getMessage(), e);
            return false;
        }
    }

    /**
     * {@inheritDoc}
     * <p>
     * Iterates over {@code topicNames} and calls {@link #deleteTopic} for each.
     * Failures are logged but do not abort the remaining deletions.
     */
    @Override
    public int deleteTopics(List<String> topicNames) {
        log.info("Deleting {} topics via Aiven API", topicNames.size());

        int successCount = 0;
        for (String topicName : topicNames) {
            if (deleteTopic(topicName)) {
                successCount++;
            }
        }

        log.info("Successfully deleted {}/{} topics via Aiven API", successCount, topicNames.size());
        return successCount;
    }

    /**
     * {@inheritDoc}
     * <p>
     * Implemented by calling {@link #getTopicList()} — a successful (non-throwing)
     * response from the broker confirms reachability and valid credentials.
     */
    @Override
    public boolean verifyConnection() {
        try {
            List<String> topics = getTopicList();
            log.info("Aiven API connection verified successfully. Found {} topics", topics.size());
            return true;
        } catch (Exception e) {
            log.error("Failed to verify Aiven API connection: {}", e.getMessage(), e);
            return false;
        }
    }

    /**
     * Deletes all topics whose names start with the configured test-topic prefix.
     * Convenience method used directly by {@code KafkaTopicCleanupManager}.
     *
     * @return number of topics deleted
     */
    public int deleteAllTestTopics() {
        log.info("Starting cleanup of all test topics via Aiven API");

        List<String> allTopics = getTopicList();
        String testTopicPrefix = config.testTopicPrefix();

        List<String> testTopics = allTopics.stream()
                .filter(topic -> topic.startsWith(testTopicPrefix))
                .collect(Collectors.toList());

        if (testTopics.isEmpty()) {
            log.info("No test topics found to delete");
            return 0;
        }

        log.info("Found {} test topics to delete: {}", testTopics.size(), testTopics);
        return deleteTopics(testTopics);
    }
}
