package qa.autotest.framework.infrastructure.api.aiven;

import io.restassured.builder.RequestSpecBuilder;
import io.restassured.filter.log.LogDetail;
import io.restassured.filter.log.RequestLoggingFilter;
import io.restassured.filter.log.ResponseLoggingFilter;
import io.restassured.http.ContentType;
import io.restassured.response.Response;
import io.restassured.specification.RequestSpecification;
import lombok.extern.slf4j.Slf4j;
import qa.autotest.framework.infrastructure.api.aiven.dto.AivenTopicListResponseDto;
import qa.autotest.framework.config.KafkaConfig;

import java.util.List;
import java.util.stream.Collectors;

import static io.restassured.RestAssured.given;

/**
 * Aiven API Controller
 * Handles communication with Aiven REST API for Kafka topic management
 * <p>
 * API Documentation: https://api.aiven.io/doc/
 */
@Slf4j
public class AivenApiController {

    private final KafkaConfig config;
    private final RequestSpecification requestSpec;

    public AivenApiController(KafkaConfig config) {
        this.config = config;
        this.requestSpec = createRequestSpecification();
    }

    /**
     * Creates REST Assured request specification with authentication.
     * <p>
     * Base URI is set exclusively via {@link RequestSpecBuilder#setBaseUri} so that
     * every instance carries its own self-contained spec. The static field
     * {@code RestAssured.baseURI} is intentionally <em>not</em> assigned here:
     * mutating that shared global state would introduce a race condition when
     * multiple {@code AivenApiController} instances are constructed concurrently
     * in parallel test threads.
     */
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
     * Get list of all Kafka topics from Aiven
     * <p>
     * GET /v1/project/{project}/service/{service}/topic
     *
     * @return List of topic names
     */
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
                AivenTopicListResponseDto topicListResponse = response.as(AivenTopicListResponseDto.class);

                if (topicListResponse.getTopics() != null) {
                    List<String> topicNames = topicListResponse.getTopics().stream()
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
     * Delete a Kafka topic via Aiven API
     * <p>
     * DELETE /v1/project/{project}/service/{service}/topic/{topic_name}
     *
     * @param topicName Name of the topic to delete
     * @return True if deletion was successful
     */
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

            // 200 OK means successful deletion
            if (response.getStatusCode() == 200) {
                log.info("Successfully deleted topic '{}' via Aiven API", topicName);
                return true;
            }

            // 404 means topic doesn't exist (which is fine for cleanup)
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
     * Delete multiple Kafka topics via Aiven API
     *
     * @param topicNames List of topic names to delete
     * @return Number of successfully deleted topics
     */
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
     * Delete all test topics (topics with test prefix) via Aiven API
     *
     * @return Number of successfully deleted topics
     */
    public int deleteAllTestTopics() {
        log.info("Starting cleanup of all test topics via Aiven API");

        List<String> allTopics = getTopicList();
        String testTopicPrefix = config.testTopicPrefix();

        // Filter only test topics
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

    /**
     * Check if Aiven API is accessible and configured correctly
     *
     * @return True if API is accessible
     */
    public boolean verifyApiConnection() {
        try {
            List<String> topics = getTopicList();
            log.info("Aiven API connection verified successfully. Found {} topics", topics.size());
            return true;
        } catch (Exception e) {
            log.error("Failed to verify Aiven API connection: {}", e.getMessage(), e);
            return false;
        }
    }
}
