package qa.autotest.framework.exceptions;

/**
 * Exception thrown when topic management operation fails
 * Categorized as KAFKA_TOPIC_MANAGEMENT for Allure
 */
public class KafkaTopicManagementException extends KafkaTestException {

    private static final String ERROR_CATEGORY = "KAFKA_TOPIC_MANAGEMENT";

    public KafkaTopicManagementException(String message, ErrorType errorType) {
        super(message, ERROR_CATEGORY, errorType);
    }

    public KafkaTopicManagementException(String message, Throwable cause, ErrorType errorType) {
        super(message, cause, ERROR_CATEGORY, errorType);
    }

    public static KafkaTopicManagementException creationFailed(String topic, int retries, Throwable cause) {
        KafkaTopicManagementException ex = new KafkaTopicManagementException(
                String.format("Failed to create topic '%s' after %d attempts", topic, retries),
                cause,
                ErrorType.TOPIC_MANAGEMENT
        );
        ex.addContext("topic", topic);
        ex.addContext("retry_attempts", String.valueOf(retries));
        return ex;
    }

    public static KafkaTopicManagementException deletionFailed(String topic, int retries, Throwable cause) {
        KafkaTopicManagementException ex = new KafkaTopicManagementException(
                String.format("Failed to delete topic '%s' after %d attempts", topic, retries),
                cause,
                ErrorType.TOPIC_MANAGEMENT
        );
        ex.addContext("topic", topic);
        ex.addContext("retry_attempts", String.valueOf(retries));
        return ex;
    }

    public static KafkaTopicManagementException notFound(String topic) {
        KafkaTopicManagementException ex = new KafkaTopicManagementException(
                String.format("Topic not found: '%s'", topic),
                ErrorType.TOPIC_MANAGEMENT
        );
        ex.addContext("topic", topic);
        return ex;
    }
}
