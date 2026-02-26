package qa.autotest.framework.domain.exceptions;

/**
 * Exception thrown when Kafka producer operation fails
 * Categorized as KAFKA_PRODUCER_ERROR for Allure
 */
public class KafkaProducerException extends KafkaTestException {

    private static final String ERROR_CATEGORY = "KAFKA_PRODUCER_ERROR";

    public KafkaProducerException(String message, ErrorType errorType) {
        super(message, ERROR_CATEGORY, errorType);
    }

    public KafkaProducerException(String message, Throwable cause, ErrorType errorType) {
        super(message, cause, ERROR_CATEGORY, errorType);
    }

    public static KafkaProducerException timeout(String topic, long timeoutMs, Throwable cause) {
        KafkaProducerException ex = new KafkaProducerException(
                String.format("Failed to send message to topic '%s'", topic),
                cause,
                ErrorType.TIMEOUT
        );
        ex.addContext("topic", topic);
        ex.addContext("timeout_ms", String.valueOf(timeoutMs));
        return ex;
    }

    public static KafkaProducerException serialization(String topic, Throwable cause) {
        KafkaProducerException ex = new KafkaProducerException(
                String.format("Serialization error for topic '%s'", topic),
                cause,
                ErrorType.SERIALIZATION
        );
        ex.addContext("topic", topic);
        return ex;
    }

    public static KafkaProducerException network(String topic, Throwable cause) {
        KafkaProducerException ex = new KafkaProducerException(
                String.format("Network error while sending to topic '%s'", topic),
                cause,
                ErrorType.NETWORK
        );
        ex.addContext("topic", topic);
        return ex;
    }
}
