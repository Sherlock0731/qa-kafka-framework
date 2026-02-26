package qa.autotest.framework.domain.exceptions;

/**
 * Exception thrown when Kafka consumer operation fails
 * Categorized as KAFKA_CONSUMER_ERROR for Allure
 */
public class KafkaConsumerException extends KafkaTestException {

    private static final String ERROR_CATEGORY = "KAFKA_CONSUMER_ERROR";

    public KafkaConsumerException(String message, ErrorType errorType) {
        super(message, ERROR_CATEGORY, errorType);
    }

    public KafkaConsumerException(String message, Throwable cause, ErrorType errorType) {
        super(message, cause, ERROR_CATEGORY, errorType);
    }

    public static KafkaConsumerException notInitialized(String threadName) {
        KafkaConsumerException ex = new KafkaConsumerException(
                String.format("Consumer not initialized for thread: %s", threadName),
                ErrorType.THREAD_SYNC
        );
        ex.addContext("thread", threadName);
        return ex;
    }

    public static KafkaConsumerException pollTimeout(String topic, long timeoutMs) {
        KafkaConsumerException ex = new KafkaConsumerException(
                String.format("Poll timeout for topic '%s'", topic),
                ErrorType.TIMEOUT
        );
        ex.addContext("topic", topic);
        ex.addContext("timeout_ms", String.valueOf(timeoutMs));
        return ex;
    }

    public static KafkaConsumerException offsetCommitFailed(String topic, int partition, Throwable cause) {
        KafkaConsumerException ex = new KafkaConsumerException(
                String.format("Failed to commit offset for topic '%s', partition %d", topic, partition),
                cause,
                ErrorType.OFFSET
        );
        ex.addContext("topic", topic);
        ex.addContext("partition", String.valueOf(partition));
        return ex;
    }
}
