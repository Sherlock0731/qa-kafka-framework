package qa.autotest.framework.exceptions;

import java.time.Duration;

/**
 * Exception thrown when Kafka operation times out
 * Categorized as INFRASTRUCTURE_TIMEOUT for Allure
 */
public class KafkaTimeoutException extends KafkaTestException {

    private static final String ERROR_CATEGORY = "INFRASTRUCTURE_TIMEOUT";

    public KafkaTimeoutException(String operation, Duration timeout) {
        super(
                String.format("Kafka operation timed out: %s", operation),
                ERROR_CATEGORY,
                ErrorType.TIMEOUT
        );
        addContext("operation", operation);
        addContext("timeout_ms", String.valueOf(timeout.toMillis()));
    }

    public KafkaTimeoutException(String operation, Duration timeout, Throwable cause) {
        super(
                String.format("Kafka operation timed out: %s", operation),
                cause,
                ERROR_CATEGORY,
                ErrorType.TIMEOUT
        );
        addContext("operation", operation);
        addContext("timeout_ms", String.valueOf(timeout.toMillis()));
    }

    public KafkaTimeoutException(String operation, long timeoutMs) {
        this(operation, Duration.ofMillis(timeoutMs));
    }

    public KafkaTimeoutException(String operation, long timeoutMs, Throwable cause) {
        this(operation, Duration.ofMillis(timeoutMs), cause);
    }
}
