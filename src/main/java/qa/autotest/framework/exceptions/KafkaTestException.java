package qa.autotest.framework.exceptions;

import lombok.Getter;

import java.util.HashMap;
import java.util.Map;

/**
 * Base exception for all Kafka test framework exceptions
 * Provides error categorization for better Allure reporting
 */
@Getter
public abstract class KafkaTestException extends RuntimeException {

    /**
     * Error category for Allure categories.json matching
     */
    private final String errorCategory;

    /**
     * Additional context information for debugging
     */
    private final Map<String, String> context;

    /**
     * Error type for programmatic handling
     */
    private final ErrorType errorType;

    /**
     * Error types for categorization
     */
    public enum ErrorType {
        TIMEOUT,
        NETWORK,
        SERIALIZATION,
        AUTHENTICATION,
        REBALANCE,
        TOPIC_MANAGEMENT,
        OFFSET,
        THREAD_SYNC,
        CONFIGURATION,
        RESOURCE_EXHAUSTION,
        TEST_DATA,
        UNKNOWN
    }

    protected KafkaTestException(String message, String errorCategory, ErrorType errorType) {
        super(message);
        this.errorCategory = errorCategory;
        this.errorType = errorType;
        this.context = new HashMap<>();
    }

    protected KafkaTestException(String message, Throwable cause, String errorCategory, ErrorType errorType) {
        super(message, cause);
        this.errorCategory = errorCategory;
        this.errorType = errorType;
        this.context = new HashMap<>();
    }

    /**
     * Add context information
     */
    public KafkaTestException addContext(String key, String value) {
        this.context.put(key, value);
        return this;
    }

    /**
     * Add multiple context entries
     */
    public KafkaTestException addContext(Map<String, String> contextMap) {
        this.context.putAll(contextMap);
        return this;
    }

    /**
     * Get formatted context for logging
     */
    public String getFormattedContext() {
        if (context.isEmpty()) {
            return "";
        }

        StringBuilder sb = new StringBuilder("\nContext:\n");
        context.forEach((key, value) ->
                sb.append(String.format("  %s: %s%n", key, value))
        );
        return sb.toString();
    }

    @Override
    public String getMessage() {
        return super.getMessage() + getFormattedContext();
    }
}
