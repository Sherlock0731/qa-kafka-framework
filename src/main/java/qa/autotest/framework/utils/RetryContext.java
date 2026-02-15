package qa.autotest.framework.utils;

import io.qameta.allure.Allure;
import lombok.Getter;
import lombok.extern.slf4j.Slf4j;

/**
 * Context object for tracking retry operations
 * Integrates with Allure for detailed retry visualization
 */
@Slf4j
@Getter
public class RetryContext {
    
    private final int attempt;
    private final long backoffMs;
    private Throwable lastError;
    private boolean success;
    private long startTime;
    private long endTime;
    
    public RetryContext(int attempt, long backoffMs) {
        this.attempt = attempt;
        this.backoffMs = backoffMs;
        this.success = false;
        this.startTime = System.currentTimeMillis();
    }
    
    /**
     * Mark retry as successful
     */
    public void markSuccess() {
        this.success = true;
        this.endTime = System.currentTimeMillis();
    }
    
    /**
     * Mark retry as failed with exception
     */
    public void markFailure(Throwable error) {
        this.success = false;
        this.lastError = error;
        this.endTime = System.currentTimeMillis();
    }
    
    /**
     * Get duration of this retry attempt
     */
    public long getDuration() {
        if (endTime == 0) {
            return System.currentTimeMillis() - startTime;
        }
        return endTime - startTime;
    }
    
    /**
     * Attach retry information to Allure report
     */
    public void attachToAllure() {
        Allure.step("Retry Attempt #" + attempt, () -> {
            Allure.parameter("Attempt", attempt);
            Allure.parameter("Backoff (ms)", backoffMs);
            Allure.parameter("Duration (ms)", getDuration());
            Allure.parameter("Success", success);
            
            if (lastError != null) {
                Allure.parameter("Error", lastError.getClass().getSimpleName());
                Allure.parameter("Error Message", lastError.getMessage());
            }
        });
        
        log.debug("Retry context attached to Allure - Attempt: {}, Success: {}, Duration: {} ms",
            attempt, success, getDuration());
    }
    
    /**
     * Attach as Allure step with custom name
     */
    public void attachToAllure(String operationName) {
        Allure.step(String.format("%s - Retry Attempt #%d", operationName, attempt), () -> {
            Allure.parameter("Operation", operationName);
            Allure.parameter("Attempt", attempt);
            Allure.parameter("Backoff (ms)", backoffMs);
            Allure.parameter("Duration (ms)", getDuration());
            Allure.parameter("Success", success);
            
            if (lastError != null) {
                Allure.parameter("Error Type", lastError.getClass().getSimpleName());
                Allure.parameter("Error Message", lastError.getMessage());
            }
        });
    }
}
