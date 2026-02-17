package qa.autotest.framework.exceptions;

/**
 * Exception thrown when test data is invalid or generation fails
 * Categorized as TEST_DATA_ISSUE for Allure
 */
public class TestDataException extends KafkaTestException {

    private static final String ERROR_CATEGORY = "TEST_DATA_ISSUE";

    public TestDataException(String message) {
        super(message, ERROR_CATEGORY, ErrorType.TEST_DATA);
    }

    public TestDataException(String message, Throwable cause) {
        super(message, cause, ERROR_CATEGORY, ErrorType.TEST_DATA);
    }

    public static TestDataException invalidFormat(String fieldName, String expectedFormat) {
        TestDataException ex = new TestDataException(
                String.format("Invalid format for field '%s', expected: %s", fieldName, expectedFormat)
        );
        ex.addContext("field", fieldName);
        ex.addContext("expected_format", expectedFormat);
        return ex;
    }

    public static TestDataException generationFailed(String dataType, Throwable cause) {
        TestDataException ex = new TestDataException(
                String.format("Failed to generate test data: %s", dataType),
                cause
        );
        ex.addContext("data_type", dataType);
        return ex;
    }
}
