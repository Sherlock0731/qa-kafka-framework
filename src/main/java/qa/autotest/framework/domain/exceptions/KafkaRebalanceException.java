package qa.autotest.framework.domain.exceptions;

/**
 * Exception thrown when consumer group rebalancing fails or times out
 * Categorized as KAFKA_REBALANCE for Allure
 */
public class KafkaRebalanceException extends KafkaTestException {

    private static final String ERROR_CATEGORY = "KAFKA_REBALANCE";

    public KafkaRebalanceException(String groupId, String reason) {
        super(
                String.format("Consumer group rebalance failed: %s", reason),
                ERROR_CATEGORY,
                ErrorType.REBALANCE
        );
        addContext("consumer_group_id", groupId);
        addContext("reason", reason);
    }

    public KafkaRebalanceException(String groupId, String reason, Throwable cause) {
        super(
                String.format("Consumer group rebalance failed: %s", reason),
                cause,
                ERROR_CATEGORY,
                ErrorType.REBALANCE
        );
        addContext("consumer_group_id", groupId);
        addContext("reason", reason);
    }
}
