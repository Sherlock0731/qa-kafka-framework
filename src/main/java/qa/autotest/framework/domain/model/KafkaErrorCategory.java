package qa.autotest.framework.domain.model;

import org.apache.kafka.common.errors.AuthenticationException;
import org.apache.kafka.common.errors.AuthorizationException;
import org.apache.kafka.common.errors.NetworkException;
import org.apache.kafka.common.errors.TimeoutException;

/**
 * Domain Enum: KafkaErrorCategory
 * <p>
 * Unified failure taxonomy for all Kafka operations.
 * Replaces the duplicate {@code PublishResult.ErrorCategory} and
 * {@code ConsumeResult.ErrorCategory} enums, which shared five identical
 * values ({@code NETWORK_ERROR}, {@code TIMEOUT_ERROR},
 * {@code AUTHENTICATION_ERROR}, {@code AUTHORIZATION_ERROR},
 * {@code UNKNOWN_ERROR}) while diverging on operation-specific ones.
 * <p>
 * <strong>Retryability contract</strong><br>
 * Each category carries an {@link #isRetryable()} flag that callers may use
 * to decide whether to retry an operation. The flag expresses the general
 * intent — transient failures are retryable, permanent ones are not — but
 * callers remain responsible for applying backoff and retry-count limits.
 * <pre>
 *   Retryable    : NETWORK_ERROR, TIMEOUT_ERROR, BROKER_NOT_AVAILABLE,
 *                  BUFFER_EXHAUSTED, GROUP_COORDINATION_ERROR, UNKNOWN_ERROR
 *   Not retryable: SERIALIZATION_ERROR, DESERIALIZATION_ERROR,
 *                  AUTHENTICATION_ERROR, AUTHORIZATION_ERROR,
 *                  TOPIC_NOT_FOUND, OFFSET_OUT_OF_RANGE
 * </pre>
 * <p>
 * <strong>Allure mapping</strong><br>
 * {@link #getDisplayName()} returns a human-readable label intended for use
 * in Allure report failure categories, log messages, and metrics tags.
 */
public enum KafkaErrorCategory {

    // ── Shared — both produce and consume ─────────────────────────────────────

    /** Underlying TCP/IP or broker connection failed. Transient; retryable. */
    NETWORK_ERROR(true, "Network error"),

    /**
     * Operation exceeded its configured timeout. Transient (broker may
     * recover); retryable with backoff.
     */
    TIMEOUT_ERROR(true, "Timeout"),

    /**
     * Client certificate or SASL credentials were rejected.
     * Permanent until credentials are corrected; not retryable.
     */
    AUTHENTICATION_ERROR(false, "Authentication failed"),

    /**
     * Client is authenticated but lacks the required ACL.
     * Permanent until ACLs are updated; not retryable.
     */
    AUTHORIZATION_ERROR(false, "Authorization denied"),

    /** Catch-all for errors that do not match any known category. Retryable as a precaution. */
    UNKNOWN_ERROR(true, "Unknown error"),

    // ── Produce-specific ──────────────────────────────────────────────────────

    /**
     * Message payload could not be serialized to bytes.
     * Permanent data-shape issue; not retryable.
     */
    SERIALIZATION_ERROR(false, "Serialization error"),

    /**
     * Target topic does not exist and auto-creation is disabled.
     * Permanent until the topic is created; not retryable.
     */
    TOPIC_NOT_FOUND(false, "Topic not found"),

    /**
     * Broker reported as unavailable or not yet elected leader.
     * Transient; retryable with backoff.
     */
    BROKER_NOT_AVAILABLE(true, "Broker not available"),

    /**
     * Producer internal send buffer is full (back-pressure).
     * Transient; retryable after draining.
     */
    BUFFER_EXHAUSTED(true, "Producer buffer exhausted"),

    // ── Consume-specific ──────────────────────────────────────────────────────

    /**
     * Consumed bytes could not be deserialized to the target type.
     * Permanent data-shape or schema-mismatch issue; not retryable.
     * Messages causing this error should be routed to the DLQ.
     */
    DESERIALIZATION_ERROR(false, "Deserialization error"),

    /**
     * Consumer group coordinator is unavailable or rebalance timed out.
     * Transient; retryable after coordinator election completes.
     */
    GROUP_COORDINATION_ERROR(true, "Group coordination error"),

    /**
     * Requested offset is outside the available range on the broker
     * (data was deleted by retention policy or compaction).
     * Permanent for the specific offset; not retryable without seeking.
     */
    OFFSET_OUT_OF_RANGE(false, "Offset out of range");

    // ── Fields ────────────────────────────────────────────────────────────────

    private final boolean retryable;
    private final String displayName;

    KafkaErrorCategory(boolean retryable, String displayName) {
        this.retryable = retryable;
        this.displayName = displayName;
    }

    // ── Accessors ──────────────────────────────────────────────────────────────

    /**
     * Returns {@code true} when the failure is transient and the operation
     * may be retried, {@code false} when it is permanent and retrying would
     * not help.
     */
    public boolean isRetryable() {
        return retryable;
    }

    /**
     * Returns a human-readable label for use in Allure failure categories,
     * log messages, and metrics tags.
     */
    public String getDisplayName() {
        return displayName;
    }

    // ── Factory methods ────────────────────────────────────────────────────────

    /**
     * Maps a produce-side exception to the closest {@link KafkaErrorCategory}.
     * <p>
     * Provides a single, consistent mapping so that infrastructure adapters
     * ({@code KafkaProducerAdapter}) do not each reimplement the same
     * {@code instanceof} chain.
     *
     * @param cause the exception thrown during message publication;
     *              {@code null} maps to {@link #UNKNOWN_ERROR}
     * @return best-fit category, never {@code null}
     */
    public static KafkaErrorCategory fromPublishException(Throwable cause) {
        if (cause == null) {
            return UNKNOWN_ERROR;
        }
        if (cause instanceof AuthenticationException) {
            return AUTHENTICATION_ERROR;
        }
        if (cause instanceof AuthorizationException) {
            return AUTHORIZATION_ERROR;
        }
        if (cause instanceof TimeoutException) {
            return TIMEOUT_ERROR;
        }
        if (cause instanceof NetworkException) {
            return NETWORK_ERROR;
        }
        if (cause instanceof org.apache.kafka.common.errors.SerializationException) {
            return SERIALIZATION_ERROR;
        }
        if (cause instanceof org.apache.kafka.common.errors.UnknownTopicOrPartitionException) {
            return TOPIC_NOT_FOUND;
        }
        if (cause instanceof org.apache.kafka.common.errors.NotLeaderOrFollowerException
                || cause instanceof org.apache.kafka.common.errors.LeaderNotAvailableException) {
            return BROKER_NOT_AVAILABLE;
        }
        if (cause instanceof org.apache.kafka.common.errors.RecordTooLargeException
                || cause instanceof org.apache.kafka.clients.producer.BufferExhaustedException) {
            return BUFFER_EXHAUSTED;
        }
        return UNKNOWN_ERROR;
    }

    /**
     * Maps a consume-side exception to the closest {@link KafkaErrorCategory}.
     * <p>
     * Provides a single, consistent mapping so that infrastructure adapters
     * ({@code KafkaConsumerAdapter}) do not each reimplement the same
     * {@code instanceof} chain.
     *
     * @param cause the exception thrown during message consumption;
     *              {@code null} maps to {@link #UNKNOWN_ERROR}
     * @return best-fit category, never {@code null}
     */
    public static KafkaErrorCategory fromConsumeException(Throwable cause) {
        if (cause == null) {
            return UNKNOWN_ERROR;
        }
        if (cause instanceof AuthenticationException) {
            return AUTHENTICATION_ERROR;
        }
        if (cause instanceof AuthorizationException) {
            return AUTHORIZATION_ERROR;
        }
        if (cause instanceof TimeoutException) {
            return TIMEOUT_ERROR;
        }
        if (cause instanceof NetworkException) {
            return NETWORK_ERROR;
        }
        if (cause instanceof org.apache.kafka.common.errors.SerializationException) {
            return DESERIALIZATION_ERROR;
        }
        if (cause instanceof org.apache.kafka.common.errors.OffsetOutOfRangeException) {
            return OFFSET_OUT_OF_RANGE;
        }
        if (cause instanceof org.apache.kafka.common.errors.RebalanceInProgressException
                || cause instanceof org.apache.kafka.common.errors.CoordinatorNotAvailableException
                || cause instanceof org.apache.kafka.common.errors.CoordinatorLoadInProgressException) {
            return GROUP_COORDINATION_ERROR;
        }
        return UNKNOWN_ERROR;
    }
}
