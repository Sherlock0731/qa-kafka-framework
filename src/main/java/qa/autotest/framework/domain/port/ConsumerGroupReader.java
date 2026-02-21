package qa.autotest.framework.domain.port;

import qa.autotest.framework.domain.model.ConsumerGroup;

/**
 * Port Interface: ConsumerGroupReader (Outbound Port)
 * <p>
 * Defines the contract for reading consumer group state from the broker.
 * Implemented by {@code KafkaAdminAdapter} — the single class responsible
 * for all AdminClient operations.
 * <p>
 * <h3>SRP rationale</h3>
 * Consumer group introspection requires an {@code AdminClient} call
 * ({@code describeConsumerGroups}) which is an <em>administrative</em>
 * operation, not a consumer operation.  Keeping it here — separate from
 * {@link MessageConsumer} — ensures that {@code KafkaConsumerAdapter} owns
 * only {@code KafkaConsumer} interactions, and {@code KafkaAdminAdapter}
 * owns all {@code AdminClient} interactions.
 */
public interface ConsumerGroupReader {

    /**
     * Returns the current state of the given consumer group as reported
     * by the broker coordinator.
     * <p>
     * Calls {@code AdminClient.describeConsumerGroups()} — a network round-trip.
     * Used by {@link qa.autotest.framework.utils.KafkaAwaitHelper#awaitRebalance}
     * to verify that the broker-side group FSM has reached
     * {@link ConsumerGroup.GroupState#STABLE} and partitions are assigned.
     *
     * @param groupId the consumer group ID to inspect
     * @return current {@link ConsumerGroup} snapshot; never {@code null}
     * (returns an EMPTY group on any broker error)
     */
    ConsumerGroup describeConsumerGroup(String groupId);
}
