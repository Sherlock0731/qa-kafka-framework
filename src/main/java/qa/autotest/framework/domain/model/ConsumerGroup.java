package qa.autotest.framework.domain.model;

import lombok.Builder;
import lombok.Value;

import java.util.Map;
import java.util.Objects;
import java.util.Set;

/**
 * Domain Entity: ConsumerGroup
 * <p>
 * Represents a consumer group in the messaging system.
 * Consumer groups enable scalable message consumption.
 */
@Value
@Builder
public class ConsumerGroup {

    /**
     * Unique consumer group ID
     */
    String groupId;

    /**
     * Topics subscribed by this consumer group
     */
    Set<Topic> subscribedTopics;

    /**
     * Current partition assignments (partition -> consumer member ID)
     */
    Map<Integer, String> partitionAssignments;

    /**
     * Consumer group state
     */
    GroupState state;

    /**
     * Number of active consumers in the group
     */
    int memberCount;

    /**
     * Coordinator broker ID
     */
    Integer coordinatorId;

    /**
     * Domain validation
     */
    public void validate() {
        Objects.requireNonNull(groupId, "Group ID cannot be null");
        Objects.requireNonNull(state, "State cannot be null");

        if (groupId.isBlank()) {
            throw new IllegalArgumentException("Group ID cannot be empty");
        }

        if (memberCount < 0) {
            throw new IllegalArgumentException("Member count cannot be negative, got: " + memberCount);
        }
    }

    /**
     * Business logic: checks if group is active and operational
     */
    public boolean isActive() {
        return state == GroupState.STABLE && memberCount > 0;
    }

    /**
     * Business logic: checks if group is rebalancing
     */
    public boolean isRebalancing() {
        return state == GroupState.REBALANCING ||
                state == GroupState.PREPARING_REBALANCE ||
                state == GroupState.COMPLETING_REBALANCE;
    }

    /**
     * Business logic: checks if group has any members
     */
    public boolean hasMembers() {
        return memberCount > 0;
    }

    /**
     * Business logic: checks if group is subscribed to a topic
     */
    public boolean isSubscribedTo(Topic topic) {
        return subscribedTopics != null && subscribedTopics.contains(topic);
    }

    /**
     * Business logic: checks if group has a coordinator
     */
    public boolean hasCoordinator() {
        return coordinatorId != null;
    }

    /**
     * Business logic: checks if group is in a healthy state
     */
    public boolean isHealthy() {
        return isActive() && hasCoordinator();
    }

    /**
     * Consumer group states (aligned with Kafka protocol)
     */
    public enum GroupState {
        /**
         * Group is operational and stable
         */
        STABLE,

        /**
         * Group is undergoing partition reassignment
         */
        REBALANCING,

        /**
         * Group has no active members
         */
        EMPTY,

        /**
         * Group is preparing to rebalance
         */
        PREPARING_REBALANCE,

        /**
         * Group coordinator is completing the rebalance
         */
        COMPLETING_REBALANCE,

        /**
         * Group metadata has been removed
         */
        DEAD
    }
}
