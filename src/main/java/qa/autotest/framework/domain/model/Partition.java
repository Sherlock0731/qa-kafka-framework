package qa.autotest.framework.domain.model;

import lombok.Builder;
import lombok.Value;

import java.util.Objects;

/**
 * Domain Entity: Partition
 * <p>
 * Represents a partition in a topic.
 * Partitions enable parallel processing and ordering guarantees.
 */
@Value
@Builder
public class Partition {

    /**
     * The topic this partition belongs to
     */
    Topic topic;

    /**
     * Partition number (0-indexed)
     */
    int partitionNumber;

    /**
     * Leader broker ID for this partition
     */
    Integer leader;

    /**
     * In-Sync Replicas (ISR) count
     */
    @Builder.Default
    int isrCount = 1;

    /**
     * Current offset (last message position)
     */
    Long currentOffset;

    /**
     * Earliest available offset
     */
    Long beginningOffset;

    /**
     * Latest available offset (high watermark)
     */
    Long endOffset;

    /**
     * Domain validation
     */
    public void validate() {
        Objects.requireNonNull(topic, "Topic cannot be null");

        if (partitionNumber < 0) {
            throw new IllegalArgumentException("Partition number must be non-negative, got: " + partitionNumber);
        }

        if (partitionNumber >= topic.getPartitionCount()) {
            throw new IllegalArgumentException(
                    String.format("Partition number %d exceeds topic partition count %d",
                            partitionNumber, topic.getPartitionCount())
            );
        }

        if (isrCount < 1) {
            throw new IllegalArgumentException("ISR count must be at least 1, got: " + isrCount);
        }
    }

    /**
     * Business logic: checks if partition has a leader
     */
    public boolean hasLeader() {
        return leader != null;
    }

    /**
     * Business logic: checks if partition is empty
     */
    public boolean isEmpty() {
        return endOffset != null && beginningOffset != null &&
                endOffset.equals(beginningOffset);
    }

    /**
     * Business logic: checks if partition is under-replicated
     */
    public boolean isUnderReplicated() {
        return topic != null && isrCount < topic.getReplicationFactor();
    }

    /**
     * Business logic: calculates number of messages in partition
     */
    public long messageCount() {
        if (endOffset == null || beginningOffset == null) {
            return 0;
        }
        return Math.max(0, endOffset - beginningOffset);
    }

    /**
     * Business logic: calculates consumer lag
     */
    public long calculateLag(Long consumerOffset) {
        if (endOffset == null || consumerOffset == null) {
            return 0;
        }
        return Math.max(0, endOffset - consumerOffset);
    }

    /**
     * Business logic: checks if consumer is at the end
     */
    public boolean isConsumerAtEnd(Long consumerOffset) {
        return endOffset != null && consumerOffset != null &&
                consumerOffset.equals(endOffset);
    }
}
