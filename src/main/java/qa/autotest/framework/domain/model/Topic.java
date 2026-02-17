package qa.autotest.framework.domain.model;

import lombok.Builder;
import lombok.Value;

import java.util.Objects;

/**
 * Domain Entity: Topic
 * <p>
 * Pure business object representing a topic in the messaging system.
 * Contains business validation and domain logic.
 */
@Value
@Builder(toBuilder = true)
public class Topic {

    /**
     * Topic name - must be unique within the cluster
     */
    String name;

    /**
     * Number of partitions for parallel processing
     */
    @Builder.Default
    int partitionCount = 3;

    /**
     * Replication factor for fault tolerance
     */
    @Builder.Default
    short replicationFactor = 1;

    /**
     * Retention period in milliseconds (-1 for infinite)
     */
    @Builder.Default
    long retentionMs = 86400000L; // 1 day default

    /**
     * Compression type (none, gzip, snappy, lz4, zstd)
     */
    @Builder.Default
    String compressionType = "producer";

    /**
     * Domain validation: ensures topic has valid configuration
     *
     * @throws IllegalArgumentException if validation fails
     */
    public void validate() {
        Objects.requireNonNull(name, "Topic name cannot be null");

        if (name.isBlank()) {
            throw new IllegalArgumentException("Topic name cannot be empty");
        }

        if (partitionCount < 1) {
            throw new IllegalArgumentException("Partition count must be at least 1, got: " + partitionCount);
        }

        if (replicationFactor < 1) {
            throw new IllegalArgumentException("Replication factor must be at least 1, got: " + replicationFactor);
        }

        // Validate topic name format (Kafka naming conventions)
        if (!name.matches("[a-zA-Z0-9._-]+")) {
            throw new IllegalArgumentException(
                    "Topic name can only contain: letters, numbers, dots, underscores, hyphens. Got: " + name
            );
        }

        if (name.length() > 249) {
            throw new IllegalArgumentException("Topic name cannot exceed 249 characters, got: " + name.length());
        }

        if (name.equals(".") || name.equals("..")) {
            throw new IllegalArgumentException("Topic name cannot be '.' or '..'");
        }
    }

    /**
     * Business logic: checks if topic is a test topic
     */
    public boolean isTestTopic() {
        return name.startsWith("qa-test") || name.startsWith("test-");
    }

    /**
     * Business logic: checks if topic is a DLQ (Dead Letter Queue)
     */
    public boolean isDlqTopic() {
        return name.endsWith("-dlq");
    }

    /**
     * Business logic: checks if topic is a retry topic
     */
    public boolean isRetryTopic() {
        return name.contains("-retry-");
    }

    /**
     * Business logic: checks if topic has high availability setup
     */
    public boolean isHighlyAvailable() {
        return replicationFactor >= 2;
    }

    /**
     * Business logic: checks if topic supports parallel processing
     */
    public boolean supportsParallelism() {
        return partitionCount > 1;
    }

    /**
     * Business logic: checks if compression is enabled
     */
    public boolean hasCompression() {
        return compressionType != null && !compressionType.equals("none");
    }

    /**
     * Creates a DLQ topic name for this topic
     */
    public Topic createDlqTopic() {
        return Topic.builder()
                .name(name + "-dlq")
                .partitionCount(partitionCount)
                .replicationFactor(replicationFactor)
                .retentionMs(retentionMs)
                .compressionType(compressionType)
                .build();
    }

    /**
     * Creates a retry topic name for this topic
     */
    public Topic createRetryTopic(int retryLevel) {
        return Topic.builder()
                .name(name + "-retry-" + retryLevel)
                .partitionCount(partitionCount)
                .replicationFactor(replicationFactor)
                .retentionMs(retentionMs)
                .compressionType(compressionType)
                .build();
    }

    /**
     * Creates a topic with different partition count
     */
    public Topic withPartitions(int newPartitionCount) {
        return toBuilder().partitionCount(newPartitionCount).build();
    }
}
