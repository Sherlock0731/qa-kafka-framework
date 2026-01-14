package qa.autotest.app.dto;

import lombok.AllArgsConstructor;
import lombok.Builder;
import lombok.Data;
import lombok.NoArgsConstructor;

/**
 * Data Transfer Object for Kafka TopicPartition
 */
@Data
@Builder
@NoArgsConstructor
@AllArgsConstructor
public class TopicPartitionDto {
    
    /**
     * Topic name
     */
    private String topic;
    
    /**
     * Partition number
     */
    private Integer partition;
}
