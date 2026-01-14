package qa.autotest.app.dto;

import lombok.AllArgsConstructor;
import lombok.Builder;
import lombok.Data;
import lombok.NoArgsConstructor;

import java.util.Map;

/**
 * Data Transfer Object for consumed Kafka records
 * Wraps Kafka ConsumerRecord with additional metadata
 */
@Data
@Builder
@NoArgsConstructor
@AllArgsConstructor
public class ConsumerRecordDto {
    
    /**
     * Topic name
     */
    private String topic;
    
    /**
     * Partition number
     */
    private Integer partition;
    
    /**
     * Offset
     */
    private Long offset;
    
    /**
     * Message key
     */
    private String key;
    
    /**
     * Message value
     */
    private String value;
    
    /**
     * Message headers
     */
    private Map<String, String> headers;
    
    /**
     * Timestamp
     */
    private Long timestamp;
    
    /**
     * Timestamp type (CREATE_TIME or LOG_APPEND_TIME)
     */
    private String timestampType;
}
