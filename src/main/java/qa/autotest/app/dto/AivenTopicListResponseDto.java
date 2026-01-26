package qa.autotest.app.dto;

import com.fasterxml.jackson.annotation.JsonIgnoreProperties;
import com.fasterxml.jackson.annotation.JsonProperty;
import lombok.AllArgsConstructor;
import lombok.Builder;
import lombok.Data;
import lombok.NoArgsConstructor;

import java.util.List;

/**
 * DTO for Aiven API topic list response
 * Represents response from GET /v1/project/{project}/service/{service}/topic
 */
@Data
@Builder
@NoArgsConstructor
@AllArgsConstructor
@JsonIgnoreProperties(ignoreUnknown = true)
public class AivenTopicListResponseDto {
    
    /**
     * List of Kafka topics
     */
    @JsonProperty("topics")
    private List<AivenTopicDto> topics;
    
    /**
     * Error message if request failed
     */
    @JsonProperty("message")
    private String message;
    
    /**
     * Errors list if request failed
     */
    @JsonProperty("errors")
    private List<ErrorDto> errors;
    
    /**
     * Single topic DTO
     */
    @Data
    @Builder
    @NoArgsConstructor
    @AllArgsConstructor
    @JsonIgnoreProperties(ignoreUnknown = true)
    public static class AivenTopicDto {
        
        /**
         * Topic name
         */
        @JsonProperty("topic_name")
        private String topicName;
        
        /**
         * Number of partitions
         */
        @JsonProperty("partitions")
        private Integer partitions;
        
        /**
         * Replication factor
         */
        @JsonProperty("replication")
        private Integer replication;
        
        /**
         * Minimum in-sync replicas
         */
        @JsonProperty("min_insync_replicas")
        private Integer minInsyncReplicas;
        
        /**
         * Retention bytes
         */
        @JsonProperty("retention_bytes")
        private Long retentionBytes;
        
        /**
         * Retention hours
         */
        @JsonProperty("retention_hours")
        private Long retentionHours;
        
        /**
         * Cleanup policy
         */
        @JsonProperty("cleanup_policy")
        private String cleanupPolicy;
    }
    
    /**
     * Error details DTO
     */
    @Data
    @Builder
    @NoArgsConstructor
    @AllArgsConstructor
    @JsonIgnoreProperties(ignoreUnknown = true)
    public static class ErrorDto {
        
        /**
         * Error message
         */
        @JsonProperty("message")
        private String message;
        
        /**
         * Error status
         */
        @JsonProperty("status")
        private Integer status;
    }
}
