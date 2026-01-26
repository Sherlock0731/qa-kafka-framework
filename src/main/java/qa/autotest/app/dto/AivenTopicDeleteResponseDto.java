package qa.autotest.app.dto;

import com.fasterxml.jackson.annotation.JsonIgnoreProperties;
import com.fasterxml.jackson.annotation.JsonProperty;
import lombok.AllArgsConstructor;
import lombok.Builder;
import lombok.Data;
import lombok.NoArgsConstructor;

import java.util.List;

/**
 * DTO for Aiven API topic delete response
 * Represents response from DELETE /v1/project/{project}/service/{service}/topic/{topic_name}
 */
@Data
@Builder
@NoArgsConstructor
@AllArgsConstructor
@JsonIgnoreProperties(ignoreUnknown = true)
public class AivenTopicDeleteResponseDto {
    
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
