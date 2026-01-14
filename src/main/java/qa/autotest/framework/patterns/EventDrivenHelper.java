package qa.autotest.framework.patterns;

import lombok.extern.slf4j.Slf4j;
import qa.autotest.app.dto.ConsumerRecordDto;
import qa.autotest.app.dto.KafkaMessageDto;

import java.util.HashMap;
import java.util.Map;

/**
 * Event-Driven Testing Helper
 * Helps implement event-driven testing patterns
 */
@Slf4j
public class EventDrivenHelper {
    
    /**
     * Creates an event message
     */
    public static KafkaMessageDto createEvent(String topic, String eventType, String payload) {
        Map<String, String> headers = new HashMap<>();
        headers.put("event-type", eventType);
        
        return KafkaMessageDto.builder()
                .topic(topic)
                .eventType(eventType)
                .value(payload)
                .headers(headers)
                .build();
    }
    
    /**
     * Validates event type from consumed record
     */
    public static boolean isEventType(ConsumerRecordDto record, String expectedEventType) {
        if (record.getHeaders() == null) {
            return false;
        }
        String actualEventType = record.getHeaders().get("event-type");
        return expectedEventType.equals(actualEventType);
    }
    
    /**
     * Creates a compensating event for Saga pattern
     */
    public static KafkaMessageDto createCompensatingEvent(String topic, String originalEventId) {
        Map<String, String> headers = new HashMap<>();
        headers.put("event-type", "COMPENSATE");
        headers.put("original-event-id", originalEventId);
        
        return KafkaMessageDto.builder()
                .topic(topic)
                .eventType("COMPENSATE")
                .headers(headers)
                .build();
    }
}
