package qa.autotest.framework.kafka;

import io.qameta.allure.Step;
import lombok.extern.slf4j.Slf4j;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.clients.producer.RecordMetadata;
import org.apache.kafka.common.header.Header;
import org.apache.kafka.common.header.internals.RecordHeader;
import org.apache.kafka.common.serialization.StringSerializer;
import qa.autotest.app.dto.KafkaMessageDto;
import qa.autotest.framework.config.KafkaConfig;

import java.lang.ref.WeakReference;
import java.nio.charset.StandardCharsets;
import java.time.Duration;
import java.util.ArrayList;
import java.util.Collections;
import java.util.HashSet;
import java.util.List;
import java.util.Properties;
import java.util.Set;
import java.util.concurrent.Future;

/**
 * Thread-safe Kafka Producer Manager with Explicit Thread Tracking
 * Manages Kafka producer instances and message sending operations
 * Prevents memory leaks in parallel execution environments
 */
@Slf4j
public class KafkaProducerManager implements AutoCloseable {

    private final KafkaConfig config;
    private final ThreadLocal<KafkaProducer<String, String>> producerThreadLocal;
    
    /**
     * Track all created producers across all threads using WeakReferences
     * This prevents memory leaks in ForkJoinPool and parallel execution scenarios
     */
    private final Set<WeakReference<KafkaProducer<String, String>>> allProducers = 
        Collections.synchronizedSet(new HashSet<>());

    public KafkaProducerManager(KafkaConfig config) {
        this.config = config;
        this.producerThreadLocal = ThreadLocal.withInitial(this::createProducer);
        log.debug("KafkaProducerManager initialized with explicit thread tracking");
    }

    /**
     * Creates a new Kafka producer with configuration and registers it for tracking
     * Uses WeakReference to allow garbage collection while maintaining cleanup capability
     */
    private KafkaProducer<String, String> createProducer() {
        log.debug("Creating Kafka producer on thread: {}", Thread.currentThread().getName());

        // Base properties
        Properties props = KafkaPropertiesBuilder.buildBaseProperties(config);
        props.put(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, StringSerializer.class.getName());
        props.put(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, StringSerializer.class.getName());
        props.put(ProducerConfig.ACKS_CONFIG, config.producerAcks());
        props.put(ProducerConfig.RETRIES_CONFIG, config.producerRetries());
        props.put(ProducerConfig.ENABLE_IDEMPOTENCE_CONFIG, config.producerEnableIdempotence());
        props.put(ProducerConfig.MAX_IN_FLIGHT_REQUESTS_PER_CONNECTION, config.producerMaxInFlightRequests());
        props.put(ProducerConfig.BATCH_SIZE_CONFIG, config.producerBatchSize());
        props.put(ProducerConfig.LINGER_MS_CONFIG, config.producerLingerMs());
        props.put(ProducerConfig.REQUEST_TIMEOUT_MS_CONFIG, config.producerRequestTimeoutMs());

        // Security configuration (SSL/TLS)
        KafkaPropertiesBuilder.configureSecurity(props, config);

        KafkaProducer<String, String> producer = new KafkaProducer<>(props);
        
        // Register producer for global tracking to prevent memory leaks
        allProducers.add(new WeakReference<>(producer));
        log.debug("Registered producer for tracking. Total tracked: {}", allProducers.size());
        
        return producer;
    }

    /**
     * Gets the producer for the current thread
     */
    private KafkaProducer<String, String> getProducer() {
        return producerThreadLocal.get();
    }

    /**
     * Sends a message synchronously
     *
     * @param message Message to send
     * @return Record metadata with offset and partition
     */
    @Step("Send message to Kafka topic: {message.topic}")
    public RecordMetadata sendSync(KafkaMessageDto message) {
        long startTime = System.currentTimeMillis();

        try {
            ProducerRecord<String, String> record = createProducerRecord(message);

            log.info("Sending message sync to topic '{}' with key '{}': {}",
                    message.getTopic(), message.getKey(), message.getValue());

            RecordMetadata metadata = getProducer().send(record).get();

            long duration = System.currentTimeMillis() - startTime;
            log.info("Message sent successfully to partition {} with offset {} (took {} ms)",
                    metadata.partition(), metadata.offset(), duration);

            // Update message with offset and partition
            message.setOffset(metadata.offset());
            message.setPartition(metadata.partition());

            // Record metrics
            qa.autotest.framework.metrics.TestMetricsCollector.recordDuration("producer_send_sync", duration);

            return metadata;
        } catch (Exception e) {
            long duration = System.currentTimeMillis() - startTime;
            log.error("Failed to send message sync to topic '{}' after {} ms: {}",
                    message.getTopic(), duration, e.getMessage(), e);

            // Categorize the exception
            Throwable cause = e.getCause();
            String errorMessage = e.getMessage() != null ? e.getMessage().toLowerCase() : "";
            String causeMessage = cause != null && cause.getMessage() != null ?
                    cause.getMessage().toLowerCase() : "";

            // Check for timeout
            if (errorMessage.contains("timeout") || causeMessage.contains("timeout") ||
                    e.getClass().getSimpleName().contains("Timeout")) {
                throw qa.autotest.framework.exceptions.KafkaProducerException.timeout(
                        message.getTopic(),
                        config.producerRequestTimeoutMs(),
                        e
                );
            }

            // Check for serialization errors
            if (errorMessage.contains("serialization") || causeMessage.contains("serialization") ||
                    e.getClass().getSimpleName().contains("Serialization")) {
                throw qa.autotest.framework.exceptions.KafkaProducerException.serialization(
                        message.getTopic(),
                        e
                );
            }

            // Check for network errors
            if (errorMessage.contains("connection") || errorMessage.contains("network") ||
                    causeMessage.contains("connection") || causeMessage.contains("network")) {
                throw qa.autotest.framework.exceptions.KafkaProducerException.network(
                        message.getTopic(),
                        e
                );
            }

            // Generic error
            throw new qa.autotest.framework.exceptions.KafkaProducerException(
                    String.format("Failed to send message to topic '%s'", message.getTopic()),
                    e,
                    qa.autotest.framework.exceptions.KafkaTestException.ErrorType.UNKNOWN
            ).addContext("topic", message.getTopic())
                    .addContext("duration_ms", String.valueOf(duration));
        }
    }

    /**
     * Sends a message asynchronously
     *
     * @param message Message to send
     * @return Future with record metadata
     */
    @Step("Send message async to Kafka topic: {message.topic}")
    public Future<RecordMetadata> sendAsync(KafkaMessageDto message) {
        try {
            ProducerRecord<String, String> record = createProducerRecord(message);

            log.info("Sending message async to topic '{}' with key '{}': {}",
                    message.getTopic(), message.getKey(), message.getValue());

            return getProducer().send(record, (metadata, exception) -> {
                if (exception != null) {
                    log.error("Failed to send message async: {}", exception.getMessage(), exception);
                } else {
                    log.info("Message sent successfully to partition {} with offset {}",
                            metadata.partition(), metadata.offset());
                    message.setOffset(metadata.offset());
                    message.setPartition(metadata.partition());
                }
            });
        } catch (Exception e) {
            log.error("Failed to send message async: {}", e.getMessage(), e);
            throw new RuntimeException("Failed to send message async", e);
        }
    }

    /**
     * Sends multiple messages in batch
     *
     * @param messages List of messages to send
     * @return List of record metadata
     */
    @Step("Send batch of messages")
    public List<RecordMetadata> sendBatch(List<KafkaMessageDto> messages) {
        log.info("Sending batch of {} messages", messages.size());
        List<RecordMetadata> metadataList = new ArrayList<>();

        for (KafkaMessageDto message : messages) {
            RecordMetadata metadata = sendSync(message);
            metadataList.add(metadata);
        }

        log.info("Batch of {} messages sent successfully", metadataList.size());
        return metadataList;
    }

    /**
     * Creates ProducerRecord from KafkaMessageDto
     */
    private ProducerRecord<String, String> createProducerRecord(KafkaMessageDto message) {
        List<Header> headers = new ArrayList<>();

        // Add custom headers if present
        if (message.getHeaders() != null) {
            message.getHeaders().forEach((key, value) ->
                    headers.add(new RecordHeader(key, value.getBytes(StandardCharsets.UTF_8))));
        }

        // Add message ID header for idempotence
        if (message.getMessageId() != null) {
            headers.add(new RecordHeader("message-id",
                    message.getMessageId().getBytes(StandardCharsets.UTF_8)));
        }

        // Add correlation ID if present
        if (message.getCorrelationId() != null) {
            headers.add(new RecordHeader("correlation-id",
                    message.getCorrelationId().getBytes(StandardCharsets.UTF_8)));
        }

        // Add event type if present
        if (message.getEventType() != null) {
            headers.add(new RecordHeader("event-type",
                    message.getEventType().getBytes(StandardCharsets.UTF_8)));
        }

        // Create record with or without partition
        if (message.getPartition() != null) {
            return new ProducerRecord<>(
                    message.getTopic(),
                    message.getPartition(),
                    message.getTimestamp().toEpochMilli(),
                    message.getKey(),
                    message.getValue(),
                    headers
            );
        } else {
            return new ProducerRecord<>(
                    message.getTopic(),
                    null, // Let Kafka decide partition
                    message.getTimestamp().toEpochMilli(),
                    message.getKey(),
                    message.getValue(),
                    headers
            );
        }
    }

    /**
     * Flushes all buffered messages
     */
    @Step("Flush producer")
    public void flush() {
        log.debug("Flushing producer");
        getProducer().flush();
    }

    /**
     * Closes the producer for the current thread only
     * For complete cleanup of all threads, use closeAll()
     */
    @Override
    public void close() {
        KafkaProducer<String, String> producer = producerThreadLocal.get();
        try {
            if (producer != null) {
                log.debug("Closing producer for thread: {}", Thread.currentThread().getName());
                producer.close(Duration.ofSeconds(5));
            }
        } catch (Exception e) {
            log.warn("Error closing producer for current thread: {}", e.getMessage());
        } finally {
            producerThreadLocal.remove();
        }
    }

    /**
     * Closes ALL producers from ALL threads - CRITICAL for preventing memory leaks
     * This method MUST be called in @AfterAll to ensure complete cleanup in parallel execution
     * 
     * In ForkJoinPool and parallel test scenarios, producers from worker threads
     * will not be closed by the standard close() method. This method ensures
     * all producers are properly closed regardless of which thread created them.
     * 
     * @apiNote Call this method in BaseTest.globalCleanup() annotated with @AfterAll
     */
    public void closeAll() {
        log.info("Closing ALL producers from ALL threads. Total tracked: {}", allProducers.size());
        
        int closedCount = 0;
        int failedCount = 0;
        
        synchronized (allProducers) {
            for (WeakReference<KafkaProducer<String, String>> ref : allProducers) {
                KafkaProducer<String, String> producer = ref.get();
                if (producer != null) {
                    try {
                        producer.close(Duration.ofSeconds(5));
                        closedCount++;
                        log.debug("Successfully closed producer instance");
                    } catch (Exception e) {
                        failedCount++;
                        log.warn("Failed to close producer instance: {}", e.getMessage());
                    }
                }
            }
            allProducers.clear();
        }
        
        // Also remove from current thread
        producerThreadLocal.remove();
        
        log.info("Producer cleanup complete. Closed: {}, Failed: {}", closedCount, failedCount);
    }
    
    /**
     * Gets the number of currently tracked producers across all threads
     * Useful for monitoring and debugging memory usage
     * 
     * @return number of producers being tracked
     */
    public int getTrackedProducerCount() {
        synchronized (allProducers) {
            // Count only non-null references (not yet garbage collected)
            return (int) allProducers.stream()
                .map(WeakReference::get)
                .filter(p -> p != null)
                .count();
        }
    }
}
