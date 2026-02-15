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

import java.nio.charset.StandardCharsets;
import java.util.ArrayList;
import java.util.List;
import java.util.Properties;
import java.util.concurrent.Future;

/**
 * Thread-safe Kafka Producer Manager
 * Manages Kafka producer instances and message sending operations
 */
@Slf4j
public class KafkaProducerManager implements AutoCloseable {

    private final KafkaConfig config;
    private final ThreadLocal<KafkaProducer<String, String>> producerThreadLocal;

    public KafkaProducerManager(KafkaConfig config) {
        this.config = config;
        this.producerThreadLocal = ThreadLocal.withInitial(this::createProducer);
    }

    /**
     * Creates a new Kafka producer with configuration
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

        return new KafkaProducer<>(props);
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
     * Closes the producer for the current thread
     */
    @Override
    public void close() {
        KafkaProducer<String, String> producer = producerThreadLocal.get();
        try {
            if (producer != null) {
                producer.close();
            }
        } finally {
            producerThreadLocal.remove();
        }
    }

    /**
     * Closes all producers (call at the end of test suite)
     */
    public void closeAll() {
        log.info("Closing all producers");
        producerThreadLocal.remove();
    }
}
