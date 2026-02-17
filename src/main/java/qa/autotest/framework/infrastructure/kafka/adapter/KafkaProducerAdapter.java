package qa.autotest.framework.infrastructure.kafka.adapter;

import lombok.extern.slf4j.Slf4j;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.clients.producer.RecordMetadata;
import org.apache.kafka.common.header.Header;
import org.apache.kafka.common.header.internals.RecordHeader;
import org.apache.kafka.common.serialization.StringSerializer;
import qa.autotest.framework.config.KafkaConfig;
import qa.autotest.framework.domain.model.Message;
import qa.autotest.framework.domain.model.PublishResult;
import qa.autotest.framework.domain.port.MessagePublisher;
import qa.autotest.framework.kafka.KafkaPropertiesBuilder;

import java.lang.ref.WeakReference;
import java.nio.charset.StandardCharsets;
import java.time.Duration;
import java.util.*;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.Future;

/**
 * Infrastructure Adapter: KafkaProducerAdapter
 * <p>
 * Implements the MessagePublisher port using Apache Kafka.
 * This adapter translates domain operations to Kafka-specific calls.
 * <p>
 * Architecture:
 * - Implements domain port (MessagePublisher)
 * - Depends on external library (Kafka)
 * - Manages technical concerns (threading, connection pooling, serialization)
 * - Thread-safe with explicit thread tracking to prevent memory leaks
 */
@Slf4j
public class KafkaProducerAdapter implements MessagePublisher {

    private final KafkaConfig config;
    private final ThreadLocal<KafkaProducer<String, String>> producerThreadLocal;

    /**
     * Track all created producers across all threads using WeakReferences
     * Prevents memory leaks in parallel execution (ForkJoinPool)
     */
    private final Set<WeakReference<KafkaProducer<String, String>>> allProducers =
            Collections.synchronizedSet(new HashSet<>());

    public KafkaProducerAdapter(KafkaConfig config) {
        this.config = config;
        this.producerThreadLocal = ThreadLocal.withInitial(this::createProducer);
        log.debug("KafkaProducerAdapter initialized with thread tracking");
    }

    /**
     * Creates a new Kafka producer with proper configuration
     */
    private KafkaProducer<String, String> createProducer() {
        log.debug("Creating Kafka producer on thread: {}", Thread.currentThread().getName());

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

        KafkaPropertiesBuilder.configureSecurity(props, config);

        KafkaProducer<String, String> producer = new KafkaProducer<>(props);

        // Register for tracking
        allProducers.add(new WeakReference<>(producer));
        log.debug("Registered producer for tracking. Total tracked: {}", getTrackedProducerCount());

        return producer;
    }

    /**
     * Gets the producer for the current thread
     */
    private KafkaProducer<String, String> getProducer() {
        return producerThreadLocal.get();
    }

    @Override
    public PublishResult publish(Message message) {
        long startTime = System.currentTimeMillis();

        try {
            // Convert domain Message to Kafka ProducerRecord
            ProducerRecord<String, String> record = toProducerRecord(message);

            // Send synchronously
            Future<RecordMetadata> future = getProducer().send(record);
            RecordMetadata metadata = future.get();

            long duration = System.currentTimeMillis() - startTime;
            log.debug("Message published: topic={}, partition={}, offset={}, duration={}ms",
                    metadata.topic(), metadata.partition(), metadata.offset(), duration);

            return PublishResult.success(
                    message,
                    metadata.partition(),
                    metadata.offset(),
                    metadata.timestamp()
            );

        } catch (Exception e) {
            long duration = System.currentTimeMillis() - startTime;
            log.error("Failed to publish message: topic={}, error={}, duration={}ms",
                    message.getTopic().getName(), e.getMessage(), duration, e);

            return PublishResult.failure(
                    message,
                    e.getMessage(),
                    categorizeError(e),
                    e
            );
        }
    }

    @Override
    public CompletableFuture<PublishResult> publishAsync(Message message) {
        CompletableFuture<PublishResult> future = new CompletableFuture<>();

        try {
            ProducerRecord<String, String> record = toProducerRecord(message);

            getProducer().send(record, (metadata, exception) -> {
                if (exception == null) {
                    future.complete(PublishResult.success(
                            message,
                            metadata.partition(),
                            metadata.offset(),
                            metadata.timestamp()
                    ));
                } else {
                    future.complete(PublishResult.failure(
                            message,
                            exception.getMessage(),
                            categorizeError(exception),
                            exception
                    ));
                }
            });

        } catch (Exception e) {
            future.complete(PublishResult.failure(
                    message,
                    e.getMessage(),
                    categorizeError(e),
                    e
            ));
        }

        return future;
    }

    @Override
    public List<PublishResult> publishBatch(List<Message> messages) {
        List<PublishResult> results = new ArrayList<>();

        for (Message message : messages) {
            results.add(publish(message));
        }

        return results;
    }

    @Override
    public CompletableFuture<List<PublishResult>> publishBatchAsync(List<Message> messages) {
        List<CompletableFuture<PublishResult>> futures = messages.stream()
                .map(this::publishAsync)
                .toList();

        return CompletableFuture.allOf(futures.toArray(new CompletableFuture[0]))
                .thenApply(v -> futures.stream()
                        .map(CompletableFuture::join)
                        .toList());
    }

    @Override
    public void flush() {
        log.debug("Flushing producer");
        getProducer().flush();
    }

    @Override
    public void close() {
        log.debug("Closing current thread producer");
        KafkaProducer<String, String> producer = producerThreadLocal.get();
        if (producer != null) {
            producer.close(Duration.ofSeconds(5));
        }
        producerThreadLocal.remove();
    }

    /**
     * CRITICAL: Closes ALL producers from ALL threads
     * Must be called in @AfterAll to prevent memory leaks
     */
    public void closeAll() {
        log.info("Closing ALL producers from ALL threads. Total tracked: {}", getTrackedProducerCount());

        int closed = 0;
        int failed = 0;

        synchronized (allProducers) {
            for (WeakReference<KafkaProducer<String, String>> ref : allProducers) {
                KafkaProducer<String, String> producer = ref.get();
                if (producer != null) {
                    try {
                        producer.close(Duration.ofSeconds(5));
                        closed++;
                    } catch (Exception e) {
                        failed++;
                        log.error("Failed to close producer: {}", e.getMessage());
                    }
                }
            }
            allProducers.clear();
        }

        producerThreadLocal.remove();
        log.info("Producer cleanup complete. Closed: {}, Failed: {}", closed, failed);
    }

    /**
     * Gets the number of tracked producers
     */
    public int getTrackedProducerCount() {
        return (int) allProducers.stream()
                .map(WeakReference::get)
                .filter(Objects::nonNull)
                .count();
    }

    /**
     * Converts domain Message to Kafka ProducerRecord
     */
    private ProducerRecord<String, String> toProducerRecord(Message message) {
        String topic = message.getTopic().getName();
        String key = message.getKey();
        String value = message.getContent();
        Integer partition = message.getPartition();

        // Convert headers
        List<Header> headers = new ArrayList<>();
        if (message.hasHeaders()) {
            message.getHeaders().forEach((k, v) ->
                    headers.add(new RecordHeader(k, v.getBytes(StandardCharsets.UTF_8)))
            );
        }

        // Add metadata headers
        headers.add(new RecordHeader("message-id", message.getMessageId().getBytes(StandardCharsets.UTF_8)));
        if (message.isCorrelated()) {
            headers.add(new RecordHeader("correlation-id", message.getCorrelationId().getBytes(StandardCharsets.UTF_8)));
        }
        if (message.isEvent()) {
            headers.add(new RecordHeader("event-type", message.getEventType().getBytes(StandardCharsets.UTF_8)));
        }

        if (partition != null) {
            return new ProducerRecord<>(topic, partition, key, value, headers);
        } else {
            return new ProducerRecord<>(topic, null, key, value, headers);
        }
    }

    /**
     * Categorizes exception into domain error category
     */
    private PublishResult.ErrorCategory categorizeError(Throwable exception) {
        String message = exception.getMessage();

        if (message == null) {
            return PublishResult.ErrorCategory.UNKNOWN_ERROR;
        }

        if (message.contains("timeout") || message.contains("timed out")) {
            return PublishResult.ErrorCategory.TIMEOUT_ERROR;
        }
        if (message.contains("network") || message.contains("connection")) {
            return PublishResult.ErrorCategory.NETWORK_ERROR;
        }
        if (message.contains("authentication") || message.contains("credentials")) {
            return PublishResult.ErrorCategory.AUTHENTICATION_ERROR;
        }
        if (message.contains("authorization") || message.contains("not authorized")) {
            return PublishResult.ErrorCategory.AUTHORIZATION_ERROR;
        }
        if (message.contains("topic") && message.contains("not found")) {
            return PublishResult.ErrorCategory.TOPIC_NOT_FOUND;
        }
        if (message.contains("broker") || message.contains("unavailable")) {
            return PublishResult.ErrorCategory.BROKER_NOT_AVAILABLE;
        }
        if (message.contains("buffer") || message.contains("memory")) {
            return PublishResult.ErrorCategory.BUFFER_EXHAUSTED;
        }
        if (message.contains("serialization")) {
            return PublishResult.ErrorCategory.SERIALIZATION_ERROR;
        }

        return PublishResult.ErrorCategory.UNKNOWN_ERROR;
    }
}
