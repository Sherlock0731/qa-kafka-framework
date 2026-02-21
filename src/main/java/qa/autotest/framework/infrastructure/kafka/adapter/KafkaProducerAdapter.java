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
import qa.autotest.framework.infrastructure.KafkaPropertiesBuilder;

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

            return PublishResult.failureFrom(message, e.getMessage(), e);
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
                    future.complete(PublishResult.failureFrom(message, exception.getMessage(), exception));
                }
            });

        } catch (Exception e) {
            future.complete(PublishResult.failureFrom(message, e.getMessage(), e));
        }

        return future;
    }

    /**
     * Publishes a batch of messages with true async pipelining.
     *
     * <h3>Algorithm</h3>
     * <ol>
     *   <li>Fire all {@code send(record, callback)} calls without blocking —
     *       Kafka's internal RecordAccumulator batches them and respects
     *       {@code linger.ms} / {@code batch.size}.</li>
     *   <li>Call {@code flush()} once to force the accumulator to drain,
     *       ensuring all records are handed to the network layer before we
     *       start waiting for ACKs.</li>
     *   <li>Collect results via {@code CompletableFuture.allOf().join()} —
     *       a single wait point instead of N sequential round-trips.</li>
     * </ol>
     *
     * <h3>Why this is ×N faster than the old loop</h3>
     * The previous implementation called {@code publish()} per message, which
     * did {@code future.get()} after every send.  This serialised all network
     * round-trips: send₁ → ACK₁ → send₂ → ACK₂ → …  With a broker RTT of
     * ~5 ms and 1 000 messages that is 5 seconds of pure waiting.
     * With pipelining all sends are in-flight simultaneously; wall-clock time
     * ≈ max(single-message latency) rather than sum(all latencies).
     *
     * <h3>Back-pressure for very large batches</h3>
     * When {@code messages.size()} exceeds {@link #BATCH_PIPELINE_CHUNK_SIZE}
     * the list is processed in chunks.  This prevents the internal
     * {@code RecordAccumulator} buffer from filling up and blocking the
     * calling thread, which would silently re-introduce sequential behaviour.
     *
     * @param messages messages to publish; must not be {@code null}
     * @return results in the same order as the input list
     */
    @Override
    public List<PublishResult> publishBatch(List<Message> messages) {
        if (messages == null || messages.isEmpty()) {
            return Collections.emptyList();
        }

        long startTime = System.currentTimeMillis();
        log.debug("publishBatch started: {} messages", messages.size());

        List<PublishResult> allResults = new ArrayList<>(messages.size());

        // Process in chunks to avoid overwhelming the RecordAccumulator buffer
        int total = messages.size();
        for (int chunkStart = 0; chunkStart < total; chunkStart += BATCH_PIPELINE_CHUNK_SIZE) {
            int chunkEnd = Math.min(chunkStart + BATCH_PIPELINE_CHUNK_SIZE, total);
            List<Message> chunk = messages.subList(chunkStart, chunkEnd);
            allResults.addAll(publishChunk(chunk));
        }

        long duration = System.currentTimeMillis() - startTime;
        long succeeded = allResults.stream().filter(PublishResult::isSuccess).count();
        log.info("publishBatch complete: {}/{} succeeded in {}ms (avg {}ms/msg)",
                succeeded, total, duration,
                total > 0 ? duration / total : 0);

        return allResults;
    }

    /**
     * Sends one chunk of messages in a fully-pipelined manner and returns
     * results in input order.
     *
     * <p>Steps:
     * <ol>
     *   <li>Fire all {@code send()} calls — no blocking.</li>
     *   <li>{@code flush()} — forces the accumulator to hand records to
     *       the network thread; callbacks are guaranteed to fire after this.</li>
     *   <li>{@code CompletableFuture.allOf().join()} — single wait point.</li>
     * </ol>
     */
    private List<PublishResult> publishChunk(List<Message> chunk) {
        // Step 1 — fire all sends without blocking
        List<CompletableFuture<PublishResult>> futures = new ArrayList<>(chunk.size());
        for (Message message : chunk) {
            futures.add(publishAsync(message));
        }

        // Step 2 — flush: drains the RecordAccumulator so all records above
        //           are handed to the I/O thread before we wait for callbacks.
        //           Without flush() callbacks might not fire until the next poll
        //           or linger.ms expiry, which would stall allOf().join() below.
        getProducer().flush();

        // Step 3 — single wait point: collect all results preserving order
        try {
            CompletableFuture.allOf(futures.toArray(new CompletableFuture[0])).join();
        } catch (Exception e) {
            // allOf() itself does not throw for individual failures —
            // each future already encodes failure as PublishResult.failure().
            // A join() exception means something unexpected (e.g. cancellation).
            log.error("Unexpected error in publishChunk allOf: {}", e.getMessage(), e);
        }

        return futures.stream()
                .map(CompletableFuture::join)
                .toList();
    }

    /**
     * Maximum number of messages sent in a single pipelined chunk.
     * Prevents the {@code RecordAccumulator} buffer (default 32 MB) from
     * filling when publishing very large batches (e.g. TC-033 with 1 000 msgs).
     * Tune via system property {@code kafka.batch.chunk.size} if needed.
     */
    private static final int BATCH_PIPELINE_CHUNK_SIZE =
            Integer.getInteger("kafka.batch.chunk.size", 200);

    @Override
    public CompletableFuture<List<PublishResult>> publishBatchAsync(List<Message> messages) {
        if (messages == null || messages.isEmpty()) {
            return CompletableFuture.completedFuture(Collections.emptyList());
        }

        // Fire all sends immediately — no chunking needed for the async variant
        // because the caller controls when to block.
        List<CompletableFuture<PublishResult>> futures = messages.stream()
                .map(this::publishAsync)
                .toList();

        // Flush so the RecordAccumulator drains without waiting for linger.ms
        getProducer().flush();

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
}
