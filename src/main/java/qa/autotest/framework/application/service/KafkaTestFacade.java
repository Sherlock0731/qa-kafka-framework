package qa.autotest.framework.application.service;

import lombok.Getter;
import lombok.extern.slf4j.Slf4j;
import qa.autotest.framework.config.KafkaConfig;
import qa.autotest.framework.domain.exception.MessageNotFoundException;
import qa.autotest.framework.domain.model.*;
import qa.autotest.framework.domain.port.MessageConsumer;
import qa.autotest.framework.domain.port.MessagePublisher;
import qa.autotest.framework.domain.port.TopicRepository;
import qa.autotest.framework.infrastructure.kafka.adapter.KafkaAdapterFactory;
import qa.autotest.framework.infrastructure.kafka.adapter.KafkaAdapterFactory.KafkaAdapters;

import java.time.Duration;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.Set;
import java.util.function.Predicate;
import java.util.function.Supplier;
import java.util.stream.Collectors;

/**
 * Application Facade: KafkaTestFacade
 * <p>
 * Provides a simplified, domain-oriented API for test classes.
 *
 * <h3>DIP compliance</h3>
 * Imports only port interfaces ({@link MessagePublisher}, {@link MessageConsumer},
 * {@link TopicRepository}) and {@link KafkaAdapterFactory}/{@link KafkaAdapters}.
 * No concrete adapter class ({@code KafkaProducerAdapter} etc.) is referenced.
 * Lifecycle and metrics are captured as {@code Runnable} / {@code Supplier<String>}
 * lambdas — zero concrete adapter types in fields.
 *
 * <h3>Two construction modes</h3>
 * <ol>
 *   <li><b>Convenience</b> — {@code new KafkaTestFacade(config)} — used by
 *       {@code BaseTest}, delegates to {@link KafkaAdapterFactory}.</li>
 *   <li><b>Primary</b> — accepts port interfaces directly — for unit tests
 *       with mocks, no real Kafka connection needed.</li>
 * </ol>
 *
 * @version 2.1.0
 */
@Slf4j
@Getter
public class KafkaTestFacade implements AutoCloseable {

    // ── Application services (depend on port interfaces only) ─────────────────

    private final MessagePublishingService publishingService;
    private final MessageConsumptionService consumptionService;
    private final TopicManagementService topicManagementService;

    // ── Metadata ──────────────────────────────────────────────────────────────

    private final KafkaConfig config;
    private final String consumerGroupId;

    // ── Lifecycle & metrics — stored as lambdas, no concrete adapter types ────

    private final Runnable closeAllAction;
    private final Supplier<String> metricsSupplier;

    // ═══════════════════════════════════════════════════════════════════════════
    // Convenience constructor — delegates to KafkaAdapterFactory
    // ═══════════════════════════════════════════════════════════════════════════

    /**
     * Production / integration-test constructor.
     * Delegates adapter creation to {@link KafkaAdapterFactory}.
     * Captures {@code closeAll()} and {@code metrics()} as lambdas so no
     * concrete adapter type leaks into this Application-layer class.
     * <p>
     * Called by {@code BaseTest.setUp()} — no existing call sites need to change.
     */
    public KafkaTestFacade(KafkaConfig config) {
        KafkaAdapters adapters = KafkaAdapterFactory.create(config);

        this.publishingService = new MessagePublishingService(adapters.publisher());
        this.consumptionService = new MessageConsumptionService(adapters.consumer());
        this.topicManagementService = new TopicManagementService(adapters.topicRepository());

        this.config = config;
        this.consumerGroupId = adapters.consumerGroupId();
        this.closeAllAction = adapters::closeAll;
        this.metricsSupplier = adapters::metrics;

        log.info("KafkaTestFacade initialized [groupId={}]", consumerGroupId);
    }

    // ═══════════════════════════════════════════════════════════════════════════
    // Primary constructor — accepts port interfaces (unit tests / mocks)
    // ═══════════════════════════════════════════════════════════════════════════

    /**
     * Unit-test constructor. Accepts port interfaces directly.
     * {@code closeAll()} and {@code getMetrics()} are no-ops in this mode.
     *
     * <pre>{@code
     * KafkaTestFacade facade = new KafkaTestFacade(
     *     mock(MessagePublisher.class),
     *     mock(MessageConsumer.class),
     *     mock(TopicRepository.class),
     *     config,
     *     "test-group-1"
     * );
     * }</pre>
     */
    public KafkaTestFacade(
            MessagePublisher publisher,
            MessageConsumer consumer,
            TopicRepository topicRepository,
            KafkaConfig config,
            String consumerGroupId) {

        this.publishingService = new MessagePublishingService(publisher);
        this.consumptionService = new MessageConsumptionService(consumer);
        this.topicManagementService = new TopicManagementService(topicRepository);

        this.config = config;
        this.consumerGroupId = consumerGroupId;
        this.closeAllAction = () -> {
        };
        this.metricsSupplier = () -> "N/A (unit-test mode)";

        log.info("KafkaTestFacade initialized in unit-test mode [groupId={}]", consumerGroupId);
    }

    // ═══════════════════════════════════════════════════════════════════════════
    // Publishing API
    // ═══════════════════════════════════════════════════════════════════════════

    public PublishResult publish(String topicName, String key, String content) {
        return publishingService.publishMessage(Message.builder()
                .topic(Topic.builder().name(topicName).build())
                .key(key).content(content).build());
    }

    public PublishResult publish(String topicName, String key, String content, Map<String, String> headers) {
        return publishingService.publishMessage(Message.builder()
                .topic(Topic.builder().name(topicName).build())
                .key(key).content(content).headers(headers).build());
    }

    public PublishResult publish(Message message) {
        return publishingService.publishMessage(message);
    }

    public PublishResult publishWithRetry(Message message, int maxRetries) {
        return publishingService.publishWithRetry(message, maxRetries);
    }

    public List<PublishResult> publishBatch(List<Message> messages) {
        return publishingService.publishBatch(messages);
    }

    public void flush() {
        publishingService.flush();
    }

    // ═══════════════════════════════════════════════════════════════════════════
    // Consumption API
    // ═══════════════════════════════════════════════════════════════════════════

    public void subscribe(String topicName) {
        consumptionService.subscribeToTopic(Topic.builder().name(topicName).build());
    }

    public void subscribe(String... topicNames) {
        Set<Topic> topics = Set.of(topicNames).stream()
                .map(n -> Topic.builder().name(n).build())
                .collect(Collectors.toSet());
        consumptionService.subscribeToTopics(topics);
    }

    public ConsumeResult poll(Duration timeout) {
        return consumptionService.consumeMessages(timeout);
    }

    public ConsumeResult pollMessages(int expectedCount, Duration timeout) {
        return consumptionService.consumeExpectedMessages(expectedCount, timeout);
    }

    public ConsumeResult consumeAll(int maxMessages, Duration timeout) {
        return consumptionService.consumeAllMessages(maxMessages, timeout);
    }

    /**
     * Safe variant — returns {@link Optional#empty()} if no match found.
     */
    public Optional<Message> consumeUntil(
            Predicate<Message> condition,
            String conditionDescription,
            int maxAttempts,
            Duration pollTimeout) {
        return consumptionService.consumeUntil(condition, conditionDescription, maxAttempts, pollTimeout);
    }

    /**
     * Strict variant — throws {@link MessageNotFoundException} if no match found.
     */
    public Message consumeUntilOrThrow(
            Predicate<Message> condition,
            String conditionDescription,
            int maxAttempts,
            Duration pollTimeout) {
        return consumptionService.consumeUntilOrThrow(condition, conditionDescription, maxAttempts, pollTimeout);
    }

    public void seekToBeginning() {
        consumptionService.seekToBeginning();
    }

    public void seekToEnd() {
        consumptionService.seekToEnd();
    }

    public void commitSync() {
        consumptionService.commitOffsets();
    }

    public void commitOffset(String topicName, int partition, long offset) {
        consumptionService.seekToOffset(Topic.builder().name(topicName).build(), partition, offset + 1);
        consumptionService.commitOffsets();
    }

    // ═══════════════════════════════════════════════════════════════════════════
    // Topic Management API
    // ═══════════════════════════════════════════════════════════════════════════

    public boolean createTopic(String topicName) {
        return topicManagementService.createTopic(Topic.builder().name(topicName).build());
    }

    public boolean createTopic(String topicName, int partitions) {
        return topicManagementService.createTopic(
                Topic.builder().name(topicName).partitionCount(partitions).build());
    }

    public boolean createTopic(Topic topic) {
        return topicManagementService.createTopic(topic);
    }

    public boolean createTopicWithDlq(String topicName) {
        return topicManagementService.createTopicWithDlq(Topic.builder().name(topicName).build());
    }

    public boolean deleteTopic(String topicName) {
        return topicManagementService.deleteTopic(Topic.builder().name(topicName).build());
    }

    public int deleteTestTopics() {
        return topicManagementService.deleteTestTopics();
    }

    public boolean topicExists(String topicName) {
        return topicManagementService.topicExists(Topic.builder().name(topicName).build());
    }

    public boolean waitForTopic(String topicName, int timeoutSeconds) {
        return topicManagementService.waitForTopic(Topic.builder().name(topicName).build(), timeoutSeconds);
    }

    // ═══════════════════════════════════════════════════════════════════════════
    // Lifecycle
    // ═══════════════════════════════════════════════════════════════════════════

    /**
     * Closes resources for the current thread. Call in {@code @AfterEach}.
     */
    @Override
    public void close() {
        publishingService.close();
        consumptionService.close();
        topicManagementService.close();
    }

    /**
     * Closes ALL thread-local Kafka clients across all threads.
     * Call in {@code @AfterAll}. No-op in unit-test mode.
     */
    public void closeAll() {
        log.info("Closing ALL KafkaTestFacade resources");
        closeAllAction.run();
        log.info("All KafkaTestFacade resources closed");
    }

    // ═══════════════════════════════════════════════════════════════════════════
    // Metrics
    // ═══════════════════════════════════════════════════════════════════════════

    /**
     * Returns tracked producer/consumer client counts.
     * Returns {@code "N/A (unit-test mode)"} when mocks are injected.
     */
    public String getMetrics() {
        return metricsSupplier.get();
    }
}
