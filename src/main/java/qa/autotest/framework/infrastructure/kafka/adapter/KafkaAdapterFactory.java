package qa.autotest.framework.infrastructure.kafka.adapter;

import lombok.extern.slf4j.Slf4j;
import qa.autotest.framework.config.KafkaConfig;
import qa.autotest.framework.domain.port.MessageConsumer;
import qa.autotest.framework.domain.port.MessagePublisher;
import qa.autotest.framework.domain.port.TopicRepository;

import java.util.UUID;

/**
 * Factory: KafkaAdapterFactory (Infrastructure layer)
 * <p>
 * The <strong>only</strong> class in the codebase that instantiates concrete
 * Kafka adapters ({@link KafkaProducerAdapter}, {@link KafkaConsumerAdapter},
 * {@link KafkaAdminAdapter}).  All other classes depend on port interfaces or
 * on the {@link KafkaAdapters} bundle — never on the adapter classes directly.
 * <p>
 * <strong>DIP fix:</strong> previously {@code KafkaTestFacade} (Application)
 * called {@code new KafkaProducerAdapter(...)} directly, coupling it to
 * Infrastructure.  Now the facade receives only port interfaces and two
 * lambdas ({@code closeAll}, {@code metrics}), so it has zero imports of
 * concrete adapter classes.
 */
@Slf4j
public final class KafkaAdapterFactory {

    private KafkaAdapterFactory() {
        throw new UnsupportedOperationException("Factory class — use static methods");
    }

    /**
     * Creates a fully-wired set of Kafka adapters for the given configuration.
     * A fresh UUID consumer-group ID is generated per call to guarantee
     * per-facade test isolation.
     *
     * @param config validated {@link KafkaConfig}
     * @return wired adapters bundled in {@link KafkaAdapters}
     */
    public static KafkaAdapters create(KafkaConfig config) {
        String groupId = config.consumerGroupIdBase() + "-" + UUID.randomUUID();
        log.debug("KafkaAdapterFactory: creating adapters [groupId={}]", groupId);

        KafkaProducerAdapter producer = new KafkaProducerAdapter(config);
        KafkaConsumerAdapter consumer = new KafkaConsumerAdapter(config, groupId);
        KafkaAdminAdapter admin = new KafkaAdminAdapter(config);

        log.debug("KafkaAdapterFactory: adapters created");
        return new KafkaAdapters(producer, consumer, admin, groupId);
    }

    /**
     * Immutable bundle produced by {@link KafkaAdapterFactory#create}.
     * <p>
     * Exposes adapters <em>only</em> as port interfaces to callers in the
     * Application layer.  Lifecycle ({@link #closeAll()}) and metrics
     * ({@link #metrics()}) are provided as ready-to-call methods so that
     * the Application layer never needs to import a concrete adapter class.
     */
    public static final class KafkaAdapters {

        private final KafkaProducerAdapter producerAdapter;
        private final KafkaConsumerAdapter consumerAdapter;
        private final KafkaAdminAdapter adminAdapter;
        private final String consumerGroupId;

        private KafkaAdapters(
                KafkaProducerAdapter producerAdapter,
                KafkaConsumerAdapter consumerAdapter,
                KafkaAdminAdapter adminAdapter,
                String consumerGroupId) {

            this.producerAdapter = producerAdapter;
            this.consumerAdapter = consumerAdapter;
            this.adminAdapter = adminAdapter;
            this.consumerGroupId = consumerGroupId;
        }

        /**
         * Port view — the only view Application layer should use.
         */
        public MessagePublisher publisher() {
            return producerAdapter;
        }

        /**
         * Port view — the only view Application layer should use.
         */
        public MessageConsumer consumer() {
            return consumerAdapter;
        }

        /**
         * Port view — the only view Application layer should use.
         */
        public TopicRepository topicRepository() {
            return adminAdapter;
        }

        /**
         * Unique consumer group ID generated for this bundle.
         */
        public String consumerGroupId() {
            return consumerGroupId;
        }

        /**
         * Closes ALL thread-local Kafka clients across all threads.
         * Call this from {@code @AfterAll} via {@code KafkaTestFacade.closeAll()}.
         */
        public void closeAll() {
            producerAdapter.closeAll();
            consumerAdapter.closeAll();
            adminAdapter.close();
        }

        /**
         * Human-readable metrics string (tracked client counts).
         */
        public String metrics() {
            return String.format("Producers tracked: %d, Consumers tracked: %d",
                    producerAdapter.getTrackedProducerCount(),
                    consumerAdapter.getTrackedConsumerCount());
        }
    }
}
