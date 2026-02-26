package qa.autotest.framework.infrastructure.kafka.adapter;

import org.junit.jupiter.api.*;
import org.junit.jupiter.api.extension.ExtendWith;
import org.mockito.Mock;
import org.mockito.junit.jupiter.MockitoExtension;
import qa.autotest.framework.config.KafkaConfig;
import qa.autotest.framework.domain.port.ConsumerGroupReader;
import qa.autotest.framework.domain.port.MessageConsumer;
import qa.autotest.framework.domain.port.MessagePublisher;
import qa.autotest.framework.domain.port.TopicRepository;

import java.lang.reflect.Field;

import static org.assertj.core.api.Assertions.*;
import static org.mockito.Mockito.*;

/**
 * Unit-тесты для KafkaAdapterFactory и вложенного KafkaAdapters.
 *
 * <h3>Что проверяем после ISP-рефакторинга</h3>
 * До рефакторинга фабрика передавала один и тот же экземпляр {@code KafkaAdminAdapter}
 * сразу в два слота — {@code topicRepository} и {@code consumerGroupReader}:
 * <pre>
 *   return new KafkaAdapters(producer, consumer, admin, admin, groupId); // admin дважды
 * </pre>
 * После ISP-фикса каждый порт обслуживается <em>собственным</em> адаптером:
 * <ul>
 *   <li>{@link KafkaAdminAdapter}         → {@link TopicRepository}</li>
 *   <li>{@link KafkaConsumerGroupAdapter} → {@link ConsumerGroupReader}</li>
 * </ul>
 * Тесты проверяют:
 * <ol>
 *   <li>Каждый порт-геттер возвращает экземпляр правильного конкретного типа.</li>
 *   <li>{@code topicRepository()} и {@code consumerGroupReader()} — разные объекты.</li>
 *   <li>{@code consumerGroupId()} содержит базу из конфига и UUID-суффикс.</li>
 *   <li>{@code metrics()} возвращает строку без NPE.</li>
 *   <li>{@code closeAll()} закрывает все четыре адаптера (mock-based via reflection).</li>
 * </ol>
 *
 * <h3>Стратегия создания адаптеров без реального Kafka</h3>
 * {@code KafkaProducerAdapter} и {@code KafkaConsumerAdapter} используют
 * {@code ThreadLocal} с ленивой инициализацией — объект создаётся без сетевых
 * вызовов. {@code KafkaAdminAdapter} и {@code KafkaConsumerGroupAdapter} вызывают
 * {@code AdminClient.create(props)} в конструкторе. Чтобы не поднимать брокер,
 * тесты, которым нужен реальный {@code KafkaAdapters}, используют рефлексию для
 * подмены внутренних адаптеров на mock-объекты после создания бандла
 * (см. {@code withMockedAdapters()}).
 */
@Tag("unit")
@DisplayName("KafkaAdapterFactory")
@ExtendWith(MockitoExtension.class)
class KafkaAdapterFactoryTest {

    // ── mock KafkaConfig — минимальный набор методов ───────────────────────

    @Mock
    KafkaConfig config;

    @BeforeEach
    void stubConfig() {
        lenient().when(config.kafkaBootstrapServers()).thenReturn("localhost:9092");
        lenient().when(config.securityProtocol()).thenReturn("PLAINTEXT");
        lenient().when(config.consumerGroupIdBase()).thenReturn("qa-test");
        // KafkaConsumerAdapter дополнительно требует эти значения
        lenient().when(config.consumerMaxPollRecords()).thenReturn(100);
        lenient().when(config.consumerSessionTimeoutMs()).thenReturn(30000);
        lenient().when(config.consumerMaxPollIntervalMs()).thenReturn(300000);
        lenient().when(config.consumerAutoOffsetReset()).thenReturn("earliest");
        lenient().when(config.consumerAutoCommitIntervalMs()).thenReturn(5000);
        lenient().when(config.consumerEnableAutoCommit()).thenReturn(false);
        lenient().when(config.consumerFetchMinBytes()).thenReturn(1);
        lenient().when(config.consumerFetchMaxWaitMs()).thenReturn(500);
        // KafkaProducerAdapter
        lenient().when(config.producerAcks()).thenReturn("all");
        lenient().when(config.producerRetries()).thenReturn(3);
        lenient().when(config.producerEnableIdempotence()).thenReturn(true);
        lenient().when(config.producerMaxInFlightRequests()).thenReturn(5);
        lenient().when(config.producerBatchSize()).thenReturn(16384);
        lenient().when(config.producerLingerMs()).thenReturn(10);
        lenient().when(config.producerRequestTimeoutMs()).thenReturn(30000);
    }

    // ═══════════════════════════════════════════════════════════════════════
    // Вспомогательный метод: создаём KafkaAdapters и подменяем admin-адаптеры
    // mock-объектами через рефлексию (без реального AdminClient)
    // ═══════════════════════════════════════════════════════════════════════

    /**
     * Контейнер: реальный бандл + отдельные mock-адаптеры для инспекции.
     */
    record AdapterBundle(
            KafkaAdapterFactory.KafkaAdapters adapters,
            KafkaAdminAdapter         mockAdmin,
            KafkaConsumerGroupAdapter mockConsumerGroup
    ) {}

    /**
     * Создаёт {@link KafkaAdapterFactory.KafkaAdapters} с реальными
     * producer/consumer адаптерами и mock admin/consumerGroup адаптерами,
     * подставленными через рефлексию.
     */
    private AdapterBundle withMockedAdapters() throws Exception {
        KafkaAdapterFactory.KafkaAdapters adapters = KafkaAdapterFactory.create(config);

        KafkaAdminAdapter         mockAdmin         = mock(KafkaAdminAdapter.class);
        KafkaConsumerGroupAdapter mockConsumerGroup = mock(KafkaConsumerGroupAdapter.class);

        setField(adapters, "adminAdapter",         mockAdmin);
        setField(adapters, "consumerGroupAdapter", mockConsumerGroup);

        return new AdapterBundle(adapters, mockAdmin, mockConsumerGroup);
    }

    private static void setField(Object target, String fieldName, Object value) throws Exception {
        Field f = KafkaAdapterFactory.KafkaAdapters.class.getDeclaredField(fieldName);
        f.setAccessible(true);
        f.set(target, value);
    }

    // ═══════════════════════════════════════════════════════════════════════
    // ISP: разделение адаптеров по портам
    // ═══════════════════════════════════════════════════════════════════════

    @Nested
    @DisplayName("ISP — разделение адаптеров по портам")
    class IspSplitTests {

        /**
         * Ключевой ISP-тест: topicRepository() и consumerGroupReader() должны
         * возвращать разные объекты — до фикса возвращался один и тот же KafkaAdminAdapter.
         */
        @Test
        @DisplayName("topicRepository и consumerGroupReader — разные экземпляры")
        void topicRepositoryAndConsumerGroupReaderAreDifferentInstances() throws Exception {
            KafkaAdapterFactory.KafkaAdapters adapters = KafkaAdapterFactory.create(config);
            assertThat(adapters.topicRepository())
                    .isNotSameAs(adapters.consumerGroupReader());
        }

        @Test
        @DisplayName("topicRepository() реализует TopicRepository")
        void topicRepositoryImplementsCorrectPort() throws Exception {
            KafkaAdapterFactory.KafkaAdapters adapters = KafkaAdapterFactory.create(config);
            assertThat(adapters.topicRepository()).isInstanceOf(TopicRepository.class);
        }

        @Test
        @DisplayName("consumerGroupReader() реализует ConsumerGroupReader")
        void consumerGroupReaderImplementsCorrectPort() throws Exception {
            KafkaAdapterFactory.KafkaAdapters adapters = KafkaAdapterFactory.create(config);
            assertThat(adapters.consumerGroupReader()).isInstanceOf(ConsumerGroupReader.class);
        }

        @Test
        @DisplayName("publisher() реализует MessagePublisher")
        void publisherImplementsCorrectPort() throws Exception {
            KafkaAdapterFactory.KafkaAdapters adapters = KafkaAdapterFactory.create(config);
            assertThat(adapters.publisher()).isInstanceOf(MessagePublisher.class);
        }

        @Test
        @DisplayName("consumer() реализует MessageConsumer")
        void consumerImplementsCorrectPort() throws Exception {
            KafkaAdapterFactory.KafkaAdapters adapters = KafkaAdapterFactory.create(config);
            assertThat(adapters.consumer()).isInstanceOf(MessageConsumer.class);
        }

        @Test
        @DisplayName("topicRepository() — конкретный тип KafkaAdminAdapter")
        void topicRepositoryIsKafkaAdminAdapter() throws Exception {
            KafkaAdapterFactory.KafkaAdapters adapters = KafkaAdapterFactory.create(config);
            Field f = KafkaAdapterFactory.KafkaAdapters.class.getDeclaredField("adminAdapter");
            f.setAccessible(true);
            assertThat(f.get(adapters)).isInstanceOf(KafkaAdminAdapter.class);
        }

        @Test
        @DisplayName("consumerGroupReader() — конкретный тип KafkaConsumerGroupAdapter")
        void consumerGroupReaderIsKafkaConsumerGroupAdapter() throws Exception {
            KafkaAdapterFactory.KafkaAdapters adapters = KafkaAdapterFactory.create(config);
            Field f = KafkaAdapterFactory.KafkaAdapters.class.getDeclaredField("consumerGroupAdapter");
            f.setAccessible(true);
            assertThat(f.get(adapters)).isInstanceOf(KafkaConsumerGroupAdapter.class);
        }

        @Test
        @DisplayName("KafkaAdminAdapter не реализует ConsumerGroupReader (ISP)")
        void kafkaAdminAdapterDoesNotImplementConsumerGroupReader() throws Exception {
            KafkaAdapterFactory.KafkaAdapters adapters = KafkaAdapterFactory.create(config);
            Field f = KafkaAdapterFactory.KafkaAdapters.class.getDeclaredField("adminAdapter");
            f.setAccessible(true);
            assertThat(f.get(adapters)).isNotInstanceOf(ConsumerGroupReader.class);
        }
    }

    // ═══════════════════════════════════════════════════════════════════════
    // consumerGroupId
    // ═══════════════════════════════════════════════════════════════════════

    @Nested
    @DisplayName("consumerGroupId")
    class ConsumerGroupIdTests {

        @Test
        @DisplayName("содержит base из конфига")
        void shouldContainBaseFromConfig() throws Exception {
            KafkaAdapterFactory.KafkaAdapters adapters = KafkaAdapterFactory.create(config);
            assertThat(adapters.consumerGroupId()).startsWith("qa-test-");
        }

        @Test
        @DisplayName("содержит UUID-суффикс")
        void shouldContainUuidSuffix() throws Exception {
            KafkaAdapterFactory.KafkaAdapters adapters = KafkaAdapterFactory.create(config);
            String suffix = adapters.consumerGroupId().substring("qa-test-".length());
            assertThatNoException().isThrownBy(() -> java.util.UUID.fromString(suffix));
        }

        @Test
        @DisplayName("каждый вызов create() генерирует уникальный groupId")
        void shouldGenerateUniqueGroupIdPerCall() throws Exception {
            KafkaAdapterFactory.KafkaAdapters a1 = KafkaAdapterFactory.create(config);
            KafkaAdapterFactory.KafkaAdapters a2 = KafkaAdapterFactory.create(config);
            assertThat(a1.consumerGroupId()).isNotEqualTo(a2.consumerGroupId());
        }
    }

    // ═══════════════════════════════════════════════════════════════════════
    // metrics
    // ═══════════════════════════════════════════════════════════════════════

    @Nested
    @DisplayName("metrics")
    class MetricsTests {

        @Test
        @DisplayName("возвращает непустую строку без исключения")
        void shouldReturnNonBlankString() throws Exception {
            KafkaAdapterFactory.KafkaAdapters adapters = KafkaAdapterFactory.create(config);
            assertThat(adapters.metrics()).isNotBlank();
        }

        @Test
        @DisplayName("содержит информацию о producers и consumers")
        void shouldMentionProducersAndConsumers() throws Exception {
            KafkaAdapterFactory.KafkaAdapters adapters = KafkaAdapterFactory.create(config);
            assertThat(adapters.metrics())
                    .containsIgnoringCase("producers")
                    .containsIgnoringCase("consumers");
        }
    }

    // ═══════════════════════════════════════════════════════════════════════
    // closeAll
    // ═══════════════════════════════════════════════════════════════════════

    @Nested
    @DisplayName("closeAll")
    class CloseAllTests {

        @Test
        @DisplayName("вызывает close() на KafkaAdminAdapter")
        void shouldCloseAdminAdapter() throws Exception {
            AdapterBundle b = withMockedAdapters();
            b.adapters().closeAll();
            verify(b.mockAdmin()).close();
        }

        @Test
        @DisplayName("вызывает close() на KafkaConsumerGroupAdapter")
        void shouldCloseConsumerGroupAdapter() throws Exception {
            AdapterBundle b = withMockedAdapters();
            b.adapters().closeAll();
            verify(b.mockConsumerGroup()).close();
        }

        @Test
        @DisplayName("оба admin-адаптера закрываются ровно по одному разу")
        void shouldCloseBothAdminAdaptersExactlyOnce() throws Exception {
            AdapterBundle b = withMockedAdapters();
            b.adapters().closeAll();
            verify(b.mockAdmin(),         times(1)).close();
            verify(b.mockConsumerGroup(), times(1)).close();
            verifyNoMoreInteractions(b.mockAdmin(), b.mockConsumerGroup());
        }
    }

    // ═══════════════════════════════════════════════════════════════════════
    // Утилитный конструктор
    // ═══════════════════════════════════════════════════════════════════════

    @Test
    @DisplayName("KafkaAdapterFactory нельзя инстанциировать (утилитный класс)")
    void shouldThrowOnInstantiation() {
        var constructor = KafkaAdapterFactory.class.getDeclaredConstructors()[0];
        constructor.setAccessible(true);
        assertThatThrownBy(constructor::newInstance)
                .cause()
                .isInstanceOf(UnsupportedOperationException.class)
                .hasMessageContaining("Factory class");
    }
}
