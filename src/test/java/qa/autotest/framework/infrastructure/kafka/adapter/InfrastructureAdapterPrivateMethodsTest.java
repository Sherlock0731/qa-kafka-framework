package qa.autotest.framework.infrastructure.kafka.adapter;

import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.common.ConsumerGroupState;
import org.apache.kafka.common.header.Header;
import org.junit.jupiter.api.*;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.EnumSource;
import qa.autotest.framework.domain.model.ConsumerGroup;
import qa.autotest.framework.domain.model.Message;
import qa.autotest.framework.domain.model.Topic;

import java.lang.reflect.Method;
import java.nio.charset.StandardCharsets;
import java.util.HashMap;
import java.util.Map;

import static org.assertj.core.api.Assertions.*;

/**
 * Unit-тесты для двух приватных методов инфраструктурных адаптеров:
 * <p>
 * 1. KafkaProducerAdapter.toProducerRecord(Message) — маппинг доменного Message
 * в Kafka ProducerRecord. Тестируется через reflection, т.к. метод private.
 * <p>
 * 2. KafkaConsumerAdapter.mapGroupState(ConsumerGroupState) — маппинг Kafka enum
 * в доменный GroupState. Тестируется через reflection.
 * <p>
 * Альтернатива рефлекции: сделать методы package-private и добавить
 * тест в тот же пакет. Рефлексия выбрана здесь чтобы не менять
 * сигнатуры production-кода.
 */
@DisplayName("Infrastructure Adapter — private methods")
class InfrastructureAdapterPrivateMethodsTest {

    // ═══════════════════════════════════════════════════════════════════════
    // KafkaProducerAdapter.toProducerRecord
    // ═══════════════════════════════════════════════════════════════════════

    @Nested
    @DisplayName("KafkaProducerAdapter.toProducerRecord")
    class ToProducerRecordTests {

        private Method toProducerRecord;
        private KafkaProducerAdapter adapter;

        /**
         * Создаём адаптер через рефлексию без подключения к Kafka.
         * KafkaProducerAdapter создаёт ThreadLocal с ленивой инициализацией,
         * поэтому сам объект создаётся без сетевых вызовов.
         * Мы никогда не вызываем getProducer() — только приватный маппер.
         */
        @BeforeEach
        void setUp() throws Exception {
            // Нам нужен KafkaConfig mock-объект — используем анонимный прокси
            // чтобы не тянуть Mockito в этот nested class (уже используется
            // в других тестах, но здесь хотим подчеркнуть чистоту)
            // Используем spy-рефлексию напрямую

            toProducerRecord = KafkaProducerAdapter.class
                    .getDeclaredMethod("toProducerRecord", Message.class);
            toProducerRecord.setAccessible(true);

            // Создаём адаптер через рефлексию — без реального KafkaConfig
            // (конструктор только сохраняет config в поле, не подключается)
            // Используем минимальный mock KafkaConfig
            var configClass = qa.autotest.framework.config.KafkaConfig.class;
            adapter = null; // см. вспомогательный метод ниже
        }

        /**
         * Вызывает toProducerRecord через рефлексию на заранее созданном
         * адаптере из вспомогательного метода.
         */
        @SuppressWarnings("unchecked")
        private ProducerRecord<String, String> toRecord(Message msg) throws Exception {
            // Ленивый способ: создаём адаптер через Mockito в рамках теста
            var config = org.mockito.Mockito.mock(qa.autotest.framework.config.KafkaConfig.class);
            org.mockito.Mockito.when(config.kafkaBootstrapServers()).thenReturn("localhost:9092");
            org.mockito.Mockito.when(config.securityProtocol()).thenReturn("PLAINTEXT");
            org.mockito.Mockito.when(config.producerAcks()).thenReturn("all");
            org.mockito.Mockito.when(config.producerRetries()).thenReturn(3);
            org.mockito.Mockito.when(config.producerEnableIdempotence()).thenReturn(true);
            org.mockito.Mockito.when(config.producerMaxInFlightRequests()).thenReturn(5);
            org.mockito.Mockito.when(config.producerBatchSize()).thenReturn(16384);
            org.mockito.Mockito.when(config.producerLingerMs()).thenReturn(10);
            org.mockito.Mockito.when(config.producerRequestTimeoutMs()).thenReturn(30000);

            KafkaProducerAdapter a = new KafkaProducerAdapter(config);
            Method m = KafkaProducerAdapter.class
                    .getDeclaredMethod("toProducerRecord", Message.class);
            m.setAccessible(true);
            return (ProducerRecord<String, String>) m.invoke(a, msg);
        }

        @Test
        @DisplayName("topic name корректно маппируется в ProducerRecord")
        void shouldMapTopicName() throws Exception {
            Message msg = Message.builder()
                    .topic(Topic.builder().name("qa-test-orders").build())
                    .key("order-1")
                    .content("{\"id\":1}")
                    .build();

            ProducerRecord<String, String> record = toRecord(msg);

            assertThat(record.topic()).isEqualTo("qa-test-orders");
        }

        @Test
        @DisplayName("key и value корректно маппируются")
        void shouldMapKeyAndValue() throws Exception {
            Message msg = Message.builder()
                    .topic(Topic.builder().name("qa-test-t").build())
                    .key("order-key")
                    .content("{\"amount\":100}")
                    .build();

            ProducerRecord<String, String> record = toRecord(msg);

            assertThat(record.key()).isEqualTo("order-key");
            assertThat(record.value()).isEqualTo("{\"amount\":100}");
        }

        @Test
        @DisplayName("message-id всегда добавляется в headers")
        void shouldAlwaysAddMessageIdHeader() throws Exception {
            Message msg = Message.builder()
                    .topic(Topic.builder().name("t").build())
                    .key("k")
                    .content("v")
                    .build();

            ProducerRecord<String, String> record = toRecord(msg);

            Header messageIdHeader = record.headers().lastHeader("message-id");
            assertThat(messageIdHeader).isNotNull();
            assertThat(new String(messageIdHeader.value(), StandardCharsets.UTF_8))
                    .isEqualTo(msg.getMessageId());
        }

        @Test
        @DisplayName("correlation-id добавляется только при isCorrelated()==true")
        void shouldAddCorrelationIdOnlyWhenPresent() throws Exception {
            Message withCorr = Message.builder()
                    .topic(Topic.builder().name("t").build())
                    .key("k").content("v")
                    .correlationId("corr-abc")
                    .build();
            Message withoutCorr = Message.builder()
                    .topic(Topic.builder().name("t").build())
                    .key("k").content("v")
                    .build();

            ProducerRecord<String, String> withCorrRecord = toRecord(withCorr);
            ProducerRecord<String, String> withoutCorrRecord = toRecord(withoutCorr);

            assertThat(withCorrRecord.headers().lastHeader("correlation-id")).isNotNull();
            assertThat(withoutCorrRecord.headers().lastHeader("correlation-id")).isNull();
        }

        @Test
        @DisplayName("event-type добавляется только при isEvent()==true")
        void shouldAddEventTypeOnlyWhenPresent() throws Exception {
            Message event = Message.builder()
                    .topic(Topic.builder().name("t").build())
                    .key("k").content("v")
                    .eventType("ORDER_CREATED")
                    .build();
            Message plain = Message.builder()
                    .topic(Topic.builder().name("t").build())
                    .key("k").content("v")
                    .build();

            assertThat(toRecord(event).headers().lastHeader("event-type")).isNotNull();
            assertThat(toRecord(plain).headers().lastHeader("event-type")).isNull();
        }

        @Test
        @DisplayName("кастомные headers добавляются в ProducerRecord")
        void shouldAddCustomHeaders() throws Exception {
            Message msg = Message.builder()
                    .topic(Topic.builder().name("t").build())
                    .key("k").content("v")
                    .headers(Map.of("x-trace-id", "trace-123", "x-tenant", "acme"))
                    .build();

            ProducerRecord<String, String> record = toRecord(msg);

            Header traceHeader = record.headers().lastHeader("x-trace-id");
            Header tenantHeader = record.headers().lastHeader("x-tenant");

            assertThat(traceHeader).isNotNull();
            assertThat(new String(traceHeader.value(), StandardCharsets.UTF_8)).isEqualTo("trace-123");
            assertThat(tenantHeader).isNotNull();
        }

        @Test
        @DisplayName("явный partition маппируется в ProducerRecord.partition()")
        void shouldMapExplicitPartition() throws Exception {
            Message msg = Message.builder()
                    .topic(Topic.builder().name("t").build())
                    .key("k").content("v")
                    .partition(2)
                    .build();

            ProducerRecord<String, String> record = toRecord(msg);

            assertThat(record.partition()).isEqualTo(2);
        }

        @Test
        @DisplayName("null partition маппируется в ProducerRecord.partition()==null")
        void shouldMapNullPartition() throws Exception {
            Message msg = Message.builder()
                    .topic(Topic.builder().name("t").build())
                    .key("k").content("v")
                    // partition не выставлен → null
                    .build();

            ProducerRecord<String, String> record = toRecord(msg);

            assertThat(record.partition()).isNull();
        }
    }

    // ═══════════════════════════════════════════════════════════════════════
    // KafkaConsumerAdapter.mapGroupState
    // ═══════════════════════════════════════════════════════════════════════

    @Nested
    @DisplayName("KafkaConsumerAdapter.mapGroupState")
    class MapGroupStateTests {

        private Method mapGroupState;

        @BeforeEach
        void setUp() throws Exception {
            mapGroupState = KafkaConsumerAdapter.class
                    .getDeclaredMethod("mapGroupState", ConsumerGroupState.class);
            mapGroupState.setAccessible(true);
        }

        private ConsumerGroup.GroupState invoke(ConsumerGroupState kafkaState) throws Exception {
            // Нужен экземпляр, но метод не использует поля — создаём через mock config
            var config = org.mockito.Mockito.mock(qa.autotest.framework.config.KafkaConfig.class);
            org.mockito.Mockito.when(config.kafkaBootstrapServers()).thenReturn("localhost:9092");
            org.mockito.Mockito.when(config.securityProtocol()).thenReturn("PLAINTEXT");
            org.mockito.Mockito.when(config.consumerAutoOffsetReset()).thenReturn("earliest");
            org.mockito.Mockito.when(config.consumerEnableAutoCommit()).thenReturn(false);
            org.mockito.Mockito.when(config.consumerMaxPollRecords()).thenReturn(500);
            org.mockito.Mockito.when(config.consumerSessionTimeoutMs()).thenReturn(30000);

            KafkaConsumerAdapter adapter = new KafkaConsumerAdapter(config, "test-group");
            return (ConsumerGroup.GroupState) mapGroupState.invoke(adapter, kafkaState);
        }

        @Test
        @DisplayName("STABLE → GroupState.STABLE")
        void shouldMapStable() throws Exception {
            assertThat(invoke(ConsumerGroupState.STABLE))
                    .isEqualTo(ConsumerGroup.GroupState.STABLE);
        }

        @Test
        @DisplayName("PREPARING_REBALANCE → GroupState.PREPARING_REBALANCE")
        void shouldMapPreparingRebalance() throws Exception {
            assertThat(invoke(ConsumerGroupState.PREPARING_REBALANCE))
                    .isEqualTo(ConsumerGroup.GroupState.PREPARING_REBALANCE);
        }

        @Test
        @DisplayName("COMPLETING_REBALANCE → GroupState.COMPLETING_REBALANCE")
        void shouldMapCompletingRebalance() throws Exception {
            assertThat(invoke(ConsumerGroupState.COMPLETING_REBALANCE))
                    .isEqualTo(ConsumerGroup.GroupState.COMPLETING_REBALANCE);
        }

        @Test
        @DisplayName("EMPTY → GroupState.EMPTY")
        void shouldMapEmpty() throws Exception {
            assertThat(invoke(ConsumerGroupState.EMPTY))
                    .isEqualTo(ConsumerGroup.GroupState.EMPTY);
        }

        @Test
        @DisplayName("DEAD → GroupState.DEAD")
        void shouldMapDead() throws Exception {
            assertThat(invoke(ConsumerGroupState.DEAD))
                    .isEqualTo(ConsumerGroup.GroupState.DEAD);
        }

        @Test
        @DisplayName("null → GroupState.DEAD (защитная ветка)")
        void shouldMapNullToDead() throws Exception {
            assertThat(invoke(null))
                    .isEqualTo(ConsumerGroup.GroupState.DEAD);
        }

        /**
         * Параметризованный тест: убеждаемся что ни одно значение ConsumerGroupState
         * не вызывает исключения при маппинге — защита от добавления новых enum-значений.
         */
        @ParameterizedTest(name = "kafkaState={0}")
        @EnumSource(ConsumerGroupState.class)
        @DisplayName("Все ConsumerGroupState.values() маппируются без исключения")
        void shouldMapAllStatesWithoutException(ConsumerGroupState state) {
            assertThatNoException().isThrownBy(() -> invoke(state));
        }
    }
}
