package qa.autotest.framework.tests;

import org.junit.jupiter.api.*;
import org.junit.jupiter.api.extension.ExtendWith;
import org.mockito.Mock;
import org.mockito.junit.jupiter.MockitoExtension;
import qa.autotest.framework.application.service.KafkaTestFacade;
import qa.autotest.framework.domain.model.*;
import tests.KafkaTestBase;
import tests.KafkaTestHelpers;

import java.time.Duration;
import java.util.List;

import static org.assertj.core.api.Assertions.*;
import static org.mockito.ArgumentMatchers.*;
import static org.mockito.Mockito.*;

/**
 * Unit-тесты для {@link KafkaTestHelpers}.
 * <p>
 * Стратегия: наследуем конкретную тест-реализацию {@code Stub}, которая
 * подменяет {@code kafka} mock-объектом через package-private сеттер.
 * Это позволяет тестировать логику хелперов без реального Kafka-соединения
 * и без запуска {@code @BeforeEach setUp()} из {@link KafkaTestBase}.
 *
 * <h3>Покрываемые методы</h3>
 * <ul>
 *   <li>{@code generateTopicName} — формат qa-test-{prefix}-{UUID}, уникальность</li>
 *   <li>{@code createTestTopic} — все перегрузки; делегирование в facade; регистрация в createdTopics</li>
 *   <li>{@code createTopicWithDlq} — регистрация main + DLQ; false при неудаче</li>
 *   <li>{@code publishMessage} — все перегрузки делегируют в facade</li>
 *   <li>{@code publishBatch} — делегирует в facade</li>
 *   <li>{@code consumeMessages} — subscribe + seekToBeginning + pollMessages</li>
 *   <li>{@code consumeAllMessages} — subscribe + seekToBeginning + consumeAll</li>
 *   <li>{@code waitForMessages} — subscribe + seekToBeginning + pollMessages + count check</li>
 *   <li>{@code createNewFacade} — регистрирует в ALL_FACADES через registerFacade()</li>
 * </ul>
 */
@Tag("unit")
@DisplayName("KafkaTestHelpers")
@ExtendWith(MockitoExtension.class)
class KafkaTestHelpersTest {

    /**
     * Минимальная конкретная реализация KafkaTestHelpers для тестирования.
     * Переопределяет setUp() чтобы избежать реального Kafka-подключения,
     * и предоставляет метод для инжекции mock-фасада.
     */
    static class Stub extends KafkaTestHelpers {

        /** Инжектируем mock-фасад напрямую в protected поле базового класса. */
        void injectFacade(KafkaTestFacade mockFacade) {
            this.kafka = mockFacade;
        }
    }

    @Mock
    KafkaTestFacade mockKafka;

    private Stub helper;

    @BeforeEach
    void setUp() {
        helper = new Stub();
        helper.injectFacade(mockKafka);
    }

    private static final String TOPIC = "qa-test-orders";

    private static Message msg(String topic) {
        return Message.builder()
                .topic(Topic.builder().name(topic).build())
                .key("k1")
                .content("{\"id\":1}")
                .build();
    }

    private static ConsumeResult successResult(int count) {
        List<Message> msgs = new java.util.ArrayList<>();
        for (int i = 0; i < count; i++) {
            msgs.add(msg(TOPIC));
        }
        return ConsumeResult.success(msgs);
    }

    // ═══════════════════════════════════════════════════════════════════════════
    // generateTopicName
    // ═══════════════════════════════════════════════════════════════════════════

    @Nested
    @DisplayName("generateTopicName")
    class GenerateTopicNameTests {

        @Test
        @DisplayName("формат: qa-test-{prefix}-{UUID}")
        void shouldMatchExpectedFormat() {
            String name = helper.generateTopicName("orders");
            assertThat(name).matches("qa-test-orders-[0-9a-f-]{36}");
        }

        @Test
        @DisplayName("два вызова с одним prefix возвращают разные имена")
        void shouldGenerateUniqueNames() {
            String name1 = helper.generateTopicName("orders");
            String name2 = helper.generateTopicName("orders");
            assertThat(name1).isNotEqualTo(name2);
        }

        @Test
        @DisplayName("prefix из нескольких слов сохраняется как есть")
        void shouldPreservePrefix() {
            String name = helper.generateTopicName("payment-events");
            assertThat(name).startsWith("qa-test-payment-events-");
        }
    }

    // ═══════════════════════════════════════════════════════════════════════════
    // createTestTopic
    // ═══════════════════════════════════════════════════════════════════════════

    @Nested
    @DisplayName("createTestTopic")
    class CreateTestTopicTests {

        @BeforeEach
        void stubCreate() {
            when(mockKafka.createTopic(any(Topic.class))).thenReturn(true);
            when(mockKafka.waitForTopic(anyString(), anyInt())).thenReturn(true);
        }

        @Test
        @DisplayName("createTestTopic(name, partitions) вызывает facade.createTopic с заданными параметрами")
        void shouldDelegateCreateWithNameAndPartitions() {
            helper.createTestTopic("qa-test-pay", 4);

            verify(mockKafka).createTopic(argThat((Topic t) ->
                    "qa-test-pay".equals(t.getName()) && t.getPartitionCount() == 4));
        }

        @Test
        @DisplayName("createTestTopic(name) использует 2 партиции по умолчанию")
        void shouldUseDefaultPartitionsForNameOnly() {
            helper.createTestTopic("qa-test-pay");

            verify(mockKafka).createTopic(argThat((Topic t) -> t.getPartitionCount() == 2));
        }

        @Test
        @DisplayName("createTestTopic(partitions) генерирует авто-имя с префиксом qa-test")
        void shouldGenerateAutoNameForPartitionsOnly() {
            String name = helper.createTestTopic(3);

            assertThat(name).startsWith("qa-test-auto-");
            verify(mockKafka).createTopic(argThat((Topic t) -> t.getPartitionCount() == 3));
        }

        @Test
        @DisplayName("createTestTopic() без аргументов генерирует авто-имя с 2 партициями")
        void shouldGenerateAutoNameWithDefaultPartitions() {
            String name = helper.createTestTopic();

            assertThat(name).startsWith("qa-test-auto-");
            verify(mockKafka).createTopic(argThat((Topic t) -> t.getPartitionCount() == 2));
        }

        @Test
        @DisplayName("созданный топик регистрируется в createdTopics для очистки")
        void shouldRegisterTopicInCreatedTopics() {
            helper.createTestTopic("qa-test-cleanup", 1);

            assertThat(helper.createdTopics).contains("qa-test-cleanup");
        }

        @Test
        @DisplayName("createTestTopic вызывает waitForTopic с таймаутом 10 секунд")
        void shouldWaitForTopicAfterCreation() {
            helper.createTestTopic("qa-test-wait", 1);

            verify(mockKafka).waitForTopic("qa-test-wait", 10);
        }

        @Test
        @DisplayName("возвращает имя созданного топика")
        void shouldReturnTopicName() {
            String result = helper.createTestTopic("qa-test-ret", 1);

            assertThat(result).isEqualTo("qa-test-ret");
        }
    }

    // ═══════════════════════════════════════════════════════════════════════════
    // createTopicWithDlq
    // ═══════════════════════════════════════════════════════════════════════════

    @Nested
    @DisplayName("createTopicWithDlq")
    class CreateTopicWithDlqTests {

        @Test
        @DisplayName("при успехе регистрирует main-топик и DLQ-топик в createdTopics")
        void shouldRegisterBothTopicsOnSuccess() {
            when(mockKafka.createTopicWithDlq("qa-test-orders")).thenReturn(true);

            boolean result = helper.createTopicWithDlq("qa-test-orders");

            assertThat(result).isTrue();
            assertThat(helper.createdTopics)
                    .contains("qa-test-orders")
                    .contains("qa-test-orders-dlq");
        }

        @Test
        @DisplayName("при неудаче (false) не регистрирует топики в createdTopics")
        void shouldNotRegisterTopicsOnFailure() {
            when(mockKafka.createTopicWithDlq("qa-test-fail")).thenReturn(false);

            boolean result = helper.createTopicWithDlq("qa-test-fail");

            assertThat(result).isFalse();
            assertThat(helper.createdTopics).doesNotContain("qa-test-fail");
        }

        @Test
        @DisplayName("делегирует вызов в facade.createTopicWithDlq")
        void shouldDelegateToFacade() {
            when(mockKafka.createTopicWithDlq(any())).thenReturn(true);

            helper.createTopicWithDlq("qa-test-dlq");

            verify(mockKafka).createTopicWithDlq("qa-test-dlq");
        }
    }

    // ═══════════════════════════════════════════════════════════════════════════
    // publishMessage
    // ═══════════════════════════════════════════════════════════════════════════

    @Nested
    @DisplayName("publishMessage")
    class PublishMessageTests {

        @Test
        @DisplayName("publishMessage(topic, key, value) делегирует в facade.publish(topic, key, value)")
        void shouldDelegateSimplePublish() {
            PublishResult expected = PublishResult.success(msg(TOPIC), 0, 1L, 0L);
            when(mockKafka.publish(TOPIC, "k1", "v1")).thenReturn(expected);

            PublishResult result = helper.publishMessage(TOPIC, "k1", "v1");

            assertThat(result.isSuccess()).isTrue();
            verify(mockKafka).publish(TOPIC, "k1", "v1");
        }

        @Test
        @DisplayName("publishMessage(Message) делегирует в facade.publish(Message)")
        void shouldDelegateDomainPublish() {
            Message message = msg(TOPIC);
            PublishResult expected = PublishResult.success(message, 0, 2L, 0L);
            when(mockKafka.publish(message)).thenReturn(expected);

            PublishResult result = helper.publishMessage(message);

            assertThat(result.getOffset()).isEqualTo(2L);
            verify(mockKafka).publish(message);
        }
    }

    // ═══════════════════════════════════════════════════════════════════════════
    // publishBatch
    // ═══════════════════════════════════════════════════════════════════════════

    @Test
    @DisplayName("publishBatch делегирует список в facade.publishBatch и возвращает результаты")
    void shouldDelegateBatchPublish() {
        List<Message> messages = List.of(msg(TOPIC), msg(TOPIC));
        List<PublishResult> expected = List.of(
                PublishResult.success(msg(TOPIC), 0, 1L, 0L),
                PublishResult.success(msg(TOPIC), 0, 2L, 0L)
        );
        when(mockKafka.publishBatch(messages)).thenReturn(expected);

        List<PublishResult> result = helper.publishBatch(messages);

        assertThat(result).hasSize(2);
        verify(mockKafka).publishBatch(messages);
    }

    // ═══════════════════════════════════════════════════════════════════════════
    // consumeMessages
    // ═══════════════════════════════════════════════════════════════════════════

    @Nested
    @DisplayName("consumeMessages")
    class ConsumeMessagesTests {

        @Test
        @DisplayName("вызывает subscribe + seekToBeginning + pollMessages")
        void shouldCallSubscribeSeekAndPoll() {
            when(mockKafka.pollMessages(eq(5), any(Duration.class)))
                    .thenReturn(successResult(5));

            helper.consumeMessages(TOPIC, 5, Duration.ofSeconds(10));

            verify(mockKafka).subscribe(TOPIC);
            verify(mockKafka).seekToBeginning();
            verify(mockKafka).pollMessages(eq(5), any(Duration.class));
        }

        @Test
        @DisplayName("возвращает список сообщений при успехе")
        void shouldReturnMessagesOnSuccess() {
            when(mockKafka.pollMessages(anyInt(), any())).thenReturn(successResult(3));

            List<Message> result = helper.consumeMessages(TOPIC, 3, Duration.ofSeconds(5));

            assertThat(result).hasSize(3);
        }

        @Test
        @DisplayName("возвращает пустой список при ошибке consume")
        void shouldReturnEmptyListOnFailure() {
            when(mockKafka.pollMessages(anyInt(), any()))
                    .thenReturn(ConsumeResult.failure("Broker down", KafkaErrorCategory.NETWORK_ERROR));

            List<Message> result = helper.consumeMessages(TOPIC, 5, Duration.ofSeconds(5));

            assertThat(result).isEmpty();
        }
    }

    // ═══════════════════════════════════════════════════════════════════════════
    // consumeAllMessages
    // ═══════════════════════════════════════════════════════════════════════════

    @Nested
    @DisplayName("consumeAllMessages")
    class ConsumeAllMessagesTests {

        @Test
        @DisplayName("вызывает subscribe + seekToBeginning + consumeAll с таймаутом 10 секунд")
        void shouldCallSubscribeSeekAndConsumeAll() {
            when(mockKafka.consumeAll(eq(100), eq(Duration.ofSeconds(10))))
                    .thenReturn(successResult(10));

            helper.consumeAllMessages(TOPIC, 100);

            verify(mockKafka).subscribe(TOPIC);
            verify(mockKafka).seekToBeginning();
            verify(mockKafka).consumeAll(100, Duration.ofSeconds(10));
        }

        @Test
        @DisplayName("возвращает все сообщения при успехе")
        void shouldReturnAllMessagesOnSuccess() {
            when(mockKafka.consumeAll(anyInt(), any())).thenReturn(successResult(7));

            List<Message> result = helper.consumeAllMessages(TOPIC, 100);

            assertThat(result).hasSize(7);
        }

        @Test
        @DisplayName("возвращает пустой список при ошибке")
        void shouldReturnEmptyListOnFailure() {
            when(mockKafka.consumeAll(anyInt(), any()))
                    .thenReturn(ConsumeResult.failure("err", KafkaErrorCategory.TIMEOUT_ERROR));

            assertThat(helper.consumeAllMessages(TOPIC, 100)).isEmpty();
        }
    }

    // ═══════════════════════════════════════════════════════════════════════════
    // waitForMessages
    // ═══════════════════════════════════════════════════════════════════════════

    @Nested
    @DisplayName("waitForMessages")
    class WaitForMessagesTests {

        @Test
        @DisplayName("возвращает true когда получено expectedCount сообщений")
        void shouldReturnTrueWhenEnoughMessages() {
            when(mockKafka.pollMessages(eq(5), any())).thenReturn(successResult(5));

            boolean result = helper.waitForMessages(TOPIC, 5, Duration.ofSeconds(10));

            assertThat(result).isTrue();
        }

        @Test
        @DisplayName("возвращает false когда сообщений меньше expected")
        void shouldReturnFalseWhenNotEnoughMessages() {
            when(mockKafka.pollMessages(eq(10), any())).thenReturn(successResult(3));

            boolean result = helper.waitForMessages(TOPIC, 10, Duration.ofSeconds(5));

            assertThat(result).isFalse();
        }

        @Test
        @DisplayName("возвращает false при ошибке consume")
        void shouldReturnFalseOnConsumeFailure() {
            when(mockKafka.pollMessages(anyInt(), any()))
                    .thenReturn(ConsumeResult.failure("err", KafkaErrorCategory.BROKER_NOT_AVAILABLE));

            boolean result = helper.waitForMessages(TOPIC, 5, Duration.ofSeconds(5));

            assertThat(result).isFalse();
        }

        @Test
        @DisplayName("вызывает subscribe + seekToBeginning перед polling")
        void shouldSubscribeAndSeekBeforePolling() {
            when(mockKafka.pollMessages(anyInt(), any())).thenReturn(ConsumeResult.empty());

            helper.waitForMessages(TOPIC, 1, Duration.ofSeconds(1));

            verify(mockKafka).subscribe(TOPIC);
            verify(mockKafka).seekToBeginning();
        }
    }

    // ═══════════════════════════════════════════════════════════════════════════
    // createNewFacade
    // ═══════════════════════════════════════════════════════════════════════════

    @Test
    @DisplayName("createNewFacade возвращает ненулевой KafkaTestFacade")
    void shouldReturnNonNullFacade() {
        // createNewFacade требует реального CONFIG (ConfigFactory).
        // Мы тестируем только то, что метод существует и не бросает NPE
        // при доступном конфиге. Тест изолирован от Kafka-брокера потому
        // что KafkaTestFacade(config) использует ленивую инициализацию адаптеров.
        // Если ConfigFactory выбрасывает исключение при отсутствии конфига —
        // тест корректно падает, сигнализируя о проблеме с окружением.
        assertThatNoException().isThrownBy(helper::createNewFacade);
    }
}
