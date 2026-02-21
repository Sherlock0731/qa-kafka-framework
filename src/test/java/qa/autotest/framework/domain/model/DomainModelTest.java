package qa.autotest.framework.domain.model;

import org.junit.jupiter.api.*;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.ValueSource;

import java.time.Instant;
import java.util.List;
import java.util.Map;

import static org.assertj.core.api.Assertions.*;

/**
 * Unit-тесты доменных моделей: Message, Topic, ConsumeResult, PublishResult.
 * <p>
 * Принцип: тестируем бизнес-логику и domain-валидацию, а не Lombok-геттеры.
 */
@DisplayName("Domain Models")
class DomainModelTest {

    // ═══════════════════════════════════════════════════════════════════════
    // Topic — валидация и бизнес-методы
    // ═══════════════════════════════════════════════════════════════════════

    @Nested
    @DisplayName("Topic")
    class TopicTests {

        @Test
        @DisplayName("validate() проходит для корректного топика")
        void shouldPassValidationForValidTopic() {
            Topic topic = Topic.builder().name("qa-test-orders").build();
            assertThatNoException().isThrownBy(topic::validate);
        }

        @Test
        @DisplayName("validate() бросает NullPointerException при null имени")
        void shouldThrowWhenNameNull() {
            Topic topic = Topic.builder().build();
            assertThatThrownBy(topic::validate).isInstanceOf(NullPointerException.class);
        }

        @Test
        @DisplayName("validate() бросает IllegalArgumentException при пустом имени")
        void shouldThrowWhenNameBlank() {
            Topic topic = Topic.builder().name("   ").build();
            assertThatThrownBy(topic::validate)
                    .isInstanceOf(IllegalArgumentException.class)
                    .hasMessageContaining("empty");
        }

        @ParameterizedTest(name = "name={0}")
        @ValueSource(strings = {"my topic", "topic!", "topic@host", "тopic"})
        @DisplayName("validate() бросает при именах с недопустимыми символами")
        void shouldThrowForInvalidCharactersInName(String name) {
            Topic topic = Topic.builder().name(name).build();
            assertThatThrownBy(topic::validate)
                    .isInstanceOf(IllegalArgumentException.class);
        }

        @Test
        @DisplayName("validate() бросает при имени длиннее 249 символов")
        void shouldThrowWhenNameTooLong() {
            String longName = "a".repeat(250);
            Topic topic = Topic.builder().name(longName).build();
            assertThatThrownBy(topic::validate)
                    .isInstanceOf(IllegalArgumentException.class)
                    .hasMessageContaining("249");
        }

        @ParameterizedTest(name = "name={0}")
        @ValueSource(strings = {".", ".."})
        @DisplayName("validate() бросает для зарезервированных имён '.' и '..'")
        void shouldThrowForReservedNames(String name) {
            Topic topic = Topic.builder().name(name).build();
            assertThatThrownBy(topic::validate)
                    .isInstanceOf(IllegalArgumentException.class);
        }

        @Test
        @DisplayName("validate() бросает при partitionCount < 1")
        void shouldThrowWhenPartitionCountIsZero() {
            Topic topic = Topic.builder().name("valid").partitionCount(0).build();
            assertThatThrownBy(topic::validate)
                    .isInstanceOf(IllegalArgumentException.class)
                    .hasMessageContaining("Partition count");
        }

        @Test
        @DisplayName("validate() бросает при replicationFactor < 1")
        void shouldThrowWhenReplicationFactorIsZero() {
            Topic topic = Topic.builder().name("valid").replicationFactor((short) 0).build();
            assertThatThrownBy(topic::validate)
                    .isInstanceOf(IllegalArgumentException.class)
                    .hasMessageContaining("Replication factor");
        }

        @Test
        @DisplayName("isTestTopic() возвращает true для qa-test-* префикса")
        void shouldDetectQaTestTopics() {
            assertThat(Topic.builder().name("qa-test-orders").build().isTestTopic()).isTrue();
            assertThat(Topic.builder().name("test-payments").build().isTestTopic()).isTrue();
            assertThat(Topic.builder().name("orders").build().isTestTopic()).isFalse();
        }

        @Test
        @DisplayName("isDlqTopic() возвращает true для топиков с суффиксом -dlq")
        void shouldDetectDlqTopics() {
            assertThat(Topic.builder().name("orders-dlq").build().isDlqTopic()).isTrue();
            assertThat(Topic.builder().name("orders").build().isDlqTopic()).isFalse();
        }

        @Test
        @DisplayName("isRetryTopic() возвращает true для топиков с -retry- в имени")
        void shouldDetectRetryTopics() {
            assertThat(Topic.builder().name("orders-retry-1").build().isRetryTopic()).isTrue();
            assertThat(Topic.builder().name("orders").build().isRetryTopic()).isFalse();
        }

        @Test
        @DisplayName("isHighlyAvailable() возвращает true при replicationFactor >= 2")
        void shouldDetectHighAvailability() {
            assertThat(Topic.builder().name("t").replicationFactor((short) 2).build().isHighlyAvailable()).isTrue();
            assertThat(Topic.builder().name("t").replicationFactor((short) 1).build().isHighlyAvailable()).isFalse();
        }

        @Test
        @DisplayName("supportsParallelism() возвращает true при partitionCount > 1")
        void shouldDetectParallelism() {
            assertThat(Topic.builder().name("t").partitionCount(2).build().supportsParallelism()).isTrue();
            assertThat(Topic.builder().name("t").partitionCount(1).build().supportsParallelism()).isFalse();
        }

        @Test
        @DisplayName("createDlqTopic() создаёт топик с суффиксом -dlq и теми же параметрами")
        void shouldCreateDlqTopicWithCorrectNameAndParams() {
            Topic source = Topic.builder().name("orders").partitionCount(3).replicationFactor((short) 2).build();
            Topic dlq = source.createDlqTopic();

            assertThat(dlq.getName()).isEqualTo("orders-dlq");
            assertThat(dlq.getPartitionCount()).isEqualTo(3);
            assertThat(dlq.getReplicationFactor()).isEqualTo((short) 2);
        }

        @Test
        @DisplayName("createRetryTopic(n) создаёт топик с -retry-n суффиксом")
        void shouldCreateRetryTopicWithLevel() {
            Topic source = Topic.builder().name("orders").build();
            Topic retry = source.createRetryTopic(2);

            assertThat(retry.getName()).isEqualTo("orders-retry-2");
        }
    }

    // ═══════════════════════════════════════════════════════════════════════
    // Message — валидация и бизнес-методы
    // ═══════════════════════════════════════════════════════════════════════

    @Nested
    @DisplayName("Message")
    class MessageTests {

        private static Topic validTopic() {
            return Topic.builder().name("qa-test-msg").build();
        }

        private static Message validMessage() {
            return Message.builder()
                    .topic(validTopic())
                    .key("k1")
                    .content("{\"id\":1}")
                    .build();
        }

        @Test
        @DisplayName("validate() проходит для корректного сообщения")
        void shouldPassValidationForValidMessage() {
            assertThatNoException().isThrownBy(validMessage()::validate);
        }

        @Test
        @DisplayName("validate() бросает NullPointerException при null topic")
        void shouldThrowWhenTopicNull() {
            Message msg = Message.builder().key("k").content("data").build();
            assertThatThrownBy(msg::validate).isInstanceOf(NullPointerException.class);
        }

        @Test
        @DisplayName("validate() бросает NullPointerException при null content")
        void shouldThrowWhenContentNull() {
            Message msg = Message.builder().topic(validTopic()).key("k").build();
            assertThatThrownBy(msg::validate).isInstanceOf(NullPointerException.class);
        }

        @Test
        @DisplayName("validate() бросает IllegalArgumentException при пустом content")
        void shouldThrowWhenContentEmpty() {
            Message msg = Message.builder().topic(validTopic()).key("k").content("").build();
            assertThatThrownBy(msg::validate)
                    .isInstanceOf(IllegalArgumentException.class)
                    .hasMessageContaining("empty");
        }

        @Test
        @DisplayName("validate() вызывает topic.validate() (проверяет имя топика)")
        void shouldDelegateTopicValidation() {
            Topic badTopic = Topic.builder().name("invalid name!").build();
            Message msg = Message.builder().topic(badTopic).key("k").content("data").build();
            assertThatThrownBy(msg::validate).isInstanceOf(IllegalArgumentException.class);
        }

        @Test
        @DisplayName("messageId генерируется автоматически при Builder.Default")
        void shouldAutoGenerateMessageId() {
            Message m1 = validMessage();
            Message m2 = validMessage();
            assertThat(m1.getMessageId()).isNotNull().isNotBlank();
            assertThat(m1.getMessageId()).isNotEqualTo(m2.getMessageId());
        }

        @Test
        @DisplayName("isCorrelated() — true только при непустом correlationId")
        void shouldDetectCorrelation() {
            Message correlated = validMessage().withCorrelation("corr-123");
            Message plain = validMessage();

            assertThat(correlated.isCorrelated()).isTrue();
            assertThat(plain.isCorrelated()).isFalse();
        }

        @Test
        @DisplayName("isEvent() — true только при непустом eventType")
        void shouldDetectEventType() {
            Message event = Message.builder()
                    .topic(validTopic()).key("k").content("d")
                    .eventType("ORDER_CREATED").build();
            Message plain = validMessage();

            assertThat(event.isEvent()).isTrue();
            assertThat(plain.isEvent()).isFalse();
        }

        @Test
        @DisplayName("hasHeaders() — true при непустых заголовках")
        void shouldDetectHeaders() {
            Message withHeaders = Message.builder()
                    .topic(validTopic()).key("k").content("d")
                    .headers(Map.of("trace-id", "abc")).build();
            Message withoutHeaders = validMessage();

            assertThat(withHeaders.hasHeaders()).isTrue();
            assertThat(withoutHeaders.hasHeaders()).isFalse();
        }

        @Test
        @DisplayName("hasExplicitPartition() — true только при выставленном partition")
        void shouldDetectExplicitPartition() {
            Message targeted = validMessage().withPartition(2);
            Message noTarget = validMessage();

            assertThat(targeted.hasExplicitPartition()).isTrue();
            assertThat(noTarget.hasExplicitPartition()).isFalse();
        }

        @Test
        @DisplayName("withContent() создаёт новый неизменяемый объект с обновлённым content")
        void shouldCreateNewInstanceWithUpdatedContent() {
            Message original = validMessage();
            Message updated = original.withContent("{\"id\":2}");

            assertThat(updated).isNotSameAs(original);
            assertThat(updated.getContent()).isEqualTo("{\"id\":2}");
            assertThat(original.getContent()).isEqualTo("{\"id\":1}"); // исходник не изменился
        }

        @Test
        @DisplayName("withHeader() добавляет заголовок без изменения оригинала")
        void shouldAddHeaderImmutably() {
            Message original = validMessage();
            Message withHdr = original.withHeader("event-type", "ORDER");

            assertThat(withHdr.getHeaders()).containsEntry("event-type", "ORDER");
            assertThat(original.hasHeaders()).isFalse(); // оригинал не изменён
        }

        @Test
        @DisplayName("getHeader() возвращает значение по ключу или null")
        void shouldGetHeaderByKey() {
            Message msg = validMessage().withHeader("trace-id", "xyz");
            assertThat(msg.getHeader("trace-id")).isEqualTo("xyz");
            assertThat(msg.getHeader("unknown")).isNull();
        }
    }

    // ═══════════════════════════════════════════════════════════════════════
    // ConsumeResult — фабричные методы и бизнес-логика
    // ═══════════════════════════════════════════════════════════════════════

    @Nested
    @DisplayName("ConsumeResult")
    class ConsumeResultTests {

        private static Message msg() {
            return Message.builder()
                    .topic(Topic.builder().name("t").build())
                    .key("k").content("v").build();
        }

        @Test
        @DisplayName("success(messages) — success=true, messageCount совпадает с размером списка")
        void shouldCreateSuccessResultWithCorrectCount() {
            List<Message> messages = List.of(msg(), msg());
            ConsumeResult result = ConsumeResult.success(messages);

            assertThat(result.isSuccess()).isTrue();
            assertThat(result.getMessageCount()).isEqualTo(2);
            assertThat(result.hasMessages()).isTrue();
        }

        @Test
        @DisplayName("empty() — success=true, messageCount=0, isEmpty=true")
        void shouldCreateEmptyResult() {
            ConsumeResult result = ConsumeResult.empty();

            assertThat(result.isSuccess()).isTrue();
            assertThat(result.getMessageCount()).isZero();
            assertThat(result.isEmpty()).isTrue();
            assertThat(result.hasMessages()).isFalse();
        }

        @Test
        @DisplayName("timeout() — success=true, isTimeout=true")
        void shouldCreateTimeoutResult() {
            ConsumeResult result = ConsumeResult.timeout();

            assertThat(result.isSuccess()).isTrue();
            assertThat(result.isTimeout()).isTrue();
            assertThat(result.isEmpty()).isTrue();
        }

        @Test
        @DisplayName("failure() — success=false, errorMessage присутствует")
        void shouldCreateFailureResult() {
            ConsumeResult result = ConsumeResult.failure("Broker down", KafkaErrorCategory.NETWORK_ERROR);

            assertThat(result.isSuccess()).isFalse();
            assertThat(result.getErrorMessage()).isEqualTo("Broker down");
            assertThat(result.getErrorCategory()).isEqualTo(KafkaErrorCategory.NETWORK_ERROR);
        }

        @Test
        @DisplayName("isRetryable() — true для NETWORK_ERROR, false для AUTHENTICATION_ERROR")
        void shouldCorrectlyClassifyRetryability() {
            ConsumeResult retryable = ConsumeResult.failure("err", KafkaErrorCategory.NETWORK_ERROR);
            ConsumeResult nonRetryable = ConsumeResult.failure("err", KafkaErrorCategory.AUTHENTICATION_ERROR);

            assertThat(retryable.isRetryable()).isTrue();
            assertThat(nonRetryable.isRetryable()).isFalse();
        }

        @Test
        @DisplayName("failureFrom(message, exception) автоматически определяет категорию")
        void shouldDeriveErrorCategoryFromException() {
            ConsumeResult result = ConsumeResult.failureFrom(
                    "poll failed",
                    new org.apache.kafka.common.errors.TimeoutException("timed out")
            );

            assertThat(result.getErrorCategory()).isEqualTo(KafkaErrorCategory.TIMEOUT_ERROR);
        }

        @Test
        @DisplayName("validate() бросает при несоответствии messageCount и messages.size()")
        void shouldThrowWhenMessageCountMismatch() {
            ConsumeResult invalid = ConsumeResult.builder()
                    .success(true)
                    .message(msg())
                    .messageCount(99) // намеренно неверный
                    .build();

            assertThatThrownBy(invalid::validate)
                    .isInstanceOf(IllegalArgumentException.class)
                    .hasMessageContaining("mismatch");
        }
    }

    // ═══════════════════════════════════════════════════════════════════════
    // PublishResult — фабричные методы и бизнес-логика
    // ═══════════════════════════════════════════════════════════════════════

    @Nested
    @DisplayName("PublishResult")
    class PublishResultTests {

        private static Message msg() {
            return Message.builder()
                    .topic(Topic.builder().name("t").build())
                    .key("k").content("v").build();
        }

        @Test
        @DisplayName("success(message, partition, offset, timestamp) — isSuccess=true")
        void shouldCreateSuccessResult() {
            PublishResult result = PublishResult.success(msg(), 1, 42L, System.currentTimeMillis());

            assertThat(result.isSuccess()).isTrue();
            assertThat(result.getPartition()).isEqualTo(1);
            assertThat(result.getOffset()).isEqualTo(42L);
        }

        @Test
        @DisplayName("failure(message, error, category) — isSuccess=false")
        void shouldCreateFailureResult() {
            PublishResult result = PublishResult.failure(
                    msg(), "Timeout", KafkaErrorCategory.TIMEOUT_ERROR);

            assertThat(result.isSuccess()).isFalse();
            assertThat(result.getErrorMessage()).isEqualTo("Timeout");
            assertThat(result.getErrorCategory()).isEqualTo(KafkaErrorCategory.TIMEOUT_ERROR);
        }

        @Test
        @DisplayName("isRetryable() — true для TIMEOUT_ERROR, false для AUTHENTICATION_ERROR")
        void shouldCorrectlyClassifyRetryability() {
            PublishResult retryable = PublishResult.failure(msg(), "err", KafkaErrorCategory.TIMEOUT_ERROR);
            PublishResult nonRetryable = PublishResult.failure(msg(), "err", KafkaErrorCategory.AUTHENTICATION_ERROR);

            assertThat(retryable.isRetryable()).isTrue();
            assertThat(nonRetryable.isRetryable()).isFalse();
        }

        @Test
        @DisplayName("failureFrom(message, error, exception) — автоматическое определение категории")
        void shouldDeriveErrorCategoryFromException() {
            PublishResult result = PublishResult.failureFrom(
                    msg(),
                    "send failed",
                    new org.apache.kafka.common.errors.NetworkException("conn refused")
            );

            assertThat(result.getErrorCategory()).isEqualTo(KafkaErrorCategory.NETWORK_ERROR);
        }

        @Test
        @DisplayName("wasTargetedPublish() — true только при явно заданном partition")
        void shouldDetectTargetedPublish() {
            Message targeted = msg().withPartition(2);
            Message untargeted = msg();

            PublishResult targetedResult = PublishResult.success(targeted, 2, 1L, 0L);
            PublishResult untargetedResult = PublishResult.success(untargeted, 0, 1L, 0L);

            assertThat(targetedResult.wasTargetedPublish()).isTrue();
            assertThat(untargetedResult.wasTargetedPublish()).isFalse();
        }

        @Test
        @DisplayName("validate() не бросает для корректного success-результата")
        void shouldPassValidationForValidSuccess() {
            PublishResult result = PublishResult.success(msg(), 0, 1L, System.currentTimeMillis());
            assertThatNoException().isThrownBy(result::validate);
        }

        @Test
        @DisplayName("validate() бросает для success без partition")
        void shouldThrowValidationForSuccessWithoutPartition() {
            PublishResult invalid = PublishResult.builder()
                    .message(msg())
                    .success(true)
                    // partition и offset не выставлены
                    .build();

            assertThatThrownBy(invalid::validate).isInstanceOf(NullPointerException.class);
        }

        @Test
        @DisplayName("validate() бросает для failure без errorMessage")
        void shouldThrowValidationForFailureWithoutErrorMessage() {
            PublishResult invalid = PublishResult.builder()
                    .message(msg())
                    .success(false)
                    // errorMessage не выставлен
                    .build();

            assertThatThrownBy(invalid::validate).isInstanceOf(NullPointerException.class);
        }
    }

    // ═══════════════════════════════════════════════════════════════════════
    // KafkaErrorCategory — retryability и fromException маппинг
    // ═══════════════════════════════════════════════════════════════════════

    @Nested
    @DisplayName("KafkaErrorCategory")
    class KafkaErrorCategoryTests {

        @Test
        @DisplayName("Retryable категории: NETWORK_ERROR, TIMEOUT_ERROR, BROKER_NOT_AVAILABLE, GROUP_COORDINATION_ERROR, UNKNOWN_ERROR")
        void shouldMarkTransientCategoriesAsRetryable() {
            assertThat(KafkaErrorCategory.NETWORK_ERROR.isRetryable()).isTrue();
            assertThat(KafkaErrorCategory.TIMEOUT_ERROR.isRetryable()).isTrue();
            assertThat(KafkaErrorCategory.BROKER_NOT_AVAILABLE.isRetryable()).isTrue();
            assertThat(KafkaErrorCategory.BUFFER_EXHAUSTED.isRetryable()).isTrue();
            assertThat(KafkaErrorCategory.GROUP_COORDINATION_ERROR.isRetryable()).isTrue();
            assertThat(KafkaErrorCategory.UNKNOWN_ERROR.isRetryable()).isTrue();
        }

        @Test
        @DisplayName("Non-retryable категории: AUTH, AUTHZ, SERIALIZATION, TOPIC_NOT_FOUND, DESERIALIZATION, OFFSET_OUT_OF_RANGE")
        void shouldMarkPermanentCategoriesAsNonRetryable() {
            assertThat(KafkaErrorCategory.AUTHENTICATION_ERROR.isRetryable()).isFalse();
            assertThat(KafkaErrorCategory.AUTHORIZATION_ERROR.isRetryable()).isFalse();
            assertThat(KafkaErrorCategory.SERIALIZATION_ERROR.isRetryable()).isFalse();
            assertThat(KafkaErrorCategory.TOPIC_NOT_FOUND.isRetryable()).isFalse();
            assertThat(KafkaErrorCategory.DESERIALIZATION_ERROR.isRetryable()).isFalse();
            assertThat(KafkaErrorCategory.OFFSET_OUT_OF_RANGE.isRetryable()).isFalse();
        }

        @Test
        @DisplayName("fromPublishException: null → UNKNOWN_ERROR")
        void shouldMapNullToUnknownError() {
            assertThat(KafkaErrorCategory.fromPublishException(null))
                    .isEqualTo(KafkaErrorCategory.UNKNOWN_ERROR);
        }

        @Test
        @DisplayName("fromPublishException: TimeoutException → TIMEOUT_ERROR")
        void shouldMapTimeoutExceptionToTimeoutError() {
            assertThat(KafkaErrorCategory.fromPublishException(
                    new org.apache.kafka.common.errors.TimeoutException("t/o")))
                    .isEqualTo(KafkaErrorCategory.TIMEOUT_ERROR);
        }

        @Test
        @DisplayName("fromPublishException: AuthenticationException → AUTHENTICATION_ERROR")
        void shouldMapAuthExceptionToAuthError() {
            assertThat(KafkaErrorCategory.fromPublishException(
                    new org.apache.kafka.common.errors.AuthenticationException("bad cert")))
                    .isEqualTo(KafkaErrorCategory.AUTHENTICATION_ERROR);
        }

        @Test
        @DisplayName("fromConsumeException: OffsetOutOfRangeException → OFFSET_OUT_OF_RANGE")
        void shouldMapOffsetExceptionToOffsetOutOfRange() {
            assertThat(KafkaErrorCategory.fromConsumeException(
                    new org.apache.kafka.common.errors.OffsetOutOfRangeException("offset out of range")))
                    .isEqualTo(KafkaErrorCategory.OFFSET_OUT_OF_RANGE);
        }

        @Test
        @DisplayName("fromConsumeException: CoordinatorNotAvailableException → GROUP_COORDINATION_ERROR")
        void shouldMapCoordinatorExceptionToGroupCoordError() {
            assertThat(KafkaErrorCategory.fromConsumeException(
                    new org.apache.kafka.common.errors.CoordinatorNotAvailableException("c/n/a")))
                    .isEqualTo(KafkaErrorCategory.GROUP_COORDINATION_ERROR);
        }

        @Test
        @DisplayName("getDisplayName() не null и не пустой для всех категорий")
        void shouldHaveNonBlankDisplayNameForAllCategories() {
            for (KafkaErrorCategory cat : KafkaErrorCategory.values()) {
                assertThat(cat.getDisplayName())
                        .as("displayName для %s", cat)
                        .isNotBlank();
            }
        }
    }
}
