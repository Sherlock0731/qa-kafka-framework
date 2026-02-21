package qa.autotest.framework.application.service;

import org.junit.jupiter.api.*;
import org.junit.jupiter.api.extension.ExtendWith;
import org.mockito.Mock;
import org.mockito.junit.jupiter.MockitoExtension;
import qa.autotest.framework.domain.model.Topic;
import qa.autotest.framework.domain.port.TopicRepository;

import java.util.Set;

import static org.assertj.core.api.Assertions.*;
import static org.mockito.ArgumentMatchers.*;
import static org.mockito.Mockito.*;

/**
 * Unit-тесты для TopicManagementService.
 * <p>
 * Покрываемые сценарии:
 * - createTopic: валидация → exists-check → createTopic
 * - createTopic возвращает false если топик уже существует
 * - createTopicWithDlq: создаёт main + DLQ; возвращает false если main уже есть
 * - createTopicWithDlq: возвращает false если main создан, но DLQ не создался
 * - createTopicWithRetries: создаёт main + N retry топиков
 * - deleteTopic: возвращает false если топик не существует
 * - deleteTopic: делегирует topicRepository.deleteTopic при существующем топике
 * - deleteTestTopics: фильтрует только qa-test* топики и удаляет их
 * - deleteTestTopics: возвращает 0 если нет тестовых топиков
 * - topicExists делегирует
 * - createTopic бросает IllegalArgumentException при невалидном топике (до вызова репозитория)
 */
@DisplayName("TopicManagementService")
@ExtendWith(MockitoExtension.class)
class TopicManagementServiceTest {

    @Mock
    TopicRepository topicRepository;

    private TopicManagementService service;

    // ── fixtures ──────────────────────────────────────────────────────────
    private static Topic testTopic(String name) {
        return Topic.builder().name(name).partitionCount(2).build();
    }

    @BeforeEach
    void setUp() {
        service = new TopicManagementService(topicRepository);
    }

    // ═══════════════════════════════════════════════════════════════════════
    // createTopic
    // ═══════════════════════════════════════════════════════════════════════

    @Test
    @DisplayName("createTopic: валидация → exists-check → createTopic при новом топике")
    void shouldCreateTopicWhenNotExists() {
        Topic topic = testTopic("qa-test-new");
        when(topicRepository.exists(topic)).thenReturn(false);
        when(topicRepository.createTopic(topic)).thenReturn(true);

        boolean result = service.createTopic(topic);

        assertThat(result).isTrue();
        verify(topicRepository).exists(topic);
        verify(topicRepository).createTopic(topic);
    }

    @Test
    @DisplayName("createTopic возвращает false и не вызывает createTopic если топик уже существует")
    void shouldReturnFalseWhenTopicAlreadyExists() {
        Topic topic = testTopic("qa-test-existing");
        when(topicRepository.exists(topic)).thenReturn(true);

        boolean result = service.createTopic(topic);

        assertThat(result).isFalse();
        verify(topicRepository, never()).createTopic(any());
    }

    @Test
    @DisplayName("createTopic бросает IllegalArgumentException при невалидном имени (до репозитория)")
    void shouldThrowBeforeRepositoryWhenTopicNameInvalid() {
        Topic invalid = Topic.builder().name("invalid name!").build(); // пробел и ! недопустимы

        assertThatThrownBy(() -> service.createTopic(invalid))
                .isInstanceOf(IllegalArgumentException.class);

        verifyNoInteractions(topicRepository);
    }

    @Test
    @DisplayName("createTopic бросает NullPointerException при null имени (до репозитория)")
    void shouldThrowBeforeRepositoryWhenTopicNameNull() {
        Topic invalid = Topic.builder().build(); // name = null

        assertThatThrownBy(() -> service.createTopic(invalid))
                .isInstanceOf(NullPointerException.class);

        verifyNoInteractions(topicRepository);
    }

    @Test
    @DisplayName("createTopic бросает при partitionCount < 1")
    void shouldThrowWhenPartitionCountZero() {
        Topic invalid = Topic.builder().name("qa-test-bad")
                .partitionCount(0)
                .build();

        assertThatThrownBy(() -> service.createTopic(invalid))
                .isInstanceOf(IllegalArgumentException.class)
                .hasMessageContaining("Partition count");

        verifyNoInteractions(topicRepository);
    }

    // ═══════════════════════════════════════════════════════════════════════
    // createTopicWithDlq
    // ═══════════════════════════════════════════════════════════════════════

    @Test
    @DisplayName("createTopicWithDlq создаёт main-топик и DLQ-топик")
    void shouldCreateMainAndDlqTopics() {
        Topic main = testTopic("qa-test-orders");
        Topic dlq = main.createDlqTopic(); // qa-test-orders-dlq

        when(topicRepository.exists(main)).thenReturn(false);
        when(topicRepository.exists(dlq)).thenReturn(false);
        when(topicRepository.createTopic(main)).thenReturn(true);
        when(topicRepository.createTopic(dlq)).thenReturn(true);

        boolean result = service.createTopicWithDlq(main);

        assertThat(result).isTrue();
        verify(topicRepository).createTopic(main);
        verify(topicRepository).createTopic(argThat(t -> t.getName().endsWith("-dlq")));
    }

    @Test
    @DisplayName("createTopicWithDlq возвращает false если main-топик уже существует")
    void shouldReturnFalseWhenMainTopicAlreadyExists() {
        Topic main = testTopic("qa-test-orders");
        when(topicRepository.exists(main)).thenReturn(true);

        boolean result = service.createTopicWithDlq(main);

        assertThat(result).isFalse();
        // DLQ не должен создаваться
        verify(topicRepository, never()).createTopic(argThat(t -> t.getName().endsWith("-dlq")));
    }

    @Test
    @DisplayName("createTopicWithDlq возвращает false если DLQ не создался")
    void shouldReturnFalseWhenDlqCreationFails() {
        Topic main = testTopic("qa-test-orders");
        Topic dlq = main.createDlqTopic();

        when(topicRepository.exists(main)).thenReturn(false);
        when(topicRepository.exists(dlq)).thenReturn(false);
        when(topicRepository.createTopic(main)).thenReturn(true);
        when(topicRepository.createTopic(dlq)).thenReturn(false); // DLQ не создался

        boolean result = service.createTopicWithDlq(main);

        assertThat(result).isFalse();
    }

    // ═══════════════════════════════════════════════════════════════════════
    // createTopicWithRetries
    // ═══════════════════════════════════════════════════════════════════════

    @Test
    @DisplayName("createTopicWithRetries создаёт main + N retry-топиков")
    void shouldCreateMainAndRetryTopics() {
        Topic main = testTopic("qa-test-payment");
        // Все топики (main + 3 retry) не существуют
        when(topicRepository.exists(any())).thenReturn(false);
        when(topicRepository.createTopic(any())).thenReturn(true);

        boolean result = service.createTopicWithRetries(main, 3);

        assertThat(result).isTrue();
        // main + retry-1 + retry-2 + retry-3 = 4 создания
        verify(topicRepository, times(4)).createTopic(any());
    }

    @Test
    @DisplayName("createTopicWithRetries возвращает false при провале создания main-топика")
    void shouldReturnFalseWhenMainTopicCreationFails() {
        Topic main = testTopic("qa-test-payment");
        when(topicRepository.exists(main)).thenReturn(true); // main уже существует

        boolean result = service.createTopicWithRetries(main, 2);

        assertThat(result).isFalse();
        // retry-топики не должны создаваться
        verify(topicRepository, never()).createTopic(argThat(t -> t.getName().contains("-retry-")));
    }

    // ═══════════════════════════════════════════════════════════════════════
    // deleteTopic
    // ═══════════════════════════════════════════════════════════════════════

    @Test
    @DisplayName("deleteTopic возвращает false если топик не существует")
    void shouldReturnFalseWhenDeletingNonExistentTopic() {
        Topic topic = testTopic("qa-test-ghost");
        when(topicRepository.exists(topic)).thenReturn(false);

        boolean result = service.deleteTopic(topic);

        assertThat(result).isFalse();
        verify(topicRepository, never()).deleteTopic(any());
    }

    @Test
    @DisplayName("deleteTopic делегирует topicRepository.deleteTopic при существующем топике")
    void shouldDelegateDeletionWhenTopicExists() {
        Topic topic = testTopic("qa-test-delete-me");
        when(topicRepository.exists(topic)).thenReturn(true);
        when(topicRepository.deleteTopic(topic)).thenReturn(true);

        boolean result = service.deleteTopic(topic);

        assertThat(result).isTrue();
        verify(topicRepository).deleteTopic(topic);
    }

    // ═══════════════════════════════════════════════════════════════════════
    // deleteTestTopics
    // ═══════════════════════════════════════════════════════════════════════

    @Test
    @DisplayName("deleteTestTopics удаляет только топики с префиксом qa-test")
    void shouldDeleteOnlyTestTopics() {
        Topic prodTopic = testTopic("orders");             // НЕ тестовый
        Topic testTopic1 = testTopic("qa-test-orders");     // тестовый
        Topic testTopic2 = testTopic("qa-test-payments");   // тестовый

        when(topicRepository.getAllTopics())
                .thenReturn(Set.of(prodTopic, testTopic1, testTopic2));
        when(topicRepository.deleteTopics(any())).thenReturn(2);

        int deleted = service.deleteTestTopics();

        assertThat(deleted).isEqualTo(2);
        verify(topicRepository).deleteTopics(argThat(topics ->
                topics.size() == 2 &&
                        topics.stream().allMatch(Topic::isTestTopic)
        ));
    }

    @Test
    @DisplayName("deleteTestTopics возвращает 0 и не вызывает deleteTopics если нет тестовых топиков")
    void shouldReturnZeroWhenNoTestTopics() {
        when(topicRepository.getAllTopics())
                .thenReturn(Set.of(testTopic("orders"), testTopic("payments")));

        int deleted = service.deleteTestTopics();

        assertThat(deleted).isZero();
        verify(topicRepository, never()).deleteTopics(any());
    }

    // ═══════════════════════════════════════════════════════════════════════
    // topicExists
    // ═══════════════════════════════════════════════════════════════════════

    @Test
    @DisplayName("topicExists делегирует в topicRepository.exists")
    void shouldDelegateTopicExists() {
        Topic topic = testTopic("qa-test-check");
        when(topicRepository.exists(topic)).thenReturn(true);

        assertThat(service.topicExists(topic)).isTrue();
        verify(topicRepository).exists(topic);
    }

    // ═══════════════════════════════════════════════════════════════════════
    // close
    // ═══════════════════════════════════════════════════════════════════════

    @Test
    @DisplayName("close() делегирует в topicRepository.close()")
    void shouldDelegateClose() {
        service.close();
        verify(topicRepository).close();
    }
}
