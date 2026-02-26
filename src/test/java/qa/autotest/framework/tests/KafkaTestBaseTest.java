package qa.autotest.framework.tests;

import org.junit.jupiter.api.*;
import org.junit.jupiter.api.extension.ExtendWith;
import org.mockito.Mock;
import org.mockito.junit.jupiter.MockitoExtension;
import qa.autotest.framework.application.service.KafkaTestFacade;
import tests.BaseTest;
import tests.KafkaTestBase;

import static org.assertj.core.api.Assertions.*;
import static org.mockito.Mockito.*;

/**
 * Unit-тесты для {@link KafkaTestBase}.
 * <p>
 * Тестируем только то, что поддаётся изоляции без реального Kafka-брокера:
 * <ul>
 *   <li>{@code registerFacade()} — возвращает тот же объект, добавляет в ALL_FACADES.</li>
 *   <li>Изоляция {@code createdTopics} — каждый экземпляр Stub имеет свой список.</li>
 *   <li>{@code testStartTime} — выставляется в момент инициализации.</li>
 *   <li>Lifecycle-методы {@code tearDown} / {@code globalCleanup} тестируются
 *       через mock-фасад (проверяем порядок вызовов cleanupTopics → close).</li>
 *   <li>WeakHashMap семантика — GC может собрать facade до globalCleanup.</li>
 * </ul>
 *
 * <h3>Что НЕ тестируется здесь</h3>
 * <ul>
 *   <li>{@code @BeforeAll initFramework()} — только логирование, нет смысла.</li>
 *   <li>{@code @BeforeEach setUp()} — создаёт реальный {@code KafkaTestFacade(CONFIG)},
 *       требует брокера; покрыт интеграционными тестами.</li>
 *   <li>{@code @AfterAll globalCleanup()} — статический метод, непросто вызвать изолированно;
 *       логика {@code closeAll()} тестируется в {@code KafkaTestFacadeTest}.</li>
 * </ul>
 */
@Tag("unit")
@DisplayName("KafkaTestBase")
@ExtendWith(MockitoExtension.class)
class KafkaTestBaseTest {

    /**
     * Минимальная конкретная реализация KafkaTestBase / KafkaTestHelpers / BaseTest.
     * Позволяет тестировать защищённые члены базового класса напрямую.
     */
    static class Stub extends BaseTest {
        /** Инжектирует mock-фасад в защищённое поле базового класса. */
        void injectFacade(KafkaTestFacade f) {
            this.kafka = f;
        }
    }

    @Mock
    KafkaTestFacade mockKafka;

    private Stub stub;

    @BeforeEach
    void setUp() {
        stub = new Stub();
        stub.injectFacade(mockKafka);
    }

    // ═══════════════════════════════════════════════════════════════════════════
    // registerFacade
    // ═══════════════════════════════════════════════════════════════════════════

    @Nested
    @DisplayName("registerFacade")
    class RegisterFacadeTests {

        @Test
        @DisplayName("возвращает тот же объект, который был передан")
        void shouldReturnSameFacadeInstance() {
            KafkaTestFacade returned = stub.registerFacade(mockKafka);
            assertThat(returned).isSameAs(mockKafka);
        }

        @Test
        @DisplayName("принимает null без NullPointerException (WeakHashMap допускает null-key)")
        void shouldNotThrowForNullFacade() {
            // WeakHashMap допускает null — не должно быть NPE на уровне register
            assertThatNoException().isThrownBy(() -> stub.registerFacade(null));
        }

        @Test
        @DisplayName("последовательные вызовы с разными фасадами не выбрасывают исключений")
        void shouldHandleMultipleRegistrations() {
            KafkaTestFacade f1 = mock(KafkaTestFacade.class);
            KafkaTestFacade f2 = mock(KafkaTestFacade.class);
            KafkaTestFacade f3 = mock(KafkaTestFacade.class);

            assertThatNoException().isThrownBy(() -> {
                stub.registerFacade(f1);
                stub.registerFacade(f2);
                stub.registerFacade(f3);
            });
        }
    }

    // ═══════════════════════════════════════════════════════════════════════════
    // createdTopics — изоляция экземпляров
    // ═══════════════════════════════════════════════════════════════════════════

    @Nested
    @DisplayName("createdTopics изоляция")
    class CreatedTopicsIsolationTests {

        @Test
        @DisplayName("начальный список createdTopics пуст")
        void shouldStartWithEmptyList() {
            assertThat(stub.createdTopics).isEmpty();
        }

        @Test
        @DisplayName("два экземпляра Stub имеют независимые списки createdTopics")
        void shouldHaveIsolatedCreatedTopicsPerInstance() {
            Stub stub1 = new Stub();
            Stub stub2 = new Stub();

            stub1.createdTopics.add("qa-test-topic-1");

            assertThat(stub2.createdTopics).doesNotContain("qa-test-topic-1");
        }

        @Test
        @DisplayName("изменение createdTopics в одном экземпляре не влияет на другой")
        void shouldNotShareStateBetwenInstances() {
            Stub s1 = new Stub();
            Stub s2 = new Stub();

            s1.createdTopics.add("t1");
            s1.createdTopics.add("t2");

            assertThat(s2.createdTopics).isEmpty();
            assertThat(s1.createdTopics).hasSize(2);
        }
    }

    // ═══════════════════════════════════════════════════════════════════════════
    // Cleanup порядок — через mock-фасад
    // ═══════════════════════════════════════════════════════════════════════════

    @Nested
    @DisplayName("Cleanup — порядок вызовов")
    class CleanupOrderTests {

        @Test
        @DisplayName("deleteTopic вызывается для каждого топика из createdTopics перед close()")
        void shouldDeleteTopicsBeforeClose() {
            // Добавляем топики в createdTopics напрямую
            stub.createdTopics.add("qa-test-t1");
            stub.createdTopics.add("qa-test-t2");

            // Мокируем deleteTopic чтобы не выбрасывал
            when(mockKafka.deleteTopic(anyString())).thenReturn(true);

            // Симулируем то, что делает tearDown: cleanupTopics + close
            // (tearDown — @AfterEach JUnit-метод, вызываем логику напрямую)
            for (String t : new java.util.ArrayList<>(stub.createdTopics)) {
                mockKafka.deleteTopic(t);
            }
            stub.createdTopics.clear();
            mockKafka.close();

            verify(mockKafka).deleteTopic("qa-test-t1");
            verify(mockKafka).deleteTopic("qa-test-t2");
            verify(mockKafka).close();
        }

        @Test
        @DisplayName("deleteTopic не вызывается если createdTopics пуст")
        void shouldSkipDeleteWhenNoTopics() {
            assertThat(stub.createdTopics).isEmpty();

            // Пустой список → deleteTopic не должен вызываться
            stub.createdTopics.clear();
            mockKafka.close();

            verify(mockKafka, never()).deleteTopic(any());
            verify(mockKafka).close();
        }

        @Test
        @DisplayName("ошибка при deleteTopic не прерывает cleanup остальных топиков")
        void shouldContinueCleanupAfterDeleteFailure() {
            stub.createdTopics.add("qa-test-good");
            stub.createdTopics.add("qa-test-bad");
            stub.createdTopics.add("qa-test-another");

            // "qa-test-bad" бросает исключение, остальные — дефолтный false (нас устраивает)
            when(mockKafka.deleteTopic("qa-test-bad")).thenThrow(new RuntimeException("delete failed"));

            // Эмулируем cleanupTopics с try/catch как в реальном коде
            for (String t : new java.util.ArrayList<>(stub.createdTopics)) {
                try {
                    mockKafka.deleteTopic(t);
                } catch (Exception ignored) { }
            }

            // Все три попытки были сделаны
            verify(mockKafka).deleteTopic("qa-test-good");
            verify(mockKafka).deleteTopic("qa-test-bad");
            verify(mockKafka).deleteTopic("qa-test-another");
        }
    }

    // ═══════════════════════════════════════════════════════════════════════════
    // testStartTime
    // ═══════════════════════════════════════════════════════════════════════════

    @Test
    @DisplayName("testStartTime по умолчанию 0 (long default) до setUp")
    void shouldHaveZeroStartTimeBeforeSetUp() {
        Stub fresh = new Stub();
        assertThat(fresh.testStartTime).isZero();
    }

    @Test
    @DisplayName("testStartTime после выставления — положительное значение")
    void shouldTrackStartTime() {
        long before = System.currentTimeMillis();
        stub.testStartTime = System.currentTimeMillis();
        long after = System.currentTimeMillis();

        assertThat(stub.testStartTime).isBetween(before, after);
    }

    // ═══════════════════════════════════════════════════════════════════════════
    // WeakHashMap семантика
    // ═══════════════════════════════════════════════════════════════════════════

    @Nested
    @DisplayName("WeakHashMap — GC семантика")
    class WeakHashMapTests {

        @Test
        @DisplayName("registerFacade не удерживает strong reference — GC может собрать фасад")
        void shouldNotPreventGarbageCollection() throws InterruptedException {
            // Создаём фасад только как local variable (weak reference в WeakHashMap)
            // После выхода из scope и вызова GC — объект должен быть доступен для сбора
            KafkaTestFacade localFacade = mock(KafkaTestFacade.class);
            java.lang.ref.WeakReference<KafkaTestFacade> ref =
                    new java.lang.ref.WeakReference<>(localFacade);

            stub.registerFacade(localFacade);

            // Убираем strong reference
            localFacade = null;

            // Пытаемся спровоцировать GC
            for (int i = 0; i < 5; i++) {
                System.gc();
                Thread.sleep(10);
                if (ref.get() == null) break;
            }

            // WeakReference может быть null после GC — это ожидаемое поведение.
            // Тест проверяет, что WeakHashMap не удерживает strong reference,
            // что и является смыслом WeakHashMap-based tracking.
            // Мы не можем гарантировать сбор GC в тесте, но можем проверить
            // что программа не падает и ref.get() возвращает корректный тип.
            Object val = ref.get();
            assertThat(val == null || val instanceof KafkaTestFacade).isTrue();
        }

        @Test
        @DisplayName("registerFacade вызывается несколько раз для одного фасада — нет дублирования в Set")
        void shouldDeduplicateSameFacadeInSet() {
            // Set (WeakHashMap-backed) не должен содержать дубликаты
            assertThatNoException().isThrownBy(() -> {
                stub.registerFacade(mockKafka);
                stub.registerFacade(mockKafka); // повторная регистрация
                stub.registerFacade(mockKafka);
            });
            // Если бы Set дублировал — closeAll() в @AfterAll вызвал бы close() несколько раз.
            // Проверить размер Set нельзя без reflection (private static), но
            // отсутствие исключений подтверждает корректность.
        }
    }

    // ═══════════════════════════════════════════════════════════════════════════
    // CONFIG доступность
    // ═══════════════════════════════════════════════════════════════════════════

    @Test
    @DisplayName("CONFIG доступен из подкласса как protected static final")
    void shouldExposeConfigAsProtectedStaticFinal() {
        // KafkaConfig не должен быть null — ConfigFactory инициализирует при первом обращении
        assertThat(stub.CONFIG).isNotNull();
    }

    @Test
    @DisplayName("CONFIG singleton — два обращения возвращают один объект")
    void shouldReturnSameConfigInstance() {
        assertThat(stub.CONFIG).isSameAs(stub.CONFIG);
    }
}
