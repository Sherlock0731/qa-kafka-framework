package tests;

import lombok.extern.slf4j.Slf4j;

import static tests.KafkaAssertions.*;

/**
 * BaseTest — точка входа для всех тест-классов.
 * <p>
 * Этот класс является точкой сборки (composition root) фреймворка тестирования.
 * Сам по себе не несёт никакой логики — только выстраивает цепочку наследования
 * и подключает {@link KafkaAssertions} через статический импорт.
 *
 * <h3>Цепочка ответственностей</h3>
 * <pre>
 *   KafkaTestBase      — жизненный цикл: @BeforeAll/Each, @AfterEach/All,
 *                        facade (kafka), createdTopics, ALL_FACADES (WeakHashMap)
 *       ↑
 *   KafkaTestHelpers   — domain helpers: createTestTopic, publishMessage,
 *                        consumeMessages, generateTopicName, createNewFacade
 *       ↑
 *   BaseTest           — composition root (этот класс)
 *       ↑
 *   ProducerTests      ← extend BaseTest
 *   ConsumerTests      ← extend BaseTest
 *   TransactionsTests  ← extend BaseTest
 *   ...
 * </pre>
 *
 * <h3>KafkaAssertions</h3>
 * Assertion-методы вынесены в статический утильный класс {@link KafkaAssertions}
 * и подключены через {@code import static}. Это исключает добавление четвёртой
 * ответственности в цепочку наследования.
 * <p>
 * Статический импорт здесь означает, что тест-классы могут вызывать
 * {@code assertPublishSuccess(result)} без дополнительных импортов:
 * <pre>{@code
 * public class ProducerTests extends BaseTest {
 *     @Test
 *     void testPublish() {
 *         String topic = createTestTopic();
 *         PublishResult result = kafka.publish(topic, "key", "value");
 *         assertPublishSuccess(result);          // из KafkaAssertions через BaseTest
 *     }
 * }
 * }</pre>
 *
 * <h3>Метрики</h3>
 * Логирование метрик доступно через {@code kafka.getMetrics()} непосредственно
 * в тест-методах. Для Allure-прикрепления метрик после каждого теста —
 * {@code TestMetricsExtension} (подключён в {@link KafkaTestBase} через
 * {@code @ExtendWith}).
 *
 * @see KafkaTestBase
 * @see KafkaTestHelpers
 * @see KafkaAssertions
 */
@Slf4j
public abstract class BaseTest extends KafkaTestHelpers {

    // Класс намеренно пуст.
    //
    // Вся логика распределена по специализированным классам:
    //   KafkaTestBase    → lifecycle
    //   KafkaTestHelpers → domain helpers
    //   KafkaAssertions  → assertions (static import ниже)
    //
    // Добавление нового поведения:
    //   - Lifecycle-хук        → KafkaTestBase
    //   - Topic/publish/consume → KafkaTestHelpers
    //   - Assertion-метод       → KafkaAssertions (static метод)
    //   - Suite-специфичный    → в конкретном тест-классе (не сюда)
}
