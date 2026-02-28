package tests;

import io.qameta.allure.Step;
import qa.autotest.framework.application.service.KafkaTestFacade;
import qa.autotest.framework.domain.model.*;

import java.time.Duration;
import java.util.List;
import java.util.UUID;

/**
 * KafkaTestHelpers — Ответственность: domain-ориентированные вспомогательные методы.
 * <p>
 * Единственная задача: скрывать рутину создания топиков, публикации и потребления
 * сообщений за читаемыми методами с {@code @Step}-аннотациями для Allure.
 * <p>
 * Жизненный цикл (инициализация {@code kafka}, очистка топиков) делегируется
 * родительскому {@link KafkaTestBase}.
 *
 * <h3>Что здесь, что нет</h3>
 * <ul>
 *   <li>Здесь: {@code createTestTopic}, {@code publishMessage}, {@code consumeMessages},
 *       {@code generateTopicName}, {@code createNewFacade}.</li>
 *   <li>Не здесь: {@code @Before/@After} — это {@link KafkaTestBase}.</li>
 *   <li>Не здесь: assertion-методы — это {@link KafkaAssertions}.</li>
 * </ul>
 */
public abstract class KafkaTestHelpers extends KafkaTestBase {

    /**
     * Создаёт топик с авто-именем и 2 партициями.
     */
    @Step("Create test topic (auto name, 2 partitions)")
    public String createTestTopic() {
        return createTestTopic(generateTopicName("auto"), 2);
    }

    /**
     * Создаёт топик с авто-именем и заданным числом партиций.
     */
    @Step("Create test topic with {partitions} partitions")
    public String createTestTopic(int partitions) {
        return createTestTopic(generateTopicName("auto"), partitions);
    }

    /**
     * Создаёт топик с заданным именем и 2 партициями (лимит Aiven free-tier).
     */
    @Step("Create test topic: {topicName}")
    public String createTestTopic(String topicName) {
        return createTestTopic(topicName, 2);
    }

    /**
     * Создаёт топик с заданным именем и числом партиций.
     * Replication factor берётся из конфигурации {@code kafka.test.topic.replication.factor}.
     *
     * @param topicName  имя топика
     * @param partitions число партиций
     * @return имя топика (для использования в тесте)
     */
    @Step("Create test topic: {topicName} with {partitions} partitions")
    public String createTestTopic(String topicName, int partitions) {
        Topic topic = Topic.builder()
                .name(topicName)
                .partitionCount(partitions)
                .replicationFactor(CONFIG.testTopicReplicationFactor())
                .build();

        kafka.createTopic(topic);
        createdTopics.add(topicName);
        kafka.waitForTopic(topicName, 10);

        return topicName;
    }

    /**
     * Создаёт топик вместе с DLQ-топиком ({@code topicName + "-dlq"}).
     *
     * @param topicName базовое имя топика
     * @return {@code true} если оба топика созданы успешно
     */
    @Step("Create topic with DLQ: {topicName}")
    public boolean createTopicWithDlq(String topicName) {
        boolean created = kafka.createTopicWithDlq(topicName);
        if (created) {
            createdTopics.add(topicName);
            createdTopics.add(topicName + "-dlq");
        }
        return created;
    }

    /**
     * Публикует сообщение по ключу и значению.
     */
    @Step("Publish message to {topicName}")
    public PublishResult publishMessage(String topicName, String key, String value) {
        return kafka.publish(topicName, key, value);
    }

    /**
     * Публикует доменное сообщение.
     */
    @Step("Publish domain message")
    public PublishResult publishMessage(Message message) {
        return kafka.publish(message);
    }

    /**
     * Публикует пакет доменных сообщений.
     */
    @Step("Publish batch of messages")
    public List<PublishResult> publishBatch(List<Message> messages) {
        return kafka.publishBatch(messages);
    }

    /**
     * Подписывается на топик, перематывает в начало и считывает до {@code expectedCount}
     * сообщений за {@code timeout}.
     *
     * @return список сообщений; пустой список при ошибке или таймауте
     */
    @Step("Consume messages from {topicName}")
    public List<Message> consumeMessages(String topicName, int expectedCount, Duration timeout) {
        kafka.subscribe(topicName);
        kafka.seekToBeginning();
        ConsumeResult result = kafka.pollMessages(expectedCount, timeout);
        return result.isSuccess() ? result.getMessages() : List.of();
    }

    /**
     * Подписывается на топик, перематывает в начало и считывает все доступные сообщения
     * (до {@code maxMessages}) за 10 секунд.
     *
     * @return список сообщений; пустой список при ошибке
     */
    @Step("Consume all messages from {topicName}")
    public List<Message> consumeAllMessages(String topicName, int maxMessages) {
        kafka.subscribe(topicName);
        kafka.seekToBeginning();
        ConsumeResult result = kafka.consumeAll(maxMessages, Duration.ofSeconds(10));
        return result.isSuccess() ? result.getMessages() : List.of();
    }

    /**
     * Проверяет наличие не менее {@code expectedCount} сообщений в топике за {@code timeout}.
     *
     * @return {@code true} если сообщения получены в ожидаемом количестве
     */
    @Step("Wait for {expectedCount} messages in {topicName}")
    public boolean waitForMessages(String topicName, int expectedCount, Duration timeout) {
        kafka.subscribe(topicName);
        kafka.seekToBeginning();
        ConsumeResult result = kafka.pollMessages(expectedCount, timeout);
        return result.isSuccess() && result.getMessageCount() >= expectedCount;
    }

    /**
     * Создаёт новый {@link KafkaTestFacade} с уникальным consumer group ID.
     * <p>
     * Используется в rebalance-тестах, где нужен второй независимый consumer.
     * Новый facade регистрируется в {@code ALL_FACADES} через {@link KafkaTestBase}
     * и будет закрыт в {@code @AfterAll}.
     *
     * <pre>{@code
     * kafka.close();
     * kafka = createNewFacade();
     * KafkaAwaitHelper.awaitRebalance(kafka, topicName, 15);
     * }</pre>
     */
    public KafkaTestFacade createNewFacade() {
        return registerFacade(new KafkaTestFacade(CONFIG));
    }

    /**
     * Генерирует уникальное имя топика по схеме {@code qa-test-{prefix}-{UUID}}.
     * <p>
     * UUID гарантирует отсутствие коллизий между параллельными тестами и
     * отдельными test runs. Префикс {@code qa-test} используется
     * {@code GlobalCleanupListener} для выборочного удаления тестовых топиков.
     */
    public String generateTopicName(String prefix) {
        return String.format("qa-test-%s-%s", prefix, UUID.randomUUID());
    }
}
