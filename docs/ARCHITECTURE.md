# ARCHITECTURE — Kafka Test Automation Framework

## Общая концепция

Фреймворк построен по **Hexagonal Architecture** (Ports & Adapters) с чистым разделением на четыре слоя:

- **Domain** — бизнес-модели и port-интерфейсы; нет зависимостей на Kafka или инфраструктуру
- **Application** — оркестрация use cases через Domain Ports; тестируется с mock-портами без реального Kafka
- **Infrastructure** — адаптеры к внешним системам (Kafka, Aiven API), реализующие port-интерфейсы
- **Tests** — интеграционные тест-кейсы, использующие `KafkaTestFacade`

---

## Архитектурная диаграмма (Hexagonal)

```
┌────────────────────────────────────────────────────────────────────┐
│                          TEST LAYER                                │
│                                                                    │
│   BaseTest (@ExtendWith: TestMetricsExtension,                     │
│             AllureKafkaListener, KafkaTestExecutionListener)       │
│       ↓                                                            │
│   ProducerTests  ConsumerTests  TransactionsTests  IdempotenceTests│
│   OffsetTests  PartitioningTests  OrderingTests  DlqTests          │
│   PerformanceTests  ErrorHandlingTests  ConsumerGroupTests         │
└───────────────────────────┬────────────────────────────────────────┘
                            │ использует
┌───────────────────────────▼────────────────────────────────────────┐
│                      APPLICATION LAYER                             │
│                                                                    │
│              KafkaTestFacade  ←  единая точка входа               │
│             /        |         \            \                      │
│  MessagePublishing  MessageConsumption  TopicManagement            │
│  Service            Service             Service                    │
│                                                                    │
│  Зависят только от Domain Ports — не импортируют адаптеры          │
└───────────────────────────┬────────────────────────────────────────┘
                            │ вызывает порты
┌───────────────────────────▼────────────────────────────────────────┐
│                        DOMAIN LAYER                                │
│                                                                    │
│  ┌─────────── Models ──────────────┐  ┌──── Ports (Interfaces) ──┐│
│  │ Message         PublishResult   │  │ MessagePublisher         ││
│  │ Topic           ConsumeResult   │  │ MessageConsumer          ││
│  │ Partition       ConsumerGroup   │  │ TopicRepository          ││
│  │ KafkaErrorCategory (13 кат.)   │  │ ConsumerGroupReader      ││
│  └─────────────────────────────────┘  └──────────────────────────┘│
│                                                                    │
│  Domain не зависит ни от чего. Ports — контракты для адаптеров.   │
└───────────────────────────┬────────────────────────────────────────┘
                            │ реализуют
┌───────────────────────────▼────────────────────────────────────────┐
│                    INFRASTRUCTURE LAYER                            │
│                                                                    │
│  ┌─── Kafka Adapters (KafkaAdapterFactory — единая точка) ──────┐ │
│  │ KafkaProducerAdapter   implements MessagePublisher           │ │
│  │ KafkaConsumerAdapter   implements MessageConsumer            │ │
│  │   • consumeAll(): consecutive-empty threshold=3 (fixed)      │ │
│  │   • isAssigned(): local assignment check (fixed)             │ │
│  │   • catch(Exception): interrupt flag restored (fixed)        │ │
│  │ KafkaAdminAdapter      implements TopicRepository            │ │
│  │                        implements ConsumerGroupReader        │ │
│  └──────────────────────────────────────────────────────────────┘ │
│                                                                    │
│  ┌─── Aiven API ─────────────────────────────────────────────────┐ │
│  │ AivenApiController  (REST Assured, Bearer token)             │ │
│  │ dto/AivenTopicListResponseDto                                │ │
│  │ dto/AivenTopicDeleteResponseDto                              │ │
│  └──────────────────────────────────────────────────────────────┘ │
│                                                                    │
│  ┌─── Config ────────────────────────────────────────────────────┐ │
│  │ KafkaConfig (Owner, 40+ props, 4-уровневый приоритет)        │ │
│  │ ConfigFactory (singleton, fail-fast validation)              │ │
│  └──────────────────────────────────────────────────────────────┘ │
│                                                                    │
│  ┌─── Utilities ─────────────────────────────────────────────────┐ │
│  │ KafkaPropertiesBuilder  — SSL/TLS config                     │ │
│  │ KafkaTopicCleanupManager                                     │ │
│  │ KafkaAwaitHelper        — awaitConsumerReady (fixed)         │ │
│  │                           awaitPropagation / awaitMessages   │ │
│  │                           awaitNewMessages / awaitRebalance  │ │
│  │ RetryContext             — Allure attachment helper          │ │
│  └──────────────────────────────────────────────────────────────┘ │
│                                                                    │
│  ┌─── Metrics ───────────────────────────────────────────────────┐ │
│  │ TestMetricsCollector   (ConcurrentHashMap + AtomicLong)      │ │
│  │ TestMetricsExtension   (JUnit5 Extension, per-class Store)   │ │
│  └──────────────────────────────────────────────────────────────┘ │
│                                                                    │
│  ┌─── Exceptions ────────────────────────────────────────────────┐ │
│  │ KafkaTestException (base: errorCategory + context map)       │ │
│  │   ├── ConfigurationException                                 │ │
│  │   ├── KafkaConsumerException                                 │ │
│  │   ├── KafkaProducerException                                 │ │
│  │   ├── KafkaRebalanceException                                │ │
│  │   ├── KafkaTimeoutException                                  │ │
│  │   ├── KafkaTopicManagementException                          │ │
│  │   ├── MessageNotFoundException                               │ │
│  │   └── TestDataException                                      │ │
│  └──────────────────────────────────────────────────────────────┘ │
└───────────────────────────┬────────────────────────────────────────┘
                            │
┌───────────────────────────▼────────────────────────────────────────┐
│                 AIVEN CLOUD KAFKA  (SSL/TLS)                       │
│       kafka-clients 3.6.1: KafkaProducer / KafkaConsumer / Admin  │
└────────────────────────────────────────────────────────────────────┘
```

---

## Структура проекта

```
qa-kafka-framework/
├── src/
│   ├── main/
│   │   ├── java/qa/autotest/framework/
│   │   │   ├── application/service/
│   │   │   │   ├── KafkaTestFacade.java
│   │   │   │   ├── MessageConsumptionService.java
│   │   │   │   ├── MessagePublishingService.java
│   │   │   │   └── TopicManagementService.java
│   │   │   ├── config/
│   │   │   │   ├── ConfigFactory.java
│   │   │   │   └── KafkaConfig.java
│   │   │   ├── domain/
│   │   │   │   ├── model/
│   │   │   │   │   ├── ConsumeResult.java
│   │   │   │   │   ├── ConsumerGroup.java
│   │   │   │   │   ├── KafkaErrorCategory.java
│   │   │   │   │   ├── Message.java
│   │   │   │   │   ├── Partition.java
│   │   │   │   │   ├── PublishResult.java
│   │   │   │   │   └── Topic.java
│   │   │   │   └── port/
│   │   │   │       ├── ConsumerGroupReader.java
│   │   │   │       ├── MessageConsumer.java
│   │   │   │       └── MessagePublisher.java
│   │   │   │       └── TopicRepository.java
│   │   │   ├── exceptions/
│   │   │   │   ├── ConfigurationException.java
│   │   │   │   ├── KafkaConsumerException.java
│   │   │   │   ├── KafkaProducerException.java
│   │   │   │   ├── KafkaRebalanceException.java
│   │   │   │   ├── KafkaTestException.java
│   │   │   │   ├── KafkaTimeoutException.java
│   │   │   │   ├── KafkaTopicManagementException.java
│   │   │   │   ├── MessageNotFoundException.java
│   │   │   │   └── TestDataException.java
│   │   │   ├── infrastructure/
│   │   │   │   ├── api/aiven/
│   │   │   │   │   ├── AivenApiController.java
│   │   │   │   │   └── dto/
│   │   │   │   │       ├── AivenTopicDeleteResponseDto.java
│   │   │   │   │       └── AivenTopicListResponseDto.java
│   │   │   │   ├── kafka/adapter/
│   │   │   │   │   ├── KafkaAdapterFactory.java
│   │   │   │   │   ├── KafkaAdminAdapter.java
│   │   │   │   │   ├── KafkaConsumerAdapter.java   ← 3 исправления
│   │   │   │   │   └── KafkaProducerAdapter.java
│   │   │   │   ├── KafkaPropertiesBuilder.java
│   │   │   │   └── KafkaTopicCleanupManager.java
│   │   │   ├── metrics/
│   │   │   │   ├── TestMetricsCollector.java
│   │   │   │   └── TestMetricsExtension.java
│   │   │   └── utils/
│   │   │       ├── KafkaAwaitHelper.java            ← 1 исправление
│   │   │       └── RetryContext.java
│   │   └── resources/
│   │       ├── config/
│   │       │   ├── ci.properties
│   │       │   ├── default.properties
│   │       │   └── local.properties
│   │       └── logback.xml
│   └── test/
│       └── java/
│           ├── qa/autotest/framework/               # Unit Tests
│           │   ├── application/service/
│           │   │   ├── KafkaTestFacadeTest.java              (20)
│           │   │   ├── MessageConsumptionServiceTest.java    (18)
│           │   │   ├── MessagePublishingServiceTest.java     (13)
│           │   │   └── TopicManagementServiceTest.java       (16)
│           │   ├── config/
│           │   │   └── ConfigFactoryTest.java                (14)
│           │   ├── domain/model/
│           │   │   └── DomainModelTest.java                  (49)
│           │   ├── infrastructure/kafka/adapter/
│           │   │   ├── InfrastructureAdapterPrivateMethodsTest.java (14)
│           │   │   └── KafkaPropertiesBuilderTest.java       (5)
│           │   ├── metrics/
│           │   │   └── TestMetricsCollectorTest.java         (17)
│           │   └── utils/
│           │       └── RetryContextTest.java                 (10)
│           └── tests/                               # Integration Tests
│               ├── BaseTest.java
│               ├── listeners/
│               │   ├── AllureKafkaListener.java
│               │   ├── GlobalCleanupListener.java
│               │   └── KafkaTestExecutionListener.java
│               ├── consumer/ConsumerTests.java               (12)
│               ├── consumergroup/ConsumerGroupTests.java     (1)
│               ├── dlq/DlqTests.java                         (3)
│               ├── errorhandling/ErrorHandlingTests.java     (5)
│               ├── idempotence/IdempotenceTests.java         (7)
│               ├── offset/OffsetTests.java                   (5)
│               ├── ordering/OrderingTests.java               (3)
│               ├── partitioning/PartitioningTests.java       (5)
│               ├── performance/PerformanceTests.java         (4)
│               ├── producer/ProducerTests.java               (12)
│               └── transactions/TransactionsTests.java       (9)
├── .github/workflows/
│   ├── test-all.yml
│   └── test-smoke.yml
├── docker/
├── docs/
│   ├── ARCHITECTURE.md
│   ├── DOCKER.md
│   ├── GITHUB_SECRETS_SETUP.md
│   ├── RUN_INSTRUCTIONS.md
│   ├── SECURITY_GUIDE.md
│   └── TEST_CASES_MATRIX.md
├── pom.xml
├── run-tests.sh
├── README.md
└── SUMMARY.md
```

---

## Слои проекта (детальное описание)

### 1. Domain Layer — `domain/`

Ядро фреймворка, **не зависит** от Kafka, REST API или любых других технических деталей.

#### Models — `domain/model/`

| Класс | Описание | Ключевые методы |
|-------|----------|-----------------|
| **Message** | Сообщение с ключом, значением, headers, partition | `validate()`, `hasHeaders()`, `isCorrelated()`, `isEvent()`, `hasExplicitPartition()` |
| **Topic** | Топик с именем и числом партиций | `validate()`, `isTestTopic()`, `createDlqTopic()`, `createRetryTopic(level)` |
| **Partition** | Метаданные партиции (offset, lag, leader) | `validate()`, `getLag()`, `isLeader()` |
| **ConsumerGroup** | Consumer group state + assignments | `validate()`, `isStable()`, `isEmpty()` |
| **PublishResult** | Результат публикации | `isSuccess()`, `isRetryable()`, `getPartitionInfo()`, `wasTargetedPublish()` |
| **ConsumeResult** | Результат потребления | `hasMessages()`, `isEmpty()`, `isTimeout()`, `isRetryable()`, `consumedFrom(topic)` |
| **KafkaErrorCategory** | Единая таксономия ошибок (13 категорий) | `isRetryable()`, `getDisplayName()`, `fromPublishException()`, `fromConsumeException()` |

**KafkaErrorCategory** объединяет 13 категорий в трёх группах:
- **Общие**: `NETWORK_ERROR`, `TIMEOUT_ERROR`, `AUTHENTICATION_ERROR`, `AUTHORIZATION_ERROR`, `UNKNOWN_ERROR`
- **Produce**: `SERIALIZATION_ERROR`, `TOPIC_NOT_FOUND`, `BROKER_NOT_AVAILABLE`, `BUFFER_EXHAUSTED`
- **Consume**: `DESERIALIZATION_ERROR`, `GROUP_COORDINATION_ERROR`, `OFFSET_OUT_OF_RANGE`

Каждая категория несёт флаг `retryable` и человекочитаемое `displayName` для Allure-отчётов.

#### Ports — `domain/port/`

| Port | Реализуется | Ключевые методы |
|------|-------------|-----------------|
| **MessagePublisher** | `KafkaProducerAdapter` | `publish()`, `publishAsync()`, `publishBatch()`, `publishBatchAsync()`, `flush()`, `close()` |
| **MessageConsumer** | `KafkaConsumerAdapter` | `subscribe()`, `poll()`, `pollMessages()`, `consumeAll()`, `seek()`, `seekToBeginning()`, `seekToEnd()`, `isAssigned()`, `commitSync()`, `commitAsync()`, `close()` |
| **TopicRepository** | `KafkaAdminAdapter` | `createTopic()`, `createTopics()`, `deleteTopic()`, `deleteTopics()`, `exists()`, `getAllTopics()`, `getTopicsByPattern()`, `getPartitions()`, `waitForTopicCreation()`, `close()` |
| **ConsumerGroupReader** | `KafkaAdminAdapter` | `describeConsumerGroup(groupId)` |

`KafkaAdminAdapter` реализует два порта (`TopicRepository + ConsumerGroupReader`) — оба требуют `AdminClient`, что исключает дублирование клиента. `KafkaConsumerAdapter` реализует только `MessageConsumer`, не смешивая consumer и admin операции.

---

### 2. Application Layer — `application/service/`

Оркестрирует use cases через Domain Ports. **Не знает** про Kafka напрямую.

| Сервис | Зависит от | Назначение |
|--------|-----------|-----------| 
| **KafkaTestFacade** | `MessagePublisher`, `MessageConsumer`, `TopicRepository`, `ConsumerGroupReader` | Единая точка входа. Два конструктора: production (через `KafkaAdapterFactory`) и unit-test (mock ports). |
| **MessagePublishingService** | `MessagePublisher` | Публикация одного/пакета сообщений с валидацией и метриками. |
| **MessageConsumptionService** | `MessageConsumer`, `ConsumerGroupReader` | `consumeUntil(Predicate)`, `consumeUntilOrThrow()`, `consumeAll()`, `consumeExactly(n)`. Возвращает `Optional<Message>` или бросает `MessageNotFoundException`. |
| **TopicManagementService** | `TopicRepository` | Создание/удаление топиков, `createTopicWithDlq()`, `createTopicWithRetries(level)`. |

Все сервисы тестируются с Mockito mock-портами (91 unit-тест) без реального Kafka.

---

### 3. Infrastructure Layer — `infrastructure/`

#### Kafka Adapters — `infrastructure/kafka/adapter/`

**`KafkaAdapterFactory`** — единственная точка создания адаптеров. Генерирует уникальный `groupId = base + UUID` на каждый вызов `create()`. `KafkaTestFacade` получает только port-интерфейсы и два лямбды (`closeAll`, `metrics`) — нет импортов конкретных адаптеров.

| Адаптер | Port | Особенности |
|---------|------|-------------|
| **KafkaProducerAdapter** | `MessagePublisher` | ThreadLocal<KafkaProducer> + Set<WeakReference> для трекинга из всех потоков. |
| **KafkaConsumerAdapter** | `MessageConsumer` | ThreadLocal<KafkaConsumer> + Set<WeakReference>. Три исправленных метода (см. ниже). |
| **KafkaAdminAdapter** | `TopicRepository` + `ConsumerGroupReader` | Один `AdminClient` обслуживает оба порта. |

#### Aiven API — `infrastructure/api/aiven/`

`AivenApiController` — HTTP-клиент (REST Assured) к Aiven Management API:
- `getTopicList()` → `GET /v1/project/{project}/service/{service}/topic`
- `deleteTopic(name)` → `DELETE /v1/project/{project}/service/{service}/topic/{name}`
- `deleteAllTestTopics()`, `verifyApiConnection()`

Авторизация: Bearer token. DTO (`AivenTopicListResponseDto`, `AivenTopicDeleteResponseDto`) инкапсулированы внутри infrastructure и не просачиваются в domain.

#### Configuration — `config/`

```java
@Config.LoadPolicy(Config.LoadType.MERGE)
@Config.Sources({
    "system:properties",                       // -Dkafka.bootstrap.servers=...
    "system:env",                              // KAFKA_BOOTSTRAP_SERVERS=...
    "classpath:config/${env}.properties",      // env=local/ci
    "classpath:config/default.properties"
})
public interface KafkaConfig extends Config { ... }
```

**ConfigFactory** создаёт singleton и выполняет `validateRequiredProperties()`:
- `kafka.bootstrap.servers` — всегда обязателен
- 5 SSL-полей — при `security.protocol=SSL` или `SASL_SSL`
- 3 Aiven-поля — проверяются как группа
- Пароли REST API / Schema Registry — при наличии URL

Выбрасывает `ConfigurationException` со списком **всех** отсутствующих свойств в одном сообщении (fail-fast без эффекта «чини по одному»).

#### Metrics — `metrics/`

**`TestMetricsCollector`** — потокобезопасная коллекция метрик: `ConcurrentHashMap` для категорий ошибок, `AtomicLong` для времён и счётчиков. Методы: `recordDuration()`, `recordError()`, `recordTestResult()`.

**`TestMetricsExtension`** — JUnit 5 Extension, хранит один `TestMetricsCollector` per test class в `ExtensionContext.Store`. Namespace: `(TestMetricsExtension.class, testClass)`. Устраняет проблему static-сброса (`resetConfig()`), которая была небезопасна при параллельном запуске. После завершения класса прикрепляет snapshot метрик к Allure.

#### Utilities — `utils/`

**`KafkaAwaitHelper`** — все методы используют `pollInSameThread()` (обязательно для ThreadLocal KafkaConsumer):

| Метод | Назначение |
|-------|-----------|
| `awaitConsumerReady(kafka, topic, timeoutSec)` | subscribe + ожидание partition assignment через `isAssigned()` (**исправлено**) |
| `awaitPropagation(kafka, topic, expectedCount, timeoutSec)` | flush + ожидание доступности топика через AdminClient |
| `awaitMessages(kafka, topic, expectedCount, timeoutSec)` | subscribe + seekToBeginning + накопительный polling |
| `awaitNewMessages(kafka, expectedCount, timeoutSec)` | накопительный polling с текущей позиции |
| `awaitRebalance(kafka, topic, timeoutSec)` | двухфазное ожидание: local assignment + broker STABLE state |

**`RetryContext`** — прикрепляет JSON-контекст retry-операций к Allure-отчёту.

---

### 4. Test Layer — `src/test/java/tests/`

#### BaseTest — жизненный цикл

```
@ExtendWith(TestMetricsExtension.class,    ← per-class metrics, ExtensionContext.Store
            AllureKafkaListener.class,     ← 7 категорий сбоев
            KafkaTestExecutionListener.class)

ALL_FACADES = Collections.synchronizedSet(Collections.newSetFromMap(new WeakHashMap<>()))
    WeakHashMap: GC собирает facade до @AfterAll → предотвращает накопление при большом числе тестов

@BeforeAll setUpAll()
    └── ConfigFactory инициализация, логирование параметров подключения

@BeforeEach setUp()
    ├── kafka = new KafkaTestFacade(CONFIG)          ← production constructor
    │       └── KafkaAdapterFactory.create(config)
    │               └── groupId = base + UUID        ← уникальный per-facade
    ├── ALL_FACADES.add(kafka)
    └── инициализация publishService / consumeService / topicService

@AfterEach tearDown()
    ├── удаление созданных топиков (retry 5×, backoff 1-5 с)
    └── kafka.close()  → закрывает ThreadLocal адаптеры текущего потока

@AfterAll globalCleanup()
    └── synchronized(ALL_FACADES) { ALL_FACADES.forEach(f -> f.closeAll()) }
            closeAll() → закрывает адаптеры из ВСЕХ потоков через WeakReference tracking
```

#### Паттерн «Consumer-First Initialization»

Все consume-тесты инициализируют consumer до публикации для корректного rebalance:

```java
// 1. Создание топика
Topic topic = topicService.createTopic("test-topic", 1, 1);

// 2. Подписка и ожидание готовности consumer
KafkaAwaitHelper.awaitConsumerReady(kafka, topic.getName(), 15);
// → внутри: kafka.subscribe(topic) + poll() + isAssigned()

// 3. Публикация
publishService.publish(message);
publishService.flush();
KafkaAwaitHelper.awaitPropagation(kafka, topic.getName(), 1, 15);

// 4. Потребление
Optional<Message> result = consumeService.consumeUntil(
    msg -> msg.getKey().equals("expected-key"),
    Duration.ofSeconds(30)
);
assertThat(result).isPresent();
```

#### Test Listeners

| Listener | Тип | Назначение |
|----------|-----|-----------|
| **AllureKafkaListener** | `TestWatcher` | Категоризация сбоев (7 категорий), прикрепление метрик к Allure |
| **KafkaTestExecutionListener** | `BeforeTestExecutionCallback` | Логирование начала/окончания каждого теста |
| **GlobalCleanupListener** | `AfterAllCallback` | Глобальный cleanup всех ресурсов после сьюта |

**Категории сбоев в `AllureKafkaListener.categorizeFailure()`:**

| Паттерн исключения | Allure Category |
|-------------------|-----------------| 
| `KafkaTimeoutException` / `TimeoutException` | `INFRASTRUCTURE_TIMEOUT` |
| `SSLException` / класс содержит "ssl" | `INFRASTRUCTURE_SSL` |
| `RebalanceInProgressException` / `CommitFailedException` | `KAFKA_REBALANCE` |
| `NetworkException` / "Connection" в сообщении | `INFRASTRUCTURE_CONNECTION` |
| `AssertionError` | `TEST_ASSERTION_FAILURE` |
| `SerializationException` | `KAFKA_SERIALIZATION` |
| `InterruptedException` | `TEST_INTERRUPTED` |
| иное | `UNKNOWN_<ClassName>` |

---

## Потоки данных

### Публикация сообщения

```
Test
  ↓ kafka.publish(message)
KafkaTestFacade
  ↓ publishingService.publish(message)
MessagePublishingService
  ↓ message.validate()
  ↓ messagePublisher.publish(message)          ← port call
KafkaProducerAdapter
  ↓ toProducerRecord(message)
  ↓ producer.send(record).get()
  ↑ PublishResult.success(partition, offset)   ← или failureFrom(e)
MessagePublishingService
  ↓ metrics.recordDuration()
  ↑ PublishResult
Test
  ↓ assertThat(result.isSuccess()).isTrue()
```

### Потребление с ожиданием готовности

```
Test
  ↓ KafkaAwaitHelper.awaitConsumerReady(kafka, topicName, 15)
KafkaAwaitHelper
  ↓ kafka.subscribe(topicName)
  ↓ Awaitility.await().pollInSameThread().until(() -> {
        kafka.poll(300ms);                     ← движет JoinGroup/SyncGroup протокол
        return kafka.isAssigned();             ← local set, no network
    })
  ↑ возврат когда assignment != empty

Test
  ↓ consumeService.consumeUntil(predicate, timeout)
MessageConsumptionService
  ↓ loop: messageConsumer.poll(pollTimeout)    ← port call
      ↓ if (any message matches predicate) → return Optional.of(msg)
  ↓ timeout → return Optional.empty()
Test
  ↓ assertThat(result).isPresent()
```

### Ошибка с категоризацией

```
KafkaProducerAdapter
  ↓ producer.send() бросает TimeoutException
  ↓ KafkaErrorCategory.fromPublishException(e) → TIMEOUT_ERROR (retryable=true)
  ↑ PublishResult(success=false, category=TIMEOUT_ERROR)

Test падает → AllureKafkaListener.testFailed()
  ↓ categorizeFailure(cause) → "INFRASTRUCTURE_TIMEOUT"
  ↓ attachErrorCategory("INFRASTRUCTURE_TIMEOUT")
  ↓ metrics.recordError("TIMEOUT_ERROR")
  ↓ Allure label: [Error Category: INFRASTRUCTURE_TIMEOUT]
```

---

## Безопасность и SSL/TLS

| `security.protocol` | Описание | Материал |
|---------------------|----------|----------|
| **SSL** | Mutual TLS | keystore.p12 (PKCS12) + truststore.jks (JKS) |
| **SASL_SSL** | SASL поверх TLS | То же + SASL credentials |
| **PLAINTEXT** | Без шифрования | Не требуется (только local dev) |

`KafkaPropertiesBuilder.configureSecurity()` добавляет SSL-свойства только при `SSL` или `SASL_SSL`, что исключает их появление в PLAINTEXT-окружении.

Файлы сертификатов: Aiven Console → Service → Overview → Download SSL certificates.

---

## Параллельное выполнение

Управление через два уровня:

**JUnit Platform** (`junit-platform.properties`):
```properties
junit.jupiter.execution.parallel.enabled = false   # default: sequential
junit.jupiter.execution.parallel.config.strategy = fixed
junit.jupiter.execution.parallel.config.fixed.parallelism = 1
```

**Maven Surefire** (профиль `parallel`):
```xml
<junit.jupiter.execution.parallel.enabled>true</junit.jupiter.execution.parallel.enabled>
<junit.jupiter.execution.parallel.mode.default>concurrent</junit.jupiter.execution.parallel.mode.default>
<junit.jupiter.execution.parallel.mode.classes.default>concurrent</junit.jupiter.execution.parallel.mode.classes.default>
<junit.jupiter.execution.parallel.config.fixed.max-pool-size>${thread.count}</junit.jupiter.execution.parallel.config.fixed.max-pool-size>
```

**Thread safety компонентов:**

| Компонент | Механизм | Статус |
|-----------|----------|--------|
| KafkaConsumer / KafkaProducer | ThreadLocal | ✅ каждый поток — свой клиент |
| TestMetricsCollector | ConcurrentHashMap + AtomicLong | ✅ lock-free |
| TestMetricsExtension | ExtensionContext.Store per class | ✅ нет static state |
| BaseTest.ALL_FACADES | synchronized(WeakHashMap) | ✅ |
| ConfigFactory | static read-only после init | ✅ (resetConfig не вызывать параллельно) |
| closeAll() | WeakReference tracking | ✅ очищает все потоки |

**Уникальность ресурсов:**
- Топики: `qa-test-{UUID}` per test
- Consumer groups: `base-{UUID}` per KafkaTestFacade instance

---

## Архитектурные принципы

### Dependency Rule

```
Tests → Application → Domain ← Infrastructure
          ↓                        ↑
       использует порты       реализует порты
```

Зависимости направлены внутрь. Domain не импортирует ничего из Kafka или инфраструктуры.

### SOLID-статус

| Принцип | Статус | Детали |
|---------|--------|--------|
| **SRP** | ✅ | `KafkaConsumerAdapter` — только `KafkaConsumer`; `KafkaAdminAdapter` — только `AdminClient` |
| **OCP** | ✅ | Новый тип сериализации → новый `MessageSerializer` port без изменения адаптеров |
| **LSP** | ✅ | Все port-реализации заменяемы mock-объектами в unit-тестах |
| **ISP** | ✅ | `ConsumerGroupReader` отделён от `MessageConsumer`; `isAssigned()` в правильном интерфейсе |
| **DIP** | ✅ | `KafkaTestFacade` не импортирует ни один конкретный адаптер |

### Fail-Fast Configuration

`ConfigFactory.validateRequiredProperties()` собирает список всех отсутствующих свойств и бросает одно `ConfigurationException` до первого обращения к Kafka.

### Explicit Error Handling

- `PublishResult` / `ConsumeResult` вместо выброса исключений из adapter
- `KafkaErrorCategory` — единая классификация; `isRetryable()` для принятия решения о retry
- `Optional<Message>` для «не найдено» vs `MessageNotFoundException` для «обязательно должно быть»
- `InterruptedException`-протокол: `Thread.interrupted()` + `Thread.currentThread().interrupt()` везде где catch(Exception)


## Ссылки

- [RUN_INSTRUCTIONS.md](RUN_INSTRUCTIONS.md) — запуск локально и в CI
- [SECURITY_GUIDE.md](SECURITY_GUIDE.md) — настройка SSL/TLS сертификатов
- [DOCKER.md](DOCKER.md) — Docker-образ для тестов
- [TEST_CASES_MATRIX.md](TEST_CASES_MATRIX.md) — полная матрица тест-кейсов
- [GITHUB_SECRETS_SETUP.md](GITHUB_SECRETS_SETUP.md) — настройка GitHub Actions secrets
