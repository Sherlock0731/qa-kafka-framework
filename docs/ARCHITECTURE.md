# ARCHITECTURE — Kafka Test Automation Framework

## Общая концепция

Фреймворк построен по принципу разделения ответственности: **ядро** (`src/main`) содержит переиспользуемую инфраструктуру, **тесты** (`src/test`) — исключительно тест-логику. Всё взаимодействие с Kafka идёт через менеджеры-фасады, скрывающие детали протокола и обеспечивающие надёжность при работе с облачным брокером.

---

## Диаграмма компонентов

```
┌─────────────────────────────────────────────────────────────────┐
│                         TEST LAYER                              │
│  BaseTest  →  ProducerTests / ConsumerTests / TransactionsTests │
│            →  IdempotenceTests / OffsetTests / PartitioningTests│
│            →  OrderingTests / DlqTests / PerformanceTests       │
│            →  ErrorHandlingTests / ConsumerGroupTests           │
└──────────────────────────┬──────────────────────────────────────┘
                           │
┌──────────────────────────▼──────────────────────────────────────┐
│                      FRAMEWORK LAYER                            │
│                                                                 │
│  KafkaProducerManager   KafkaConsumerManager   KafkaTopicManager│
│        │                       │                      │         │
│        └───────────────────────┴──────────────────────┘         │
│                           │                                     │
│                  KafkaPropertiesBuilder                         │
│                  (shared SSL + base config)                     │
│                                                                 │
│  AivenApiController    AsyncTestHelper    TestDataGenerator     │
│  TestMetricsCollector  RetryContext       EventDrivenHelper     │
└──────────────────────────┬──────────────────────────────────────┘
                           │
┌──────────────────────────▼──────────────────────────────────────┐
│                     INFRASTRUCTURE                              │
│                                                                 │
│  KafkaConfig (Owner)     ConfigFactory                          │
│  KafkaTestException hierarchy (7 типов)                         │
│  DTO: KafkaMessageDto, ConsumerRecordDto, TopicPartitionDto     │
└──────────────────────────┬──────────────────────────────────────┘
                           │
┌──────────────────────────▼──────────────────────────────────────┐
│               AIVEN CLOUD KAFKA  (SSL/TLS)                      │
│   kafka-clients 3.6.1: KafkaProducer, KafkaConsumer, Admin     │
└─────────────────────────────────────────────────────────────────┘
```

---

## Конфигурационный слой

### KafkaConfig (Owner library)

```java
@Config.LoadPolicy(Config.LoadType.MERGE)
@Config.Sources({
    "system:properties",
    "system:env",
    "classpath:config/${env}.properties",
    "classpath:config/default.properties"
})
public interface KafkaConfig extends Config { ... }
```

Интерфейс содержит 40+ свойств, сгруппированных по разделам:

- **Connection** — `kafkaBootstrapServers()`, `securityProtocol()`
- **SSL** — `sslTruststoreLocation/Password/Type`, `sslKeystoreLocation/Password/Type`, `sslKeyPassword`
- **API** — `kafkaRestApiUrl/Username/Password`, `schemaRegistryUrl/Username/Password`
- **Producer** — `producerAcks()` (default: `all`), `producerRetries()` (3), `producerEnableIdempotence()` (true), `producerMaxInFlightRequests()` (5), `producerBatchSize()` (16384), `producerLingerMs()` (10), `producerRequestTimeoutMs()` (30000)
- **Consumer** — `consumerGroupIdBase()` (qa-test-group), `consumerAutoOffsetReset()` (earliest), `consumerEnableAutoCommit()` (false), `consumerSessionTimeoutMs()` (30000), `consumerMaxPollIntervalMs()` (300000), `consumerMaxPollRecords()` (500)
- **Test** — `testTopicPrefix()` (qa-test), `testTopicPartitions()` (3), `testTopicReplicationFactor()` (1), `dlqTopicSuffix()` (-dlq), `testTimeoutSeconds()` (30), `cleanupTopics()` (true)
- **Aiven** — `aivenApiUrl()` (https://api.aiven.io/v1), `aivenApiToken()`, `aivenProjectName()`, `aivenServiceName()`

### ConfigFactory

Создаёт единственный инстанс `KafkaConfig` через `ConfigFactory.create(KafkaConfig.class)`.

---

## Kafka-клиенты

### KafkaPropertiesBuilder

Утилитный класс (только статические методы), устраняет дублирование между producer и consumer:

```java
// Общая база
Properties props = KafkaPropertiesBuilder.buildBaseProperties(config);
// → bootstrap.servers

// SSL/TLS конфигурация
KafkaPropertiesBuilder.configureSecurity(props, config);
// → security.protocol, ssl.truststore.*, ssl.keystore.*, ssl.key.password
```

Применяется в `KafkaProducerManager`, `KafkaConsumerManager` и `KafkaTopicManager`.

### KafkaProducerManager

**Паттерн хранения:**
```
producerThreadLocal: ThreadLocal<KafkaProducer> — инстанс для текущего потока
allProducers:        synchronized Set<WeakReference<KafkaProducer>> — трекинг всех инстансов
```

**Создание producer** (`ThreadLocal.withInitial(this::createProducer)`):
- Base properties + serializers (StringSerializer key/value)
- acks=all, retries=3, enable.idempotence=true, max.in.flight=5
- batch.size=16384, linger.ms=10, request.timeout.ms=30000
- SSL через `KafkaPropertiesBuilder.configureSecurity()`
- Регистрация `WeakReference` в `allProducers`

**Ключевые методы:**

| Метод | Описание |
|-------|----------|
| `sendSync(KafkaMessageDto)` | Синхронная отправка с `future.get()`; обновляет offset/partition в DTO; метрики; категоризация ошибок (timeout/serialization/network) |
| `sendAsync(KafkaMessageDto)` | Асинхронная отправка с callback; обновляет offset/partition |
| `sendBatch(List<KafkaMessageDto>)` | Последовательный `sendSync()` для каждого сообщения |
| `flush()` | `producer.flush()` для сброса буфера |
| `close()` | Закрытие producer текущего потока + `ThreadLocal.remove()` |
| `closeAll()` | **Критический метод**: итерирует `allProducers`, вызывает `close(5s)` на каждом живом инстансе; очищает set |
| `getTrackedProducerCount()` | Количество живых WeakReference (для мониторинга) |

**Категоризация ошибок sendSync:**
```
exception contains "timeout"      → KafkaProducerException.timeout()
exception contains "serialization" → KafkaProducerException.serialization()
exception contains "connection/network" → KafkaProducerException.network()
иначе                              → KafkaProducerException(UNKNOWN) + context
```

### KafkaConsumerManager

**Паттерн хранения:**
```
consumerThreadLocal: ThreadLocal<KafkaConsumer>
groupIdThreadLocal:  ThreadLocal<String>
allConsumers:        synchronized Set<WeakReference<KafkaConsumer>>
```

**Создание consumer** (явно через `createConsumer(groupId)`):
- Base properties + StringDeserializer key/value
- GROUP_ID = `consumerGroupIdBase + "-" + UUID.randomUUID()`
- AUTO_OFFSET_RESET = earliest, ENABLE_AUTO_COMMIT = false
- SESSION_TIMEOUT_MS = 30000, MAX_POLL_INTERVAL_MS = 300000
- MAX_POLL_RECORDS = 500, FETCH_MIN_BYTES = 1, FETCH_MAX_WAIT_MS = 500

**Ключевые методы:**

| Метод | Описание |
|-------|----------|
| `initConsumer(topic)` | Создание consumer с UUID-groupId, `subscribe([topic])`, сохранение в ThreadLocal |
| `initConsumer(topic, groupId)` | То же с явным groupId |
| `subscribeToTopics(List<String>)` | Подписка на несколько топиков |
| `poll(timeoutSeconds)` | Разовый poll; метрики; `KafkaTimeoutException` при таймауте |
| `pollAll(maxAttempts)` | Опрос до опустошения топика |
| `commitSync()` / `commitAsync()` | Ручной commit |
| `commitOffset(topic, partition, offset)` | Commit конкретного offset (+1) |
| `seekToBeginning(topic)` / `seekToEnd(topic)` | Перемотка |
| `getPosition(topic, partition)` | Текущая позиция |
| `close()` / `closeAll()` | Аналогично ProducerManager |
| `getGroupId()` | Текущий groupId из ThreadLocal |

### KafkaTopicManager

Обёртка над `Admin` (AdminClient):
- `createUniqueTopic()` → `qa-test-{UUID}` с `testTopicPartitions` партициями
- `createTopicWithPartitions(int)` → `qa-test-{UUID}` с заданным числом партиций
- `createTopic(name, partitions, replicationFactor)` → произвольный топик
- `deleteTopic(name)` → `admin.deleteTopics()`
- `listTopics()` → `Set<String>` имён топиков

AdminClient создаётся с SSL-конфигурацией и `REQUEST_TIMEOUT_MS=30000`.

---

## Слой тестов

### BaseTest — жизненный цикл

```
@BeforeAll setUpAll()
    └── Логирование параметров SSL и security protocol

@BeforeEach setUp()
    ├── new KafkaProducerManager(CONFIG)  → ALL_PRODUCER_MANAGERS.add()
    ├── new KafkaConsumerManager(CONFIG)  → ALL_CONSUMER_MANAGERS.add()
    ├── new KafkaTopicManager(CONFIG)     → ALL_TOPIC_MANAGERS.add()
    └── createdTopics = new ArrayList<>()

@AfterEach tearDown()
    ├── Удаление topicов из createdTopics (с retry 5 раз, linear backoff 1-5с)
    ├── safeClose(producerManager)
    ├── safeClose(consumerManager)
    └── safeClose(topicManager)

@AfterAll globalCleanup()
    ├── for manager in ALL_PRODUCER_MANAGERS: manager.closeAll()
    ├── for manager in ALL_CONSUMER_MANAGERS: manager.closeAll()
    ├── for manager in ALL_TOPIC_MANAGERS: manager.close()
    └── Очистка всех Set-ов
```

**Создание топиков с retry:**
```
createTopicWithRetry(supplier)
    attempts: 1..5
    backoff: 3000 * attempt ms (3s, 6s, 9s, 12s, 15s)
    каждая попытка: RetryContext.attachToAllure("Topic Creation")
```

### Паттерн "Consumer-First Initialization"

Все тесты, требующие чтения сообщений, инициализируют consumer **до** отправки:

```java
// 1. Инициализация consumer и ожидание завершения rebalance
consumerManager.initConsumer(topic);
AsyncTestHelper.waitFor(5);       // ожидание rebalance (5с)
consumerManager.poll(2);          // pre-warm poll (trigger partition assignment)
AsyncTestHelper.waitFor(1);       // стабилизация

// 2. Только теперь отправка сообщений
producerManager.sendBatch(messages);
producerManager.flush();
AsyncTestHelper.waitFor(2);

// 3. Polling с retry
List<ConsumerRecordDto> records = AsyncTestHelper.pollWithRetry(consumerManager, 30, expectedCount);
```

### AllureKafkaListener

`TestWatcher` implementation:

```
testSuccessful → attachTestInfo(PASSED) + recordTestResult(true)
testFailed     → categorizeFailure() + recordError(category) + recordCategory() + recordTestResult(false)
testAborted    → логирование + attachTestInfo(BROKEN)
testDisabled   → логирование
```

**Категоризация сбоев (`categorizeFailure`):**

| Паттерн в исключении | Категория Allure |
|---------------------|-----------------|
| TimeoutException | `KAFKA_TIMEOUT` |
| SSLException / certificate | `SSL_ERROR` |
| RebalanceException / rebalance | `CONSUMER_REBALANCE` |
| Connection / NetworkException | `CONNECTION_ERROR` |
| AssertionError | `TEST_ASSERTION` |
| иное | `UNKNOWN_ERROR` |

---

## Утилиты

### AsyncTestHelper

```java
// Polling с retry (основной метод)
pollWithRetry(consumer, timeoutSeconds, expectedMinMessages)
    // Каждая итерация: consumer.poll(10s)
    // Между итерациями: Thread.sleep(2000)
    // Выход: size >= expectedMinMessages или истёк timeoutSeconds

// Полный drain
pollAllWithRetry(consumer, timeoutSeconds)

// Ожидание (секунды)
waitFor(int seconds)
```

### TestDataGenerator

```java
generateMessage(topic)
    // key = UUID, value = "Test message " + UUID, timestamp = Instant.now()
    
generateMessages(topic, count)
    // count сообщений с порядковыми key-ами
    
generateMessageWithKey(topic, key)
    // фиксированный ключ для тестирования партиционирования
```

---

## Иерархия исключений

```
KafkaTestException (RuntimeException)
    ├── ErrorType: TIMEOUT | NETWORK | CONFIGURATION | SERIALIZATION
    │              | REBALANCE | TOPIC_MANAGEMENT | UNKNOWN
    ├── addContext(key, value) → накопление Map<String,String> контекста
    │
    ├── KafkaProducerException
    │       factory: timeout(topic, ms, cause)
    │                serialization(topic, cause)
    │                network(topic, cause)
    │
    ├── KafkaConsumerException
    │       factory: notInitialized(threadName)
    │
    ├── KafkaTimeoutException(operation, timeoutMs, cause)
    ├── KafkaRebalanceException
    ├── KafkaTopicManagementException
    └── TestDataException
```

---

## Безопасность и SSL

Поддерживаемые конфигурации:

| `security.protocol` | Описание |
|---------------------|----------|
| `SSL` | Mutual TLS (keystore + truststore) — для Aiven |
| `SASL_SSL` | SASL поверх TLS |
| `PLAINTEXT` | Без шифрования (только local dev) |

Keystore типы: `PKCS12` (keystore, от Aiven), `JKS` (truststore, от Aiven).

`KafkaPropertiesBuilder.configureSecurity()` добавляет SSL-свойства только при `SSL` или `SASL_SSL` протоколах.

---

## Параллельное выполнение

Управляется через два уровня:

**JUnit Platform** (`junit-platform.properties`):
```properties
junit.jupiter.execution.parallel.enabled = false       # по умолчанию
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

**Важно:** Параллельные тесты должны использовать уникальные топики (обеспечивается UUID-суффиксом) и уникальные consumer group ID (UUID в `initConsumer()`).

---

## Aiven API Controller

```java
// GET /v1/project/{project}/service/{service}/topic
List<String> topics = controller.getTopicList();

// DELETE /v1/project/{project}/service/{service}/topic/{topic}
controller.deleteTopic(topicName);
```

Авторизация: `Authorization: Bearer {aivenApiToken}`

RestAssured RequestSpec логирует все запросы/ответы через Allure filter (`allure-rest-assured`).

---

## TestMetricsCollector

Потокобезопасный (`AtomicLong`, `ConcurrentHashMap`):

```
recordDuration(operation, millis)   // "producer_send_sync", "consumer_poll"
recordError(category)               // инкремент счётчика по категории
recordCategory(category)            // для Allure attachment
recordTestResult(passed)            // pass/fail счётчики
```

Данные прикрепляются к Allure-отчёту в `AllureKafkaListener.attachTestInfo()`.
