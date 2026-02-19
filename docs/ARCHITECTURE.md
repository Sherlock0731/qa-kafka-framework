# ARCHITECTURE — Kafka Test Automation Framework

## Общая концепция

Фреймворк построен по **Hexagonal Architecture** (Ports & Adapters) с чистым разделением слоёв:
- **Domain** — бизнес-логика и модели, независимые от инфраструктуры
- **Application** — use cases и оркестрация через сервисы
- **Infrastructure** — адаптеры к внешним системам (Kafka, Aiven API)
- **Tests** — тест-кейсы, использующие фасад фреймворка

---

## Архитектурная диаграмма (Hexagonal)

```
┌────────────────────────────────────────────────────────────────┐
│                         TEST LAYER                             │
│   BaseTest → ProducerTests / ConsumerTests / TransactionsTests │
│           → IdempotenceTests / OffsetTests / PartitioningTests │
│           → OrderingTests / DlqTests / PerformanceTests        │
│           → ErrorHandlingTests / ConsumerGroupTests            │
└─────────────────────────┬──────────────────────────────────────┘
                          │
┌─────────────────────────▼──────────────────────────────────────┐
│                   APPLICATION LAYER                            │
│                 (Orchestration Services)                       │
│                                                                │
│   KafkaTestFacade ← единая точка входа для тестов             │
│        ↓                                                       │
│   MessagePublishingService    MessageConsumptionService        │
│   TopicManagementService                                       │
│                                                                │
│   Зависят только от Domain Ports (интерфейсов)                │
└─────────────────────────┬──────────────────────────────────────┘
                          │
┌─────────────────────────▼──────────────────────────────────────┐
│                      DOMAIN LAYER                              │
│                  (Business Logic Core)                         │
│                                                                │
│  ┌────────── Models ─────────┐   ┌─── Ports (Interfaces) ───┐ │
│  │ Message                   │   │ MessagePublisher         │ │
│  │ Topic                     │   │ MessageConsumer          │ │
│  │ Partition                 │   │ TopicRepository          │ │
│  │ ConsumerGroup             │   └──────────────────────────┘ │
│  │ PublishResult             │                                │
│  │ ConsumeResult             │   ┌─── Exceptions ──────────┐ │
│  │ KafkaErrorCategory        │   │ MessageNotFoundException │ │
│  └───────────────────────────┘   └──────────────────────────┘ │
│                                                                │
│  • Domain не зависит от infrastructure                         │
│  • Ports определяют контракты для адаптеров                   │
└─────────────────────────┬──────────────────────────────────────┘
                          │
┌─────────────────────────▼──────────────────────────────────────┐
│                  INFRASTRUCTURE LAYER                          │
│                (Adapters to External Systems)                  │
│                                                                │
│  ┌─── Kafka Adapters ────────────────────────────────────┐    │
│  │ KafkaProducerAdapter  implements MessagePublisher     │    │
│  │ KafkaConsumerAdapter  implements MessageConsumer      │    │
│  │ KafkaAdminAdapter     implements TopicRepository      │    │
│  └────────────────────────────────────────────────────────┘   │
│                                                                │
│  ┌─── Aiven API Client ──────────────────────────────────┐    │
│  │ AivenApiClient                                        │    │
│  │   ├── dto/AivenTopicListResponseDto                   │    │
│  │   └── dto/AivenTopicDeleteResponseDto                 │    │
│  └────────────────────────────────────────────────────────┘   │
│                                                                │
│  ┌─── Configuration ──────────────────────────────────────┐   │
│  │ KafkaConfig (Owner interface)                          │   │
│  │ ConfigFactory (singleton creator + validation)         │   │
│  │ ConfigurationException                                 │   │
│  └────────────────────────────────────────────────────────┘   │
│                                                                │
│  ┌─── Legacy Exceptions (to be migrated) ────────────────┐   │
│  │ KafkaTestException hierarchy:                          │   │
│  │   KafkaProducerException, KafkaConsumerException,      │   │
│  │   KafkaTimeoutException, KafkaRebalanceException,      │   │
│  │   KafkaTopicManagementException, TestDataException     │   │
│  └────────────────────────────────────────────────────────┘   │
│                                                                │
│  ┌─── Utilities ──────────────────────────────────────────┐   │
│  │ KafkaPropertiesBuilder (SSL/TLS config)                │   │
│  │ KafkaTopicCleanupManager                               │   │
│  │ KafkaAwaitHelper (async wait utilities)                │   │
│  │ RetryContext (Allure attachment helper)                │   │
│  │ TestMetricsCollector                                   │   │
│  └────────────────────────────────────────────────────────┘   │
└─────────────────────────┬──────────────────────────────────────┘
                          │
┌─────────────────────────▼──────────────────────────────────────┐
│              AIVEN CLOUD KAFKA (SSL/TLS)                       │
│   kafka-clients 3.6.1: KafkaProducer, KafkaConsumer, Admin    │
└────────────────────────────────────────────────────────────────┘
```

---

## Слои проекта (детальное описание)

### 1. Domain Layer — `domain/`

Ядро бизнес-логики, **не зависит** от Kafka, REST API или любых других технических деталей.

#### Models — `domain/model/`

Доменные сущности с валидацией и бизнес-методами:

| Класс | Описание | Ключевые методы |
|-------|----------|-----------------|
| **Message** | Сообщение в системе | `validate()`, `hasHeaders()`, `isCorrelated()`, `isEvent()`, `hasExplicitPartition()` |
| **Topic** | Топик Kafka | `validate()`, `isTestTopic()`, `createDlqTopic()`, `createRetryTopic(level)` |
| **Partition** | Метаданные партиции | `validate()`, `getLag()`, `isLeader()` |
| **ConsumerGroup** | Consumer group | `validate()`, `isStable()`, `isEmpty()` |
| **PublishResult** | Результат публикации | `isSuccess()`, `isRetryable()`, `getPartitionInfo()`, `wasTargetedPublish()` |
| **ConsumeResult** | Результат потребления | `hasMessages()`, `isEmpty()`, `isTimeout()`, `isRetryable()`, `consumedFrom(topic)` |
| **KafkaErrorCategory** | Единая таксономия ошибок | `isRetryable()`, `getDisplayName()`, `fromPublishException()`, `fromConsumeException()` |

**KafkaErrorCategory** — ключевое нововведение, объединяющее 13 категорий ошибок:
- **Общие**: `NETWORK_ERROR`, `TIMEOUT_ERROR`, `AUTHENTICATION_ERROR`, `AUTHORIZATION_ERROR`, `UNKNOWN_ERROR`
- **Produce**: `SERIALIZATION_ERROR`, `TOPIC_NOT_FOUND`, `BROKER_NOT_AVAILABLE`, `BUFFER_EXHAUSTED`
- **Consume**: `DESERIALIZATION_ERROR`, `GROUP_COORDINATION_ERROR`, `OFFSET_OUT_OF_RANGE`

Каждая категория несёт флаг `retryable` и человекочитаемое имя `displayName` для Allure-отчётов.

#### Ports — `domain/port/`

Интерфейсы, определяющие контракты для infrastructure adapters:

| Port | Реализуется | Методы |
|------|-------------|--------|
| **MessagePublisher** | `KafkaProducerAdapter` | `publish()`, `publishAsync()`, `publishBatch()`, `publishBatchAsync()`, `flush()`, `close()` |
| **MessageConsumer** | `KafkaConsumerAdapter` | `subscribe()`, `poll()`, `pollMessages()`, `consumeAll()`, `seek()`, `seekToBeginning()`, `seekToEnd()`, `commitSync()`, `commitAsync()`, `getConsumerGroup()`, `close()` |
| **TopicRepository** | `KafkaAdminAdapter` | `createTopic()`, `createTopics()`, `deleteTopic()`, `deleteTopics()`, `exists()`, `getAllTopics()`, `getTopicsByPattern()`, `getPartitions()`, `waitForTopicCreation()`, `close()` |

#### Exceptions — `domain/exception/`

| Исключение | Назначение |
|-----------|-----------|
| **MessageNotFoundException** | Выбрасывается `consumeUntilOrThrow()` когда сообщение не найдено за отведённое время. Содержит: `conditionDescription`, `attemptsExhausted`, `pollTimeout`, `totalMessagesInspected`, `getTotalTimeSpent()` |

---

### 2. Application Layer — `application/service/`

Оркестрирует use cases через Domain Ports. **Не знает** про Kafka напрямую.

| Сервис | Зависит от | Назначение |
|--------|-----------|-----------|
| **KafkaTestFacade** | `MessagePublisher`, `MessageConsumer`, `TopicRepository` | Единая точка входа для тестов. Делегирует вызовы сервисам ниже. |
| **MessagePublishingService** | `MessagePublisher` | Публикация одного/пакета сообщений с валидацией и метриками |
| **MessageConsumptionService** | `MessageConsumer` | Polling с условиями: `consumeUntil(Predicate)`, `consumeAll()`, `consumeExactly(n)`. Методы возвращают `Optional<Message>` или выбрасывают `MessageNotFoundException`. |
| **TopicManagementService** | `TopicRepository` | Создание/удаление топиков, включая DLQ и retry-топики. `createTopicWithDlq()`, `createTopicWithRetries(level)` |

**Ключевая особенность**: все сервисы тестируемы с mock-портами без реального Kafka.

---

### 3. Infrastructure Layer — `infrastructure/`

Реализации портов и адаптеры к внешним системам.

#### Kafka Adapters — `infrastructure/kafka/adapter/`

| Адаптер | Implements | Особенности |
|---------|-----------|-------------|
| **KafkaProducerAdapter** | `MessagePublisher` | ThreadLocal<KafkaProducer> + Set<WeakReference> для трекинга всех producer'ов. `closeAll()` закрывает все инстансы из всех потоков. Ошибки маппятся через `KafkaErrorCategory.fromPublishException()`. |
| **KafkaConsumerAdapter** | `MessageConsumer` | ThreadLocal<KafkaConsumer> + Set<WeakReference>. `closeAll()` для глобальной очистки. Ошибки через `KafkaErrorCategory.fromConsumeException()`. |
| **KafkaAdminAdapter** | `TopicRepository` | Использует `AdminClient` для управления топиками. Реализует все методы из `TopicRepository`. |

**ThreadLocal + WeakReference паттерн** предотвращает утечки ресурсов при параллельном выполнении тестов в ForkJoinPool.

#### Aiven API Client — `infrastructure/api/aiven/`

```
infrastructure/api/aiven/
    ├── AivenApiClient.java          # REST-клиент к Aiven Management API
    └── dto/
        ├── AivenTopicListResponseDto.java
        └── AivenTopicDeleteResponseDto.java
```

**AivenApiClient** — HTTP-клиент (RestAssured) для управления топиками через Aiven API:
- `getTopicList()` → `GET /v1/project/{project}/service/{service}/topic`
- `deleteTopic(name)` → `DELETE /v1/project/{project}/service/{service}/topic/{name}`
- `deleteTopics(names)`, `deleteAllTestTopics()`, `verifyApiConnection()`

Авторизация: Bearer token. DTO живут в пакете адаптера и не просачиваются в domain.

#### Configuration — `config/`

```java
@Config.LoadPolicy(Config.LoadType.MERGE)
@Config.Sources({
    "system:properties",      // -Dkafka.bootstrap.servers=...
    "system:env",             // KAFKA_BOOTSTRAP_SERVERS=...
    "classpath:config/${env}.properties",  // env=local/ci
    "classpath:config/default.properties"
})
public interface KafkaConfig extends Config { ... }
```

**ConfigFactory** — создаёт singleton и вызывает `validateRequiredProperties()`:
- Проверяет наличие `kafka.bootstrap.servers`
- Проверяет SSL-свойства (5 полей) при `security.protocol=SSL/SASL_SSL`
- Проверяет Aiven-свойства (3 поля) как группу
- Проверяет REST API / Schema Registry пароли при наличии URL

Выбрасывает **ConfigurationException** со списком всех отсутствующих свойств и гайдом по их заполнению.

**Группы свойств в KafkaConfig:**

| Группа | Ключевые свойства | Описание |
|--------|------------------|----------|
| **Connection** | `kafkaBootstrapServers`, `securityProtocol` | Обязательно. |
| **SSL** | `sslTruststoreLocation/Password/Type`, `sslKeystoreLocation/Password/Type`, `sslKeyPassword` | 5 полей. Обязательны при SSL/SASL_SSL. |
| **Producer** | `producerAcks` (all), `producerRetries` (3), `producerEnableIdempotence` (true), `producerBatchSize` (16384) | Настройки публикации. |
| **Consumer** | `consumerGroupIdBase` (qa-test-group), `consumerAutoOffsetReset` (earliest), `consumerEnableAutoCommit` (false), `consumerMaxPollRecords` (500) | Настройки потребления. |
| **Test** | `testTopicPrefix` (qa-test), `testTopicPartitions` (3), `dlqTopicSuffix` (-dlq), `testTimeoutSeconds` (30) | Управление тестовыми топиками. |
| **Aiven** | `aivenApiUrl`, `aivenApiToken`, `aivenProjectName`, `aivenServiceName` | 3 последних обязательны как группа. |
| **REST API** | `kafkaRestApiUrl`, `kafkaRestApiUsername`, `kafkaRestApiPassword` | Password обязателен при наличии URL. |
| **Schema Registry** | `kafkaSchemaRegistryUrl`, `kafkaSchemaRegistryUsername`, `kafkaSchemaRegistryPassword` | Password обязателен при наличии URL. |

#### Utilities — `utils/`, `kafka/`, `metrics/`

| Класс | Назначение |
|-------|-----------|
| **KafkaPropertiesBuilder** | Статические методы: `buildBaseProperties()`, `configureSecurity()`. Устраняет дублирование SSL-конфигурации между producer/consumer/admin. |
| **KafkaTopicCleanupManager** | Удаление тестовых топиков через Aiven API или AdminClient. |
| **KafkaAwaitHelper** | Async wait utilities для стабилизации rebalance. |
| **RetryContext** | Прикрепление контекста retry к Allure-отчёту. |
| **TestMetricsCollector** | Потокобезопасная коллекция метрик (AtomicLong, ConcurrentHashMap). `recordDuration()`, `recordError()`, `recordTestResult()`. |

#### Legacy Exceptions — `exceptions/`

> **Примечание**: Эти исключения унаследованы из старой архитектуры и будут мигрированы в `domain/exception` в следующих итерациях.

```
KafkaTestException (RuntimeException)
    ├── ErrorType: TIMEOUT | NETWORK | CONFIGURATION | SERIALIZATION
    │              | REBALANCE | TOPIC_MANAGEMENT | UNKNOWN
    ├── addContext(key, value) → накопление контекста
    │
    ├── KafkaProducerException
    ├── KafkaConsumerException
    ├── KafkaTimeoutException
    ├── KafkaRebalanceException
    ├── KafkaTopicManagementException
    └── TestDataException
```

---

### 4. Test Layer — `src/test/java/tests/`

#### BaseTest — жизненный цикл

```
@BeforeAll setUpAll()
    └── Инициализация ConfigFactory и логирование параметров

@BeforeEach setUp()
    ├── KafkaTestFacade facade = new KafkaTestFacade(CONFIG, consumerGroupId)
    │       └── создаёт KafkaProducerAdapter, KafkaConsumerAdapter, KafkaAdminAdapter
    │           и инжектит их в сервисы
    │
    ├── MessagePublishingService publishService = facade.getPublishService()
    ├── MessageConsumptionService consumeService = facade.getConsumeService()
    └── TopicManagementService topicService = facade.getTopicService()

@AfterEach tearDown()
    ├── Удаление созданных топиков (с retry 5 раз, backoff 1-5с)
    └── facade.close() → закрывает все адаптеры текущего потока

@AfterAll globalCleanup()
    └── ALL_FACADES.forEach(f -> f.closeAll())
            → вызывает closeAll() на всех адаптерах → очистка всех ThreadLocal
```

#### Паттерн "Consumer-First Initialization"

Все consume-тесты инициализируют consumer до отправки для корректного rebalance:

```java
// 1. Создание топика
Topic topic = topicService.createTopic(...);

// 2. Подписка consumer и ожидание rebalance
consumeService.subscribe(topic);
KafkaAwaitHelper.waitFor(5);  // ожидание rebalance
consumeService.poll(Duration.ofSeconds(2));  // pre-warm
KafkaAwaitHelper.waitFor(1);  // стабилизация

// 3. Публикация
publishService.publishBatch(messages);
publishService.flush();
KafkaAwaitHelper.waitFor(2);

// 4. Потребление
Optional<Message> result = consumeService.consumeUntil(
    msg -> msg.getKey().equals("target-key"),
    Duration.ofSeconds(30)
);
```

#### Test Listeners

| Listener | Назначение |
|----------|-----------|
| **AllureKafkaListener** | `TestWatcher` impl: категоризация сбоев, прикрепление метрик к Allure, `recordTestResult()` |
| **KafkaTestExecutionListener** | Логирование начала/окончания тестов |
| **GlobalCleanupListener** | `@AfterAll` глобальный cleanup всех ресурсов |

**Категоризация сбоев в AllureKafkaListener:**

| Паттерн исключения | Allure Category |
|-------------------|-----------------|
| `TimeoutException` | KAFKA_TIMEOUT |
| `SSLException` / `certificate` | SSL_ERROR |
| `RebalanceException` / `rebalance` | CONSUMER_REBALANCE |
| `Connection` / `NetworkException` | CONNECTION_ERROR |
| `AssertionError` | TEST_ASSERTION |
| прочее | UNKNOWN_ERROR |

---

## Потоки данных

### Публикация сообщения

```
Test
  ↓ publishService.publish(message)
Application/MessagePublishingService
  ↓ message.validate()
  ↓ messagePublisher.publish(message) [port call]
Infrastructure/KafkaProducerAdapter
  ↓ toProducerRecord(message)
  ↓ producer.send(record).get()
  ↓ PublishResult.failureFrom() при ошибке
  ↑ PublishResult.success() при успехе
Application/MessagePublishingService
  ↓ metrics.recordDuration()
  ↑ возвращает PublishResult
Test
  ↓ Assertions.assertTrue(result.isSuccess())
```

### Потребление с условием

```
Test
  ↓ consumeService.consumeUntil(predicate, timeout)
Application/MessageConsumptionService
  ↓ for (attempt in 1..maxAttempts)
      ↓ result = messageConsumer.poll(pollTimeout) [port call]
      ↓ if (any message matches predicate) → return Optional.of(msg)
  ↓ все попытки исчерпаны → return Optional.empty()
  ↑ Optional<Message>
Test
  ↓ assertTrue(result.isPresent())
```

### Ошибка с категоризацией

```
Infrastructure/KafkaProducerAdapter
  ↓ producer.send() выбрасывает TimeoutException
  ↓ catch (Exception e)
      ↓ PublishResult.failureFrom(message, e.getMessage(), e)
          ↓ KafkaErrorCategory.fromPublishException(e)
              ↓ instanceof TimeoutException → TIMEOUT_ERROR
          ↑ category=TIMEOUT_ERROR, retryable=true
      ↑ PublishResult(success=false, category=TIMEOUT_ERROR)
  ↑ return PublishResult
Application/MessagePublishingService
  ↑ передаёт результат выше
Test
  ↓ if (!result.isSuccess())
      ↓ AllureKafkaListener ловит сбой теста
          ↓ categorizeFailure() → attachCategory("KAFKA_TIMEOUT")
          ↓ metrics.recordError("TIMEOUT_ERROR")
```

---

## Безопасность и SSL/TLS

Поддерживаемые протоколы:

| `security.protocol` | Описание | Keystore/Truststore |
|---------------------|----------|---------------------|
| **SSL** | Mutual TLS | keystore.p12 (PKCS12), truststore.jks (JKS) |
| **SASL_SSL** | SASL поверх TLS | То же + SASL credentials |
| **PLAINTEXT** | Без шифрования | Не требуется (только local dev) |

**KafkaPropertiesBuilder.configureSecurity()** добавляет SSL-свойства только при `SSL` или `SASL_SSL`:
```java
security.protocol = SSL
ssl.truststore.location = /path/to/ca-chain.jks
ssl.truststore.password = ***
ssl.truststore.type = JKS
ssl.keystore.location = /path/to/service.keystore.p12
ssl.keystore.password = ***
ssl.keystore.type = PKCS12
ssl.key.password = ***
```

Файлы сертификатов получаются из Aiven Console → Service → Overview → Download SSL certificates.

---

## Параллельное выполнение

Управление через два уровня:

**JUnit Platform** (`junit-platform.properties`):
```properties
junit.jupiter.execution.parallel.enabled = false  # default
junit.jupiter.execution.parallel.config.strategy = fixed
junit.jupiter.execution.parallel.config.fixed.parallelism = 1
```

**Maven Surefire** (профиль `parallel`):
```xml
<properties>
    <junit.jupiter.execution.parallel.enabled>true</junit.jupiter.execution.parallel.enabled>
    <junit.jupiter.execution.parallel.mode.default>concurrent</junit.jupiter.execution.parallel.mode.default>
    <junit.jupiter.execution.parallel.mode.classes.default>concurrent</junit.jupiter.execution.parallel.mode.classes.default>
    <junit.jupiter.execution.parallel.config.fixed.max-pool-size>${thread.count}</junit.jupiter.execution.parallel.config.fixed.max-pool-size>
</properties>
```

**Безопасность параллельных тестов:**
1. Уникальные топики: `qa-test-{UUID}` генерируется для каждого теста
2. Уникальные consumer groups: `qa-test-group-{UUID}` при `initConsumer()`
3. ThreadLocal для producer/consumer: каждый поток имеет свой Kafka-клиент
4. WeakReference tracking: `closeAll()` очищает клиенты из всех потоков

---

## Архитектурные принципы

### 1. Dependency Rule (Hexagonal Architecture)

```
Test → Application → Domain ← Infrastructure
         ↓ depends on      ↑ implements
      Domain Ports       Domain Ports
```

Зависимости направлены внутрь:
- Infrastructure зависит от Domain (implements Ports)
- Application зависит от Domain (uses Ports)
- Domain **не зависит** ни от чего

### 2. Single Responsibility

Каждый слой решает одну задачу:
- **Domain**: бизнес-правила, валидация
- **Application**: оркестрация use cases
- **Infrastructure**: интеграция с внешними системами
- **Tests**: проверка требований

### 3. Inversion of Control

Application не создаёт адаптеры напрямую — они инжектятся через конструктор:
```java
public MessagePublishingService(MessagePublisher publisher) { ... }
```

Это позволяет подменить `KafkaProducerAdapter` на mock в unit-тестах сервисов.

### 4. Fail-Fast Configuration

`ConfigFactory.validateRequiredProperties()` проверяет все обязательные свойства до первого обращения к Kafka. При отсутствии — выбрасывает `ConfigurationException` со списком недостающих полей и инструкциями по их заполнению.

### 5. Explicit Error Handling

- `PublishResult` / `ConsumeResult` вместо выброса исключений
- `KafkaErrorCategory` для категоризации всех ошибок
- `Optional<Message>` для "не найдено" vs `MessageNotFoundException` для "обязательно должно быть"
- `isRetryable()` флаг для принятия решения о retry

### 6. Thread Safety

- ThreadLocal для Kafka-клиентов
- ConcurrentHashMap / AtomicLong в метриках
- synchronized блоки при доступе к `Set<WeakReference>`
- `closeAll()` для глобальной очистки из всех потоков

---

## Недостатки текущей архитектуры (Technical Debt)

> Эти элементы будут устранены в следующих итерациях рефакторинга.

1. **Legacy Exceptions** (`framework/exceptions/`) — не в Domain layer
2. **Отсутствие RetryPolicy в Domain** — retry-логика размазана по сервисам
3. **Отсутствие EventBus для test listeners** — прямая зависимость от Allure
4. **Неполное покрытие unit-тестами сервисов** — нужны тесты с mock-портами
5. **KafkaTopicCleanupManager** использует и Aiven API, и AdminClient — дублирование
6. **KafkaAwaitHelper.waitFor()** — примитивный sleep вместо condition wait

---

## Miграция старой архитектуры (до Hexagonal)

До рефакторинга фреймворк использовал "Manager" паттерн:
```
KafkaProducerManager  → KafkaProducerAdapter (теперь)
KafkaConsumerManager  → KafkaConsumerAdapter (теперь)
KafkaTopicManager     → KafkaAdminAdapter (теперь)
```

Ключевые изменения:
- Менеджеры были application layer, но работали с Kafka напрямую
- Теперь адаптеры в infrastructure, сервисы в application
- Добавлен domain layer с моделями и портами
- DTO убраны из app-пакета в infrastructure
- Единая таксономия ошибок `KafkaErrorCategory`

---

## Дальнейшее развитие

### Краткосрочные задачи (Sprint 1-2)
- [x] Создать `TopicRepository` port
- [x] Переместить `AivenApiController` в infrastructure
- [x] Объединить `PublishResult.ErrorCategory` и `ConsumeResult.ErrorCategory` → `KafkaErrorCategory`
- [ ] Удалить `@Deprecated` классы (старые DTO, `AivenApiController` из `framework/api`)
- [ ] Мигрировать legacy exceptions в `domain/exception`
- [ ] Покрыть application services unit-тестами с mock-портами

### Среднесрочные задачи (Sprint 3-4)
- [ ] Добавить `RetryPolicy` в domain
- [ ] Реализовать EventBus для decoupling listeners
- [ ] Заменить `Thread.sleep()` на condition-based wait
- [ ] Унифицировать `KafkaTopicCleanupManager` (один путь через порт)

### Долгосрочные задачи (Strategic)
- [ ] Поддержка других брокеров (Confluent Cloud, MSK)
- [ ] Поддержка Schema Registry (Avro/Protobuf)
- [ ] Транзакционный API для multi-topic atomic writes
- [ ] Performance benchmarking framework
- [ ] Cloud-native configuration (Vault, AWS Secrets Manager)

---

## Ссылки на дополнительную документацию

- [RUN_INSTRUCTIONS.md](RUN_INSTRUCTIONS.md) — как запускать тесты локально и в CI
- [SECURITY_GUIDE.md](SECURITY_GUIDE.md) — настройка SSL/TLS сертификатов
- [DOCKER.md](DOCKER.md) — Docker-образ для тестов
- [TEST_CASES_MATRIX.md](TEST_CASES_MATRIX.md) — полная матрица тест-кейсов
- [GITHUB_SECRETS_SETUP.md](GITHUB_SECRETS_SETUP.md) — настройка GitHub Actions secrets
