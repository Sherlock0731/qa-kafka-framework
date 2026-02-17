# SUMMARY — Kafka Test Automation Framework

## Обзор проекта

**Kafka Test Automation Framework** — enterprise-grade фреймворк для комплексного тестирования Apache Kafka, реализованный на Java 17. Разработан для работы с Aiven Cloud Kafka (SSL/TLS), обеспечивает надёжный параллельный запуск тестов без утечек памяти и предоставляет детальную Allure-отчётность.

**Версия:** 1.0.0  
**Целевая платформа:** Aiven Cloud Kafka (free tier и выше)  
**Артефакт:** `qa.autotest:kafka-test-framework:1.0.0`

---

## Что реализовано

### Ядро фреймворка (`src/main/java`)

**Конфигурация:**
- `KafkaConfig` — интерфейс Owner с 40+ параметрами (connection, SSL, producer, consumer, test, Aiven API)
- `ConfigFactory` — фабрика конфигурации с поддержкой merge-стратегии: system props → env vars → env.properties → default.properties
- `src/main/resources/config/` — профили `default.properties`, `local.properties`, `ci.properties`

**Kafka-клиенты:**
- `KafkaProducerManager` — thread-safe producer с `ThreadLocal` инстансами, `WeakReference`-трекингом всех созданных инстансов и методом `closeAll()` для очистки из всех потоков. Поддерживает: синхронную/асинхронную отправку, batch-отправку, headers (message-id, correlation-id, event-type), метрики `TestMetricsCollector`, детальную категоризацию ошибок (timeout, serialization, network)
- `KafkaConsumerManager` — thread-safe consumer с теми же паттернами защиты. Поддерживает: `initConsumer`, `subscribeToTopics`, `poll`/`pollAll`, ручной commit (sync/async/per-partition), seek (beginning/end), позиции offset
- `KafkaTopicManager` — AdminClient-обёртка: создание уникальных топиков (UUID-суффикс), топиков с заданными партициями, удаление, листинг
- `KafkaTopicCleanupManager` — отдельный менеджер очистки топиков после тестов
- `KafkaPropertiesBuilder` — статическая утилита: `buildBaseProperties()` + `configureSecurity()` без дублирования кода между producer/consumer

**Вспомогательные компоненты:**
- `AivenApiController` — REST Assured клиент к Aiven API v1: `getTopicList()`, `deleteTopic()` с Bearer-token авторизацией и Allure-логированием запросов/ответов
- `AsyncTestHelper` — `pollWithRetry()` (retry-polling без Thread.sleep, через Awaitility), `pollAllWithRetry()`, `waitFor()`
- `TestDataGenerator` — генерация тестовых данных: `generateMessage()`, `generateMessages()`, `generateMessageWithKey()`, с UUID-ключами и Instant-временными метками
- `RetryContext` — обёртка для retry-попыток с Allure-аттачментами (атрибуты попытки, backoff, результат)
- `TestMetricsCollector` — потокобезопасный сборщик метрик: durations (producer_send_sync, consumer_poll), категории ошибок, счётчики pass/fail
- `EventDrivenHelper` — паттерны event-driven тестирования

**Иерархия исключений:**
- `KafkaTestException` (base) — с `ErrorType` enum (TIMEOUT, NETWORK, CONFIGURATION, SERIALIZATION, REBALANCE, TOPIC_MANAGEMENT, UNKNOWN) и `addContext()` для накопления контекста
- `KafkaProducerException` — фабричные методы `timeout()`, `serialization()`, `network()`
- `KafkaConsumerException` — `notInitialized()` с именем потока
- `KafkaTimeoutException`, `KafkaRebalanceException`, `KafkaTopicManagementException`, `TestDataException`

**DTO:**
- `KafkaMessageDto` — `@Builder`, `@Data`; поля: topic, key, value, partition, offset, headers (Map), messageId, correlationId, eventType, timestamp
- `ConsumerRecordDto` — mapped из `ConsumerRecord` с headers, timestampType
- `TopicPartitionDto`, `AivenTopicListResponseDto`, `AivenTopicDeleteResponseDto`

---

### Тесты (`src/test/java`)

**BaseTest:**
- `@BeforeEach` — инициализация `KafkaProducerManager`, `KafkaConsumerManager`, `KafkaTopicManager`; регистрация в статических `synchronized Set` для global tracking
- `@AfterEach` — удаление тестовых топиков с retry (5 попыток, linear backoff 1–5с), закрытие ресурсов текущего потока
- `@AfterAll globalCleanup()` — `closeAll()` на всех зарегистрированных менеджерах; полностью предотвращает утечки памяти в ForkJoinPool
- Вспомогательные методы: `createTestTopic()`, `createTestTopic(partitions)`, `createAndTrackTopic()`, `trackTopicForCleanup()` — все с retry-механизмом (5 попыток, exponential backoff 3–15с)

**JUnit Listeners:**
- `AllureKafkaListener` (`TestWatcher`) — обогащает отчёт: категоризация сбоев (timeout, SSL, rebalance, connection, assertion), Allure-аттачменты с деталями теста и метриками, timestamp
- `KafkaTestExecutionListener` — логирование этапов жизненного цикла теста
- `GlobalCleanupListener` — финальная очистка ресурсов на уровне launcher

---

### Тест-сьюты (66 тест-кейсов)

| Класс | Тегов | Кейсов | Ключевые сценарии |
|-------|-------|--------|-------------------|
| `ProducerTests` | `producer`, `critical` | 12 | Single/batch sync, async, headers, key-based routing, acks=all, retry, timeout, buffer, метрики |
| `ConsumerTests` | `consumer`, `critical` | 12 | earliest/latest offset reset, headers, multi-consumer group, pause/resume, max.poll.records, seek, rebalance, lag |
| `TransactionsTests` | `transactions` | 9 | Commit, rollback, read_committed isolation, exactly-once, multi-topic, timeout, producer-consumer coordination, recovery after failure |
| `IdempotenceTests` | `idempotence` | 7 | Идемпотентный producer, дедупликация через message-id, exactly-once, producer ID + sequence number, retry без дублей |
| `OffsetTests` | `offset` | 5 | Manual sync commit, async commit, per-partition commit, auto-commit, offset при rebalance |
| `PartitioningTests` | `partitioning` | 5 | Распределение без ключа (round-robin), hash по ключу, смена числа партиций |
| `PerformanceTests` | `performance` | 4 | Throughput, E2E latency, optimal batch size, consumer lag under load |
| `OrderingTests` | `ordering`, `critical` | 3 | Порядок в одной партиции, между партициями, для keyed messages |
| `DlqTests` | `dlq`, `critical` | 3 | Poison pill + retry N раз → DLQ, routing с метаданными ошибки, reprocessing из DLQ |
| `ErrorHandlingTests` | `error-handling` | 5 | Serialization error, deserialization error, network recovery, broker unavailable, topic not found |
| `ConsumerGroupTests` | `consumer-group` | 1 | Consumer group rebalance |

**Smoke-тесты** (тег `smoke`, 7 кейсов): TC-001 (single message), TC-009 (read from beginning), TC-021 (ordering), TC-049 (DLQ), и др.

---

## Ключевые технические решения

### Предотвращение утечек памяти
Проблема: при параллельном выполнении через JUnit 5 ForkJoinPool consumer/producer создаются в рабочих потоках, которые завершаются до вызова `@AfterEach`. Стандартный `ThreadLocal.remove()` не достигает этих потоков.

Решение: `WeakReference`-трекинг — каждый созданный инстанс добавляется в `Collections.synchronizedSet(new HashSet<WeakReference<...>>())`. Метод `closeAll()` итерирует все живые ссылки и вызывает `close()`. `@AfterAll globalCleanup()` вызывает `closeAll()` на всех зарегистрированных менеджерах.

### Consumer rebalance в облачном Kafka
Проблема: Aiven Cloud Kafka имеет более высокую сетевую задержку, из-за чего rebalance занимает дольше. Тесты, которые подписывались на топик и сразу отправляли сообщения, теряли их.

Решение: паттерн "consumer-first initialization" — в каждом тесте сначала `initConsumer(topic)` → `waitFor(5)` → `poll(2)` (pre-warm) → `waitFor(1)` → только потом отправка. Polling выполняется через `AsyncTestHelper.pollWithRetry()` с таймаутом до 90 секунд.

### Retry при создании топиков
Aiven иногда возвращает ошибки при быстром создании топиков (rate limiting). Решение: `createTopicWithRetry()` с 5 попытками и exponential backoff (3с, 6с, 9с, 12с, 15с), `RetryContext` сохраняет детали каждой попытки в Allure.

---

## CI/CD

GitHub Actions (`workflows/test-all.yml`):
1. Checkout + JDK 17 setup
2. Maven build + test (с Aiven SSL секретами из GitHub Secrets)
3. Allure report generation
4. Deploy на GitHub Pages

GitHub Secrets: `KAFKA_BOOTSTRAP_SERVERS`, `KAFKA_SSL_TRUSTSTORE_PASSWORD`, `KAFKA_SSL_KEYSTORE_PASSWORD`, `KAFKA_SSL_KEY_PASSWORD`, `AIVEN_API_TOKEN`, `AIVEN_PROJECT_NAME`, `AIVEN_SERVICE_NAME`

---

## Структура модулей Maven

```
groupId:    qa.autotest
artifactId: kafka-test-framework
version:    1.0.0
packaging:  jar
```

Профили: `local` (default), `ci`, `parallel`, `security-check`, а также профили по тест-группам: `producer`, `consumer`, `idempotence`, `ordering`, `offset`, `error-handling`, `partitioning`, `consumer-group`, `dlq`, `smoke`, `critical`.

Параллельное выполнение управляется через `junit.jupiter.execution.parallel.*` properties (в `junit-platform.properties`) и Maven Surefire `systemPropertyVariables`. По умолчанию выключено; включается профилем `parallel` с параметром `-Dthread.count=N`.

Flaky test retry: `rerunFailingTestsCount=2` в Maven Surefire.

---

## Файловая структура

```
qa-kafka-framework/
├── pom.xml
├── README.md
├── SUMMARY.md
├── categories.json                         # Allure failure categories
├── run-tests.sh                            # Bash-скрипт запуска
├── setup-git-hooks.sh
├── docker/
│   ├── Dockerfile
│   ├── docker-compose.yml
│   ├── docker-run.sh
│   └── README.md
├── docs/
│   ├── ARCHITECTURE.md
│   ├── DOCKER.md
│   ├── GITHUB_SECRETS_SETUP.md
│   ├── RUN_INSTRUCTIONS.md
│   ├── SECURITY_GUIDE.md
│   └── TEST_CASES_MATRIX.md
└── src/
    ├── main/
    │   ├── java/qa/autotest/
    │   │   ├── app/dto/                    # 5 DTO-классов
    │   │   └── framework/                  # 20+ классов фреймворка
    │   └── resources/
    │       ├── logback.xml
    │       └── config/
    │           ├── default.properties
    │           ├── local.properties
    │           └── ci.properties
    └── test/
        ├── java/tests/                     # 11 тест-классов + BaseTest + 3 listener'а
        └── resources/
            ├── junit-platform.properties
            └── META-INF/services/
```
