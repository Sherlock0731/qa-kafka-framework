# Kafka Test Automation Framework

[![Java](https://img.shields.io/badge/Java-17-orange.svg)](https://www.oracle.com/java/)
[![JUnit](https://img.shields.io/badge/JUnit-5.10.1-red.svg)](https://junit.org/junit5/)
[![Kafka](https://img.shields.io/badge/kafka--clients-3.6.1-blue.svg)](https://kafka.apache.org/)
[![Rest-Assured](https://img.shields.io/badge/Rest--Assured-5.4.0-purple.svg)](https://rest-assured.io/)
[![Allure](https://img.shields.io/badge/Allure-2.25.0-yellow.svg)](http://allure.qatools.ru/)
[![Maven](https://img.shields.io/badge/Maven-3.8+-red.svg)](https://maven.apache.org/)

![Tests](https://github.com/Sherlock0731/qa-kafka-framework/actions/workflows/test-all.yml/badge.svg)
[![Allure Report](https://img.shields.io/badge/Allure-Report-orange)](https://sherlock0731.github.io/qa-kafka-framework/)

Комплексный фреймворк автотестирования Apache Kafka на Java 17 с поддержкой SSL/TLS, параллельного выполнения и интеграции с Aiven Cloud Kafka.

---

## Возможности

- **SSL/TLS из коробки** — PKCS12 (keystore) + JKS (truststore) для Aiven и любых защищённых кластеров
- **Потокобезопасные менеджеры** — `ThreadLocal` + `WeakReference`-трекинг предотвращают утечки памяти при параллельном запуске
- **Глобальная очистка ресурсов** — `closeAll()` закрывает все producer/consumer из любого потока ForkJoinPool
- **Retry с exponential backoff** — создание топиков и polling сообщений с повторными попытками (до 5 раз)
- **Богатая иерархия исключений** — 7 типов специализированных исключений с контекстной информацией и категоризацией для Allure
- **Aiven API Controller** — управление топиками через REST API Aiven (Bearer-token авторизация)
- **TestMetricsCollector** — время отправки, polling, категории ошибок, процент успешных тестов
- **Allure listeners** — `AllureKafkaListener` и `KafkaTestExecutionListener` для обогащения отчётов
- **66 тест-кейсов** покрывают 11 функциональных областей Kafka
- **CI/CD через GitHub Actions** с публикацией Allure-отчёта на GitHub Pages
- **Docker / Docker Compose** для запуска в контейнере

---

## Архитектура

Фреймворк построен по **Hexagonal Architecture** (Ports & Adapters) с чистым разделением слоёв:

```
src/
├── main/
│   ├── java/qa/autotest/framework/
│   │   │
│   │   ├── domain/                              # Domain Layer
│   │   │   ├── exception/
│   │   │   │   └── MessageNotFoundException.java
│   │   │   ├── model/
│   │   │   │   ├── ConsumeResult.java
│   │   │   │   ├── ConsumerGroup.java
│   │   │   │   ├── KafkaErrorCategory.java      # Единая таксономия ошибок
│   │   │   │   ├── Message.java
│   │   │   │   ├── Partition.java
│   │   │   │   ├── PublishResult.java
│   │   │   │   └── Topic.java
│   │   │   └── port/
│   │   │       ├── MessageConsumer.java
│   │   │       ├── MessagePublisher.java
│   │   │       └── TopicRepository.java
│   │   │
│   │   ├── application/service/                 # Application Layer
│   │   │   ├── KafkaTestFacade.java             # Главный фасад
│   │   │   ├── MessageConsumptionService.java
│   │   │   ├── MessagePublishingService.java
│   │   │   └── TopicManagementService.java
│   │   │
│   │   ├── infrastructure/                      # Infrastructure Layer
│   │   │   ├── kafka/adapter/
│   │   │   │   ├── KafkaAdminAdapter.java       # implements TopicRepository
│   │   │   │   ├── KafkaConsumerAdapter.java    # implements MessageConsumer
│   │   │   │   └── KafkaProducerAdapter.java    # implements MessagePublisher
│   │   │   └── api/aiven/
│   │   │       ├── AivenApiController.java      # REST-клиент к Aiven API
│   │   │       └── dto/
│   │   │           ├── AivenTopicDeleteResponseDto.java
│   │   │           └── AivenTopicListResponseDto.java
│   │   │
│   │   ├── config/                              # Configuration
│   │   │   ├── ConfigFactory.java               # Fail-fast validation
│   │   │   ├── ConfigurationException.java
│   │   │   └── KafkaConfig.java                 # Owner interface (40+ properties)
│   │   │
│   │   ├── kafka/                               # Kafka Utilities
│   │   │   ├── KafkaPropertiesBuilder.java      # SSL/TLS config builder
│   │   │   └── KafkaTopicCleanupManager.java
│   │   │
│   │   ├── utils/                               # General Utilities
│   │   │   ├── KafkaAwaitHelper.java
│   │   │   └── RetryContext.java
│   │   │
│   │   ├── metrics/
│   │   │   └── TestMetricsCollector.java        # Thread-safe metrics
│   │   │
│   │   └── exceptions/                          # Legacy (to be migrated to domain/)
│   │       ├── KafkaConsumerException.java
│   │       ├── KafkaProducerException.java
│   │       ├── KafkaRebalanceException.java
│   │       ├── KafkaTestException.java          # Base exception
│   │       ├── KafkaTimeoutException.java
│   │       ├── KafkaTopicManagementException.java
│   │       └── TestDataException.java
│   │
│   └── resources/
│       ├── config/
│       │   ├── ci.properties
│       │   ├── default.properties
│       │   └── local.properties
│       └── logback.xml
│
└── test/java/tests/
    ├── BaseTest.java                            # Жизненный цикл тестов
    │
    ├── listeners/
    │   ├── AllureKafkaListener.java             # Категоризация сбоев
    │   ├── GlobalCleanupListener.java
    │   └── KafkaTestExecutionListener.java
    │
    ├── consumer/ConsumerTests.java              # 12 тест-кейсов
    ├── consumergroup/ConsumerGroupTests.java    #  1 тест-кейс
    ├── dlq/DlqTests.java                        #  3 тест-кейса
    ├── errorhandling/ErrorHandlingTests.java    #  5 тест-кейсов
    ├── idempotence/IdempotenceTests.java        #  7 тест-кейсов
    ├── offset/OffsetTests.java                  #  5 тест-кейсов
    ├── ordering/OrderingTests.java              #  3 тест-кейса
    ├── partitioning/PartitioningTests.java      #  5 тест-кейсов
    ├── performance/PerformanceTests.java        #  4 тест-кейса
    ├── producer/ProducerTests.java              # 12 тест-кейсов
    └── transactions/TransactionsTests.java      #  9 тест-кейсов
```

Подробная документация: [docs/ARCHITECTURE.md](docs/ARCHITECTURE.md)

---

## Требования

| Компонент | Версия |
|-----------|--------|
| Java | 17+ |
| Maven | 3.8+ |
| Apache Kafka | 3.x |
| Aiven Cloud Kafka | Free tier или выше |

SSL-сертификаты от брокера (truststore JKS + keystore PKCS12) обязательны.

---

## Быстрый старт

### 1. Получение SSL-сертификатов

В консоли Aiven скачайте:
- `kafka.truststore.jks` — CA-сертификат брокера
- `kafka.keystore.p12` — клиентский сертификат + ключ

Разместите файлы:
- Windows: `C:\AUTO\kafka_key\`
- Linux/macOS: `~/kafka_key/`

### 2. Настройка подключения

**Вариант А — Переменные окружения** (рекомендуется для CI/CD):

```bash
export KAFKA_BOOTSTRAP_SERVERS=kafka-xxxx.aivencloud.com:28330
export KAFKA_SSL_TRUSTSTORE_PASSWORD=ваш_пароль
export KAFKA_SSL_KEYSTORE_PASSWORD=ваш_пароль
export KAFKA_SSL_KEY_PASSWORD=ваш_пароль
export AIVEN_API_TOKEN=ваш_токен
```

**Вариант Б — Файл `src/main/resources/config/local.properties`**:

```properties
kafka.bootstrap.servers=kafka-xxxx.aivencloud.com:28330
kafka.security.protocol=SSL
kafka.ssl.truststore.location=/path/to/kafka.truststore.jks
kafka.ssl.truststore.password=ваш_пароль
kafka.ssl.keystore.location=/path/to/kafka.keystore.p12
kafka.ssl.keystore.password=ваш_пароль
kafka.ssl.key.password=ваш_пароль
aiven.api.token=ваш_токен
aiven.project.name=ваш-проект
aiven.service.name=ваш-сервис
```

### 3. Запуск тестов

```bash
# Все тесты (последовательно)
mvn test

# Smoke-тесты
mvn test -Dtest.groups=smoke

# По категории (Maven профиль)
mvn test -P producer
mvn test -P consumer
mvn test -P transactions

# Параллельный запуск с 4 потоками
mvn test -P parallel -Dthread.count=4

# Через bash-скрипт
./run-tests.sh
./run-tests.sh --groups smoke
./run-tests.sh --parallel 4
```

### 4. Allure-отчёт

```bash
mvn allure:report   # генерация отчёта
mvn allure:serve    # запуск в браузере
```

---

## Тест-сьюты

| Тег | Класс | Кейсов | Описание |
|-----|-------|--------|----------|
| `producer` | `ProducerTests` | 12 | Sync/async отправка, headers, acks=all, retry, batch, метрики |
| `consumer` | `ConsumerTests` | 12 | earliest/latest, headers, seek, pause/resume, lag |
| `transactions` | `TransactionsTests` | 9 | Commit, rollback, exactly-once, read_committed, multi-topic |
| `idempotence` | `IdempotenceTests` | 7 | Дедупликация, producer ID, exactly-once, retry без дублей |
| `offset` | `OffsetTests` | 5 | Manual commit, seek, auto-commit, offset при rebalance |
| `partitioning` | `PartitioningTests` | 5 | Round-robin, hash by key, смена числа партиций |
| `performance` | `PerformanceTests` | 4 | Throughput, E2E latency, batch size, consumer lag |
| `ordering` | `OrderingTests` | 3 | Порядок в партиции, между партициями, для keyed messages |
| `dlq` | `DlqTests` | 3 | Poison pill, DLQ routing с метаданными, retry из DLQ |
| `error-handling` | `ErrorHandlingTests` | 5 | Serialization, network, broker unavailable, topic not found |
| `consumer-group` | `ConsumerGroupTests` | 1 | Rebalance группы потребителей |
| `smoke` | (несколько) | 7 | Быстрая проверка ключевой функциональности |

**Итого: 66 тест-кейсов** (TC-001 — TC-049)

---

## Конфигурация

Приоритет загрузки (от высокого к низкому):
1. System properties (`-Dkey=value`)
2. Переменные окружения
3. `config/${env}.properties`
4. `config/default.properties`

Ключевые параметры:

| Параметр | По умолчанию | Описание |
|----------|-------------|----------|
| `kafka.security.protocol` | `SSL` | Протокол безопасности |
| `kafka.ssl.keystore.type` | `PKCS12` | Тип keystore |
| `kafka.ssl.truststore.type` | `JKS` | Тип truststore |
| `kafka.producer.acks` | `all` | Режим подтверждения |
| `kafka.producer.retries` | `3` | Количество retry |
| `kafka.producer.enable.idempotence` | `true` | Идемпотентный producer |
| `kafka.consumer.auto.offset.reset` | `earliest` | Стратегия сброса offset |
| `kafka.consumer.enable.auto.commit` | `false` | Ручной commit offset'ов |
| `kafka.consumer.session.timeout.ms` | `30000` | Таймаут сессии (мс) |
| `kafka.consumer.max.poll.records` | `500` | Макс. записей за poll |
| `kafka.test.topic.prefix` | `qa-test` | Префикс тестовых топиков |
| `kafka.test.topic.partitions` | `1` | Партиций по умолчанию |
| `test.timeout.seconds` | `30` | Таймаут async-операций |
| `thread.count` | `1` | Потоков параллельного запуска |

---

## Maven профили

```bash
# Окружение
mvn test -P local           # по умолчанию
mvn test -P ci

# Группы тестов
mvn test -P producer
mvn test -P consumer
mvn test -P idempotence
mvn test -P ordering
mvn test -P offset
mvn test -P error-handling
mvn test -P partitioning
mvn test -P consumer-group
mvn test -P dlq
mvn test -P smoke
mvn test -P critical

# Параллельное выполнение
mvn test -P parallel -Dthread.count=4

# Проверка безопасности зависимостей (OWASP NVD)
mvn verify -P security-check -DnvdApiKey=ВАШ_КЛЮЧ
```

---

## Docker

```bash
# Запуск в контейнере
cd docker
docker-compose up --build

# С переменными окружения
KAFKA_BOOTSTRAP_SERVERS=... AIVEN_API_TOKEN=... docker-compose up
```

Подробнее: [docker/README.md](docker/README.md)

---

## CI/CD

Пайплайн GitHub Actions запускается при каждом push и PR:

1. Сборка и компиляция
2. Запуск тест-сьютов
3. Генерация Allure-отчёта
4. Публикация на GitHub Pages

Необходимые GitHub Secrets: `KAFKA_BOOTSTRAP_SERVERS`, `KAFKA_SSL_TRUSTSTORE_PASSWORD`, `KAFKA_SSL_KEYSTORE_PASSWORD`, `KAFKA_SSL_KEY_PASSWORD`, `AIVEN_API_TOKEN`.

Подробнее: [docs/GITHUB_SECRETS_SETUP.md](docs/GITHUB_SECRETS_SETUP.md)

---

## Документация

| Документ | Описание |
|----------|----------|
| [docs/ARCHITECTURE.md](docs/ARCHITECTURE.md) | Детальная архитектура фреймворка |
| [docs/RUN_INSTRUCTIONS.md](docs/RUN_INSTRUCTIONS.md) | Инструкция по запуску всех вариантов |
| [docs/DOCKER.md](docs/DOCKER.md) | Docker и Docker Compose |
| [docs/SECURITY_GUIDE.md](docs/SECURITY_GUIDE.md) | Настройка SSL/TLS |
| [docs/GITHUB_SECRETS_SETUP.md](docs/GITHUB_SECRETS_SETUP.md) | Настройка CI/CD секретов |
| [docs/TEST_CASES_MATRIX.md](docs/TEST_CASES_MATRIX.md) | Матрица всех тест-кейсов |
| [SUMMARY.md](SUMMARY.md) | Резюме проекта |

---

## Стек технологий

| Библиотека | Версия | Назначение |
|-----------|--------|-----------|
| `kafka-clients` | 3.6.1 | Producer, Consumer, AdminClient |
| `junit-jupiter` | 5.10.1 | Тестовый фреймворк |
| `assertj-core` | 3.24.2 | Fluent assertions |
| `allure-junit5` | 2.25.0 | Allure-отчёты |
| `allure-rest-assured` | 2.25.0 | Allure-фильтр для REST Assured |
| `rest-assured` | 5.4.0 | HTTP-тестирование (Aiven API) |
| `awaitility` | 4.2.0 | Async assertions |
| `owner` | 1.0.12 | Конфигурация через properties + env |
| `lombok` | 1.18.30 | Builder, @Slf4j, @Data |
| `jackson-databind` | 2.16.1 | JSON-сериализация |
| `logback-classic` | 1.4.14 | Логирование |
| `gson` | 2.10.1 | JSON в metrics/attachments |

## License

[![License: MIT](https://img.shields.io/badge/License-MIT-green.svg)](https://opensource.org/licenses/MIT)

## Authors

- **Vitaliy Popravka** - QA Automation Engineer

## Контакты

Для вопросов и предложений создавайте Issue в репозитории.
