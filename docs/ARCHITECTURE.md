# Обзор архитектуры

## Принципы проектирования фреймворка

### 1. Потокобезопасность
Все Kafka менеджеры используют `ThreadLocal` для обеспечения потокобезопасной работы при параллельном выполнении:
- **KafkaProducerManager**: Каждый поток имеет свой экземпляр producer
- **KafkaConsumerManager**: Каждый поток имеет свой экземпляр consumer
- **KafkaTopicManager**: Admin клиент потокобезопасен

### 2. Независимость тестов
Каждый тест:
- Создает уникальные топики с UUID суффиксами
- Использует уникальные consumer group ID
- Очищает ресурсы в `@AfterEach`
- Никогда не разделяет состояние с другими тестами

### 3. Асинхронное тестирование без Thread.sleep()
Использование библиотеки Awaitility:
```java
await().atMost(5, SECONDS).untilAsserted(() -> {
    List<ConsumerRecordDto> records = consumerManager.poll(2);
    assertThat(records).isNotEmpty();
});
```

### 4. DTO паттерн
Чистое разделение между внутренними классами Kafka и тестовыми данными:
- **KafkaMessageDto**: Сообщение для отправки
- **ConsumerRecordDto**: Полученное сообщение
- **TopicPartitionDto**: Информация о партиции топика

## Архитектура компонентов

```
┌─────────────────────────────────────────────────────────────┐
│                    Слой тестов                               │
│  (Producer, Consumer, Idempotence, Ordering, Offset и т.д.)  │
└────────────────────┬────────────────────────────────────────┘
                     │
                     ▼
┌─────────────────────────────────────────────────────────────┐
│                    BaseTest                                  │
│  - Setup/Teardown                                            │
│  - Инициализация менеджеров                                  │
│  - Отслеживание и очистка топиков                            │
└────────────────────┬────────────────────────────────────────┘
                     │
          ┌──────────┴──────────┐
          ▼                     ▼
┌──────────────────┐  ┌──────────────────┐
│ Kafka Менеджеры  │  │  Утилиты         │
│ - Producer Mgr   │  │ - TestDataGen    │
│ - Consumer Mgr   │  │ - AsyncHelper    │
│ - Topic Mgr      │  │ - EventHelper    │
└──────────────────┘  └──────────────────┘
          │
          ▼
┌──────────────────────────────────────────────────────────────┐
│               Слой конфигурации                               │
│  - ConfigFactory (Singleton)                                  │
│  - KafkaConfig (Owner library)                                │
│  - Properties файлы (default, local, ci)                      │
└──────────────────────────────────────────────────────────────┘
          │
          ▼
┌──────────────────────────────────────────────────────────────┐
│              Apache Kafka (Aiven Cloud)                       │
│  - Bootstrap Servers                                          │
│  - SSL/TLS подключение                                        │
│  - Топики, Партиции, Consumer Groups                          │
└──────────────────────────────────────────────────────────────┘
```

## Стратегии параллельного выполнения

### Стратегия 1: JUnit 5 параллельное выполнение
- Использует `@Execution(ExecutionMode.CONCURRENT)`
- Контролируется параметром `thread.count`
- Лучше для: Независимых тестовых методов

```bash
mvn test -Pparallel -Dthread.count=4
```

### Стратегия 2: Maven Surefire Fork-Based
- Использует параметр `forkCount`
- Каждый fork - отдельная JVM
- Лучше для: Строгой изоляции ресурсов

```bash
mvn test -Pparallel-strict -Dthread.count=4
```

## Архитектура логирования

### Многопоточное логирование с Logback

**Три аппендера:**
1. **CONSOLE**: Консольный вывод с информацией о потоке
2. **FILE**: Единый консолидированный лог файл
3. **SIFT**: Отдельный лог файл для каждого потока

**Пример вывода лога:**
```
10:23:45.123 [ForkJoinPool-1-worker-1] INFO  qa.autotest.framework.kafka.KafkaProducerManager - Отправка сообщения в топик 'qa-test-abc123'
10:23:45.456 [ForkJoinPool-1-worker-2] INFO  qa.autotest.framework.kafka.KafkaProducerManager - Отправка сообщения в топик 'qa-test-def456'
```

## Управление конфигурацией

### Приоритет (от высшего к низшему):
1. **System Properties**: `-Dkafka.ssl.truststore.password=xxx`
2. **Environment Variables**: `KAFKA_SSL_TRUSTSTORE_PASSWORD=xxx`
3. **Properties окружения**: `ci.properties`, `local.properties`
4. **Дефолтные Properties**: `default.properties`

### Пример загрузки конфигурации:
```java
// В BaseTest
protected static final KafkaConfig CONFIG = ConfigFactory.getConfig();

// ConfigFactory использует Owner библиотеку
config = org.aeonbits.owner.ConfigFactory.create(KafkaConfig.class);
```

## Архитектура безопасности

### SSL/TLS подключение
- **Truststore**: Проверяет сертификат брокера Kafka
- **Keystore**: Клиентский сертификат для mutual TLS
- **Пароли**: Хранятся в переменных окружения или secrets

### Лучшие практики:
1. Никогда не коммитьте сертификаты в репозиторий
2. Используйте переменные окружения для паролей
3. Храните сертификаты вне директории проекта
4. Используйте GitHub Secrets для CI/CD

## Паттерны тестирования

### 1. Event-Driven тестирование
```java
KafkaMessageDto event = EventDrivenHelper.createEvent(
    topic, "ORDER_CREATED", orderJson
);
producerManager.sendSync(event);
```

### 2. Saga Pattern тестирование
```java
// Отправка компенсирующего события
KafkaMessageDto compensate = EventDrivenHelper.createCompensatingEvent(
    sagaTopic, originalEventId
);
```

### 3. Dead Letter Queue (DLQ)
```java
String dlqTopic = topicManager.createDlqTopic(originalTopic);
// Обработка неудачных сообщений в DLQ
```

## Соображения производительности

### Оптимизация Producer
- **Идемпотентность**: Включена по умолчанию (без дубликатов)
- **Батчинг**: `batch.size=16384`, `linger.ms=10`
- **Сжатие**: Может быть включено по необходимости

### Оптимизация Consumer
- **Fetch настройки**: `fetch.min.bytes=1`, `fetch.max.wait.ms=500`
- **Poll Records**: `max.poll.records=500`
- **Ручной Commit**: Отключен auto-commit для точного контроля

## Обработка ошибок

### Ошибки Producer
- **Retry**: Настроен с `retries=3`
- **Timeout**: `request.timeout.ms=30000`
- **Callbacks**: Асинхронная обработка ошибок

### Ошибки Consumer
- **Десериализация**: Try-catch с DLQ паттерном
- **Ошибки обработки**: Retry с backoff
- **Rebalance**: Graceful обработка

## Точки расширения

### Добавление новых категорий тестов
1. Создайте пакет в `tests/`
2. Расширьте `BaseTest`
3. Добавьте `@Tag` для фильтрации
4. Используйте существующие менеджеры

### Добавление новых паттернов
1. Создайте helper в `framework/patterns/`
2. Задокументируйте использование паттерна
3. Добавьте примеры тестов

### Добавление новых утилит
1. Создайте класс в `framework/utils/`
2. Сделайте потокобезопасным при необходимости
3. Добавьте unit тесты

## Docker интеграция

### Многоступенчатая сборка
- **Этап Builder**: Компиляция и сборка
- **Этап Runtime**: Минимальный образ для запуска

### Преимущества Docker
- Изоляция окружения
- Воспроизводимые сборки
- Кроссплатформенность
- Легкое масштабирование

---

**Версия документации:** 1.1.0  
**Последнее обновление:** 2026-01-18
