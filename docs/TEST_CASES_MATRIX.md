# Матрица Тест-Кейсов Kafka

## Обзор

Всего тест-кейсов: **66**
- Producer (Продюсер): 12 тестов
- Consumer (Консьюмер): 12 тестов
- Idempotence (Идемпотентность): 6 тестов
- Ordering (Порядок): 3 теста
- Offset Management (Управление офсетами): 5 тестов
- Error Handling (Обработка ошибок): 5 тестов
- Partitioning (Партиционирование): 4 теста
- Consumer Groups (Группы консьюмеров): 1 тест
- Dead Letter Queue (Очередь отказов): 3 теста
- Transactions (Транзакции): 9 тестов
- Performance (Производительность): 4 теста
- Compression (Сжатие): 2 теста

## Тест-кейсы по приоритету

### Критичные (BLOCKER/CRITICAL): 35 тестов
### Нормальные (NORMAL): 26 тестов
### Минорные (MINOR): 5 тестов

---

## 1. Producer Tests (Тесты продюсера) - 12 тестов

| ID | Название | Тип | Приоритет | Теги |
|----|----------|-----|-----------|------|
| TC-001 | Отправка одиночного сообщения в топик | Positive | CRITICAL | `producer`, `smoke`, `critical` |
| TC-002 | Массовая отправка сообщений (batch 100) | Positive | CRITICAL | `producer`, `critical` |
| TC-003 | Отправка сообщения с custom headers | Positive | NORMAL | `producer` |
| TC-004 | Отправка с ключом для партиционирования | Positive | CRITICAL | `producer`, `smoke`, `critical` |
| TC-006 | Отправка сообщения больше max.message.bytes | Negative | NORMAL | `producer` |
| TC-006 | Сжатие сообщений (gzip/lz4/snappy) | Positive | NORMAL | `producer`, `compression` |
| TC-006A | Таймаут запроса продюсера | Negative | NORMAL | `producer`, `error-handling` |
| TC-006B | Переполнение буфера продюсера | Negative | NORMAL | `producer`, `error-handling` |
| TC-006C | Транзакционная отправка сообщений | Positive | CRITICAL | `producer`, `transactions` |
| TC-006D | Сбор метрик продюсера | Positive | NORMAL | `producer`, `performance` |
| TC-007 | Гарантия acks=all | Positive | CRITICAL | `producer`, `critical` |
| TC-008 | Автоматический retry при временных ошибках | Positive | CRITICAL | `producer`, `error-handling`, `critical` |

**Описание категории:**
Тесты продюсера проверяют базовую функциональность отправки сообщений, обработку ошибок, производительность и транзакционные возможности.

---

## 2. Consumer Tests (Тесты консьюмера) - 12 тестов

| ID | Название | Тип | Приоритет | Теги |
|----|----------|-----|-----------|------|
| TC-009 | Чтение сообщений с начала топика (earliest) | Positive | CRITICAL | `consumer`, `smoke`, `critical` |
| TC-010 | Чтение только новых сообщений (latest) | Positive | CRITICAL | `consumer`, `critical` |
| TC-011 | Чтение сообщений с headers | Positive | NORMAL | `consumer` |
| TC-011A | Таймаут сессии и ребалансировка | Negative | NORMAL | `consumer`, `error-handling` |
| TC-011B | Механизм heartbeat | Positive | NORMAL | `consumer` |
| TC-011C | Ручное назначение партиций | Positive | NORMAL | `consumer` |
| TC-011D | Seek to beginning и end | Positive | NORMAL | `consumer`, `offset` |
| TC-011E | Метрики lag консьюмера | Positive | NORMAL | `consumer`, `performance` |
| TC-012 | Несколько консьюмеров в одной группе | Positive | CRITICAL | `consumer`, `consumer-group`, `critical` |
| TC-013 | Pause и resume консьюмера | Positive | NORMAL | `consumer` |
| TC-014 | Лимит max.poll.records | Positive | NORMAL | `consumer` |
| TC-015 | Таймаут poll | Positive | NORMAL | `consumer` |

**Описание категории:**
Тесты консьюмера покрывают различные стратегии чтения, управление партициями, механизмы координации и обработку ошибок.

---

## 3. Idempotence Tests (Тесты идемпотентности) - 6 тестов

| ID | Название | Тип | Приоритет | Теги |
|----|----------|-----|-----------|------|
| TC-016 | Включение идемпотентного producer | Positive | BLOCKER | `idempotence`, `smoke`, `critical` |
| TC-017 | Обработка дубликатов через message ID | Positive | CRITICAL | `idempotence`, `critical` |
| TC-023 | Проверка идемпотентности producer | Positive | CRITICAL | `idempotence`, `critical` |
| TC-024 | Дубликаты при retry | Negative | NORMAL | `idempotence` |
| TC-025 | Exactly-once доставка сообщений | Positive | BLOCKER | `idempotence`, `smoke`, `critical` |
| TC-025A | Producer ID и sequence number | Positive | NORMAL | `idempotence` |

**Описание категории:**
Тесты идемпотентности проверяют механизмы предотвращения дубликатов и гарантии exactly-once семантики.

---

## 4. Ordering Tests (Тесты порядка) - 3 теста

| ID | Название | Тип | Приоритет | Теги |
|----|----------|-----|-----------|------|
| TC-021 | Гарантия порядка в одной партиции | Positive | BLOCKER | `ordering`, `smoke`, `critical` |
| TC-027 | Порядок сообщений между партициями | Negative | NORMAL | `ordering` |
| TC-028 | Гарантия порядка для сообщений с ключом | Positive | CRITICAL | `ordering`, `critical` |

**Описание категории:**
Тесты порядка проверяют гарантии упорядоченности сообщений внутри партиции и между партициями.

---

## 5. Offset Management Tests (Управление офсетами) - 5 тестов

| ID | Название | Тип | Приоритет | Теги |
|----|----------|-----|-----------|------|
| TC-018 | Автоматический commit офсетов | Positive | NORMAL | `offset` |
| TC-019 | Стратегия reset офсетов | Positive | NORMAL | `offset` |
| TC-019A | Commit офсетов во время ребалансировки | Positive | NORMAL | `offset`, `consumer-group` |
| TC-027 | Ручной sync commit после обработки | Positive | CRITICAL | `offset`, `smoke`, `critical` |
| TC-029 | Commit определенного offset | Positive | NORMAL | `offset` |

**Описание категории:**
Тесты управления офсетами проверяют различные стратегии commit и обработку сценариев с потерей данных.

---

## 6. Error Handling Tests (Обработка ошибок) - 5 тестов

| ID | Название | Тип | Приоритет | Теги |
|----|----------|-----|-----------|------|
| TC-029 | Обработка ошибок сериализации | Negative | CRITICAL | `error-handling`, `critical` |
| TC-029A | Обработка ошибок десериализации | Negative | CRITICAL | `error-handling`, `critical` |
| TC-029B | Восстановление после сетевых ошибок | Negative | NORMAL | `error-handling` |
| TC-029C | Обработка недоступности брокера | Negative | CRITICAL | `error-handling`, `critical` |
| TC-029D | Обработка отсутствующего топика | Negative | NORMAL | `error-handling` |

**Описание категории:**
Тесты обработки ошибок проверяют устойчивость системы к различным типам сбоев и корректность recovery механизмов.

---

## 7. Partitioning Tests (Партиционирование) - 4 теста

| ID | Название | Тип | Приоритет | Теги |
|----|----------|-----|-----------|------|
| TC-020 | Распределение сообщений по партициям | Positive | CRITICAL | `partitioning`, `critical` |
| TC-022 | Один ключ → одна партиция | Positive | CRITICAL | `partitioning`, `smoke`, `critical` |
| TC-022A | Изменение количества партиций | Negative | NORMAL | `partitioning` |
| TC-038 | Round-robin без ключа | Positive | NORMAL | `partitioning` |
| TC-039 | Hash партиционирование по ключу | Positive | CRITICAL | `partitioning`, `smoke`, `critical` |

**Описание категории:**
Тесты партиционирования проверяют механизмы распределения сообщений по партициям и консистентность маршрутизации.

---

## 8. Consumer Group Tests (Группы консьюмеров) - 1 тест

| ID | Название | Тип | Приоритет | Теги |
|----|----------|-----|-----------|------|
| TC-037 | Ребалансировка группы консьюмеров | Positive | CRITICAL | `consumer-group`, `smoke`, `critical` |

**Описание категории:**
Тесты групп консьюмеров проверяют координацию между несколькими консьюмерами, механизмы ребалансировки и распределение партиций.

---

## 9. Dead Letter Queue Tests (Очередь отказов) - 3 теста

| ID | Название | Тип | Приоритет | Теги |
|----|----------|-----|-----------|------|
| TC-031 | Маршрутизация в DLQ с метаданными | Positive | CRITICAL | `dlq`, `critical` |
| TC-032 | Retry обработки из DLQ | Positive | NORMAL | `dlq` |
| TC-049 | Отправка poison pill в DLQ после N попыток | Positive | CRITICAL | `dlq`, `smoke`, `critical` |

**Описание категории:**
Тесты DLQ проверяют механизмы обработки проблемных сообщений, сохранение метаданных об ошибках и возможность повторной обработки.

---

## 10. Transaction Tests (Транзакции) - 9 тестов

| ID | Название | Тип | Приоритет | Теги |
|----|----------|-----|-----------|------|
| TC-040 | Транзакционная отправка сообщений | Positive | CRITICAL | `transactions`, `smoke`, `critical` |
| TC-041 | Commit транзакции | Positive | CRITICAL | `transactions`, `critical` |
| TC-042 | Rollback транзакции | Positive | CRITICAL | `transactions`, `critical` |
| TC-043 | Read committed isolation level | Positive | CRITICAL | `transactions`, `critical` |
| TC-044 | Exactly-once семантика | Positive | BLOCKER | `transactions`, `smoke`, `critical` |
| TC-045 | Транзакции между несколькими топиками | Positive | CRITICAL | `transactions`, `critical` |
| TC-046 | Timeout транзакции | Negative | NORMAL | `transactions`, `error-handling` |
| TC-047 | Координация producer и consumer транзакций | Positive | CRITICAL | `transactions`, `critical` |
| TC-048 | Восстановление после сбоя транзакции | Negative | NORMAL | `transactions`, `error-handling` |

**Описание категории:**
Тесты транзакций проверяют ACID гарантии, координацию между продюсером и консьюмером, и обработку сбоев в транзакциях.

---

## 11. Performance Tests (Производительность) - 4 теста

| ID | Название | Тип | Приоритет | Теги |
|----|----------|-----|-----------|------|
| TC-033 | Бенчмарк throughput продюсера | Performance | NORMAL | `performance` |
| TC-034 | Измерение end-to-end латентности | Performance | NORMAL | `performance` |
| TC-035 | Оптимизация размера batch | Performance | NORMAL | `performance` |
| TC-036 | Consumer lag под нагрузкой | Performance | NORMAL | `performance` |

**Описание категории:**
Тесты производительности измеряют throughput, latency, оптимальные параметры батчинга и поведение под высокой нагрузкой.

---

## Стратегия выполнения тестов

### Фаза 1: Smoke Tests (~20 тестов, ~7 минут)
Проверка критичной функциональности:
```bash
mvn test -Dgroups=smoke
```

**Включает:**
- TC-001: Отправка одиночного сообщения
- TC-004: Отправка с ключом
- TC-009: Чтение с начала
- TC-016: Идемпотентный producer
- TC-021: Порядок в партиции
- TC-022: Ключ → партиция
- TC-025: Exactly-once
- TC-027: Ручной commit
- TC-037: Ребалансировка группы
- TC-039: Hash партиционирование
- TC-040: Транзакционная отправка
- TC-044: Exactly-once семантика
- TC-049: DLQ базовая функциональность

### Фаза 2: Critical Tests (~35 тестов, ~15 минут)
Все высокоприоритетные тесты:
```bash
mvn test -Dgroups=critical
```

### Фаза 3: Full Suite (~66 тестов, ~30 минут)
Все тесты включая граничные случаи:
```bash
mvn clean test
```

### Фаза 4: Parallel Full Suite (~66 тестов, ~12 минут)
Самое быстрое выполнение:
```bash
mvn clean test -Pparallel -Dthread.count=2
```
*Примечание: используем 2 потока из-за ограничений Aiven на количество партиций*

---

## Карта покрытия тестами

```
Producer (12)
├── Базовые операции (5) ✓
├── Обработка ошибок (4) ✓
├── Производительность (2) ✓
└── Транзакции (1) ✓

Consumer (12)
├── Стратегии чтения (3) ✓
├── Управление партициями (3) ✓
├── Механизмы координации (3) ✓
├── Обработка ошибок (2) ✓
└── Производительность (1) ✓

Idempotence (6)
├── Идемпотентность продюсера (3) ✓
├── Обработка дубликатов (2) ✓
└── Exactly-once (1) ✓

Ordering (3)
├── Гарантированный порядок (2) ✓
└── Нарушения порядка (1) ✓

Offset Management (5)
├── Стратегии commit (4) ✓
└── Сценарии отказов (1) ✓

Error Handling (5)
├── Ошибки сериализации (2) ✓
├── Сетевые ошибки (2) ✓
└── Ошибки доступности (1) ✓

Partitioning (4)
├── Стратегии распределения (3) ✓
└── Граничные случаи (1) ✓

Consumer Groups (1)
└── Ребалансировка (1) ✓

DLQ (3)
├── Операции DLQ (2) ✓
└── Poison pill (1) ✓

Transactions (9)
├── Базовые транзакции (3) ✓
├── Exactly-once (2) ✓
├── Координация (2) ✓
└── Обработка сбоев (2) ✓

Performance (4)
├── Throughput (1) ✓
├── Latency (1) ✓
├── Оптимизация (1) ✓
└── Нагрузка (1) ✓
```

---

## Рекомендуемые последовательности запуска

### Для локальной разработки
1. Запустить измененную категорию тестов
2. Запустить smoke тесты
3. Запустить полный набор при значительных изменениях

**Пример:**
```bash
# Изменили producer → запустить producer тесты
mvn test -Dtest=ProducerTests

# Smoke тесты
mvn test -Dgroups=smoke

# Полный набор
mvn clean test
```

### Для CI/CD
1. Smoke тесты (при каждом коммите) - ~7 минут
2. Полный набор (при Pull Request) - ~30 минут
3. Параллельный полный набор (ночные запуски) - ~12 минут

**Пример конфигурации:**
```yaml
# .github/workflows/tests.yml
on_push:
  - mvn test -Dgroups=smoke
  
on_pull_request:
  - mvn clean test
  
on_schedule:
  - mvn clean test -Pparallel -Dthread.count=2
```

### Для релиза
1. Последовательный полный набор (проверка стабильности)
2. Параллельный полный набор (проверка thread-safety)
3. Повтор на всех платформах (Windows, Linux, macOS)

**Пример:**
```bash
# Шаг 1: Sequential
mvn clean test

# Шаг 2: Parallel
mvn clean test -Pparallel -Dthread.count=2

# Шаг 3: Repeat 3 times
for i in {1..3}; do
  mvn clean test -Pparallel -Dthread.count=2
done
```

---

## Запуск по категориям

### Producer Tests
```bash
mvn test -Dtest=ProducerTests
```

### Consumer Tests
```bash
mvn test -Dtest=ConsumerTests
```

### Idempotence Tests
```bash
mvn test -Dtest=IdempotenceTests
```

### Ordering Tests
```bash
mvn test -Dtest=OrderingTests
```

### Offset Management Tests
```bash
mvn test -Dtest=OffsetTests
```

### Error Handling Tests
```bash
mvn test -Dtest=ErrorHandlingTests
```

### Partitioning Tests
```bash
mvn test -Dtest=PartitioningTests
```

### Consumer Group Tests
```bash
mvn test -Dtest=ConsumerGroupTests
```

### DLQ Tests
```bash
mvn test -Dtest=DlqTests
```

### Transaction Tests
```bash
mvn test -Dtest=TransactionsTests
```

### Performance Tests
```bash
mvn test -Dtest=PerformanceTests
```

---

## Запуск конкретных тестов

### По ID тест-кейса
```bash
# TC-001: Отправка одиночного сообщения
mvn test -Dtest=ProducerTests#testSendSingleMessage

# TC-009: Чтение с начала
mvn test -Dtest=ConsumerTests#testReadFromBeginning

# TC-016: Идемпотентный producer
mvn test -Dtest=IdempotenceTests#testEnableIdempotence

# TC-040: Транзакционная отправка
mvn test -Dtest=TransactionsTests#testTransactionalSend

# TC-049: DLQ после N попыток
mvn test -Dtest=DlqTests#testSendToDlqAfterRetries
```

---

## Метрики покрытия

### По типу тестов
- **Positive tests:** 52 (79%)
- **Negative tests:** 14 (21%)
- **Performance tests:** 4 (6%)

### По приоритету
- **BLOCKER:** 3 тестов (5%)
- **CRITICAL:** 32 теста (48%)
- **NORMAL:** 31 тест (47%)

### По тегам
- `smoke`: 13 тестов
- `critical`: 35 тестов
- `producer`: 12 тестов
- `consumer`: 12 тестов
- `transactions`: 9 тестов
- `idempotence`: 6 тестов
- `error-handling`: 10 тестов
- `offset`: 5 тестов
- `partitioning`: 4 тестов
- `dlq`: 3 тестов
- `ordering`: 3 тестов
- `consumer-group`: 2 теста
- `performance`: 4 теста

---

## Особенности Aiven Cloud

### Ограничения
- **Максимум партиций:** 2 (все тесты адаптированы)
- **SSL:** обязателен (настроен в конфигурации)
- **Топики:** автоматическое создание с 2 партициями

### Рекомендации
1. Используйте параллельное выполнение с `thread.count=2`
2. При создании топиков используйте `createTestTopic(2)`
3. SSL сертификаты должны быть в `src/test/resources/certs/`

---

## Генерация отчетов

### Allure Report
```bash
# Запуск тестов с генерацией Allure данных
mvn clean test

# Генерация и открытие отчета
mvn allure:serve
```

### Surefire Report
```bash
mvn clean test
mvn surefire-report:report
```

---

## Матрица совместимости

| Компонент | Версия | Статус |
|-----------|--------|--------|
| Java | 17+ | ✅ |
| Maven | 3.6+ | ✅ |
| Kafka Clients | 3.6.1 | ✅ |
| JUnit | 5.10.1 | ✅ |
| Allure | 2.25.0 | ✅ |
| AssertJ | 3.24.2 | ✅ |
| Aiven Cloud | Latest | ✅ |

---

## Итоговая статистика

**Общее количество тестов:** 66  
**Время выполнения:**
- Smoke: ~7 минут
- Critical: ~15 минут
- Full Sequential: ~30 минут
- Full Parallel (2 threads): ~12 минут

---

## Версия матрицы

**Версия:** v1  
**Дата:** 2026-01-14  
**Автор:** **Vitaliy Popravka** - QA Automation Engineer  
**Статус:** COMPLETE ✅
