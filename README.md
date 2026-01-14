# Фреймворк автотестирования Kafka

[![Java](https://img.shields.io/badge/Java-17-orange.svg)](https://www.oracle.com/java/)
[![JUnit](https://img.shields.io/badge/JUnit-5.10.1-red.svg)](https://junit.org/junit5/)
[![Rest-Assured](https://img.shields.io/badge/Rest--Assured-5.4.0-purple.svg)](https://rest-assured.io/)
[![Allure](https://img.shields.io/badge/Allure-2.25.0-yellow.svg)](http://allure.qatools.ru/)

![Tests](https://github.com/Sherlock0731/qa-kafka-framework/actions/workflows/test-all.yml/badge.svg)    
[![Allure Report](https://img.shields.io/badge/Allure-Report-orange)](https://sherlock0731.github.io/qa-kafka-framework/)

Комплексный многопоточный фреймворк для тестирования Apache Kafka на Java 17, JUnit 5, AssertJ и Allure Reports.

## Возможности

- **Многопоточное выполнение** с потокобезопасными менеджерами
- **Event-Driven паттерны тестирования** (Event-Driven, Saga, Outbox)
- **Кроссплатформенность** (Windows, Linux, macOS)
- **Асинхронное тестирование** с Awaitility (без Thread.sleep)
- **Поддержка SSL/TLS** для безопасного подключения к Kafka
- **Отчеты Allure** для визуализации результатов
- **Запуск по тегам** (запуск тестов по категориям)
- **Интеграция с GitHub Actions** для CI/CD
- **Docker и Docker Compose** для контейнеризации

## Предварительные требования

- Java 17+
- Maven 3.8+
- Apache Kafka (например, Aiven Cloud)
- SSL сертификаты (truststore и keystore)
- Docker (опционально)

## Быстрый старт

### Вариант 1: Локальный запуск

#### 1. Настройка SSL сертификатов

Поместите ваши Kafka SSL сертификаты:
- `C:\\kafka_key\\` (Windows)
- `~/kafka_key/` (Linux/Mac)

#### 2. Установка паролей

**Вариант А: Переменные окружения** (Рекомендуется для CI/CD)
```bash
export KAFKA_SSL_TRUSTSTORE_PASSWORD=ваш_пароль
export KAFKA_SSL_KEYSTORE_PASSWORD=ваш_пароль
export KAFKA_REST_API_PASSWORD=ваш_пароль
```

**Вариант Б: System Properties** (Для локальной разработки)
```bash
mvn test -Dkafka.ssl.truststore.password=ваш_пароль \
         -Dkafka.ssl.keystore.password=ваш_пароль
```

#### 3. Запуск тестов

**Запустить все тесты (последовательно):**
```bash
mvn clean test
```

**Запустить конкретную группу тестов:**
```bash
mvn clean test -Dgroups=producer
mvn clean test -Dgroups=consumer
mvn clean test -Dgroups=smoke
```

**Запустить параллельно (4 потока):**
```bash
mvn clean test -Pparallel -Dthread.count=4
```

### Вариант 2: Запуск в Docker

#### 1. Подготовка

```bash
# Создайте директорию для сертификатов
mkdir kafka_key

# Скопируйте ваши сертификаты
cp /путь/к/kafka.truststore.jks kafka_key/
cp /путь/к/kafka.keystore.p12 kafka_key/

# Создайте .env файл
cp .env.example .env
# Отредактируйте .env и укажите пароли
```

#### 2. Запуск тестов в Docker

```bash
# Все тесты
./docker/docker-run.sh

# Smoke тесты
./docker/docker-run.sh --smoke

# Параллельное выполнение
./docker/docker-run.sh --parallel

# С пересборкой образа
./docker/docker-run.sh --build
```

#### 3. Просмотр отчетов

```bash
./docker/docker-run.sh --report
# Откройте http://localhost:5050
# Логин: admin, Пароль: admin
```

## Категории тестов и теги

| Тег | Описание | Кол-во тестов |
|-----|----------|---------------|
| `producer` | Тесты отправки сообщений | 8 |
| `consumer` | Тесты получения сообщений | 7 |
| `idempotence` | Идемпотентность и дубликаты | 5 |
| `ordering` | Упорядоченность сообщений | 5 |
| `offset` | Управление offset'ами | 6 |
| `error-handling` | Обработка ошибок и retry | 6 |
| `partitioning` | Партиционирование | 5 |
| `consumer-group` | Consumer groups | 6 |
| `dlq` | Dead Letter Queue | 5 |
| `smoke` | Критические smoke тесты | ~15 |
| `critical` | Высокоприоритетные тесты | ~30 |

## Запуск тестов по тегам

**Один тег:**
```bash
mvn test -Dgroups=smoke
```

**Несколько тегов (логика ИЛИ):**
```bash
mvn test -Dgroups="producer | consumer"
```

**Несколько тегов (логика И):**
```bash
mvn test -Dgroups="producer & critical"
```

**Исключение тегов:**
```bash
mvn test -Dgroups="!slow"
```

## ⚡ Параллельное выполнение

Фреймворк поддерживает три режима параллельного выполнения:

### 1. Последовательное (По умолчанию)
```bash
mvn test -Psequential
```

### 2. Параллельное (JUnit 5 параллельное выполнение)
```bash
mvn test -Pparallel -Dthread.count=4
```

### 3. Параллельное строгое (Fork-based выполнение)
```bash
mvn test -Pparallel-strict -Dthread.count=4
```

## Безопасность

### ВАЖНО: Защита сертификатов Kafka

**Файлы `*.jks` и `*.p12` НИКОГДА не должны попадать в Git!**

#### Быстрая настройка:

1. **Локально:** Храните сертификаты вне репозитория
   ```bash
   # Создайте безопасную директорию
   mkdir -p ~/secure/kafka-certs
   cp kafka.keystore.p12 ~/secure/kafka-certs/
   cp kafka.truststore.jks ~/secure/kafka-certs/
   
   # Установите переменные окружения
   export KAFKA_SSL_TRUSTSTORE_PASSWORD="ваш_пароль"
   export KAFKA_SSL_KEYSTORE_PASSWORD="ваш_пароль"
   ```

2. **GitHub Actions:** Используйте GitHub Secrets
   - См. подробную инструкцию: [docs/GITHUB_SECRETS_SETUP.md](docs/GITHUB_SECRETS_SETUP.md)
   - Нужно добавить 6 секретов (сертификаты в Base64 + пароли)

3. **Git Hook:** Установите защиту от случайных коммитов
   ```bash
   ./setup-git-hooks.sh
   ```

**Полное руководство:** [docs/SECURITY_GUIDE.md](docs/SECURITY_GUIDE.md)

## Конфигурация

### Локальная разработка
Отредактируйте `src/main/resources/config/local.properties`:
```properties
kafka.ssl.truststore.location=C:\\kafka_key\\kafka.truststore.jks
kafka.ssl.keystore.location=C:\\kafka_key\\kafka.keystore.p12
```

### CI/CD окружение
Отредактируйте `src/main/resources/config/ci.properties` или используйте переменные окружения в GitHub Actions.

## Отчеты Allure

**Сгенерировать и открыть отчет:**
```bash
mvn clean test
mvn allure:report
mvn allure:serve
```

**В Docker:**
```bash
./docker/docker-run.sh --report
```

## Структура проекта

```
kafka-test-framework/
├── docker/                      # Docker файлы
│   ├── Dockerfile              # Образ для тестов
│   ├── docker-compose.yml      # Композиция сервисов
│   ├── docker-run.sh          # Скрипт запуска
│   └── README.md              # Docker документация
├── docs/                       # Документация
│   ├── ARCHITECTURE.md        # Архитектура
│   ├── RUN_INSTRUCTIONS.md    # Инструкции по запуску
│   └── TEST_CASES_MATRIX.md   # Матрица тест-кейсов
├── src/
│   ├── main/
│   │   ├── java/qa/autotest/
│   │   │   ├── app/dto/              # Data Transfer Objects
│   │   │   └── framework/
│   │   │       ├── config/           # Управление конфигурацией
│   │   │       ├── kafka/            # Kafka менеджеры
│   │   │       ├── patterns/         # Паттерны тестирования
│   │   │       └── utils/            # Утилиты
│   │   └── resources/
│   │       ├── config/               # Properties файлы
│   │       └── logback.xml           # Конфигурация логирования
│   └── test/
│       └── java/tests/
│           ├── BaseTest.java         # Базовый тестовый класс
│           ├── producer/             # Producer тесты
│           ├── consumer/             # Consumer тесты
│           ├── idempotence/          # Идемпотентность
│           ├── ordering/             # Упорядоченность
│           ├── offset/               # Offset тесты
│           ├── partitioning/         # Партиционирование
│           └── dlq/                  # DLQ тесты
├── .github/workflows/          # GitHub Actions
├── pom.xml                     # Maven конфигурация
├── README.md                   # Эта документация
├── SUMMARY.md                  # Сводка проекта
└── run-tests.sh               # Скрипт запуска тестов
```

## Реализованные лучшие практики

1. **Без Thread.sleep()** - используется Awaitility для асинхронных ожиданий
2. **Потокобезопасный дизайн** - ThreadLocal для Kafka клиентов
3. **Независимые тесты** - каждый тест создает уникальные топики
4. **Правильная очистка** - @AfterEach всегда закрывает ресурсы
5. **DTO паттерн** - чистое разделение моделей данных
6. **Event-Driven паттерны** - поддержка Saga, Outbox паттернов
7. **Комплексное логирование** - многопоточное логирование с Logback
8. **Docker контейнеризация** - полная изоляция окружения

## Docker

Полная поддержка Docker и Docker Compose:

```bash
# Сборка образа
docker build -t kafka-test-framework -f docker/Dockerfile .

# Запуск через docker-compose
docker-compose -f docker/docker-compose.yml run kafka-tests

# Запуск через helper скрипт
./docker/docker-run.sh --parallel
```

Подробнее см. [docker/README.md](docker/README.md)

## Безопасность

1. **SSL/TLS поддержка**
   - Truststore для верификации сервера
   - Keystore для клиентского сертификата
   - Mutual TLS аутентификация

2. **Управление секретами**
   - Переменные окружения
   - GitHub Secrets
   - Нет захардкоженных паролей

3. **Работа с сертификатами**
   - Исключены из репозитория (.gitignore)
   - Base64 кодирование для CI/CD
   - Безопасное хранение

## Документация

1. **README.md** - Обзор проекта и быстрый старт
2. **docker/README.md** - Docker документация
3. **docs/ARCHITECTURE.md** - Архитектура системы
4. **docs/RUN_INSTRUCTIONS.md** - Детальные инструкции по запуску
5. **docs/TEST_CASES_MATRIX.md** - Полная матрица тест-кейсов
6. **SUMMARY.md** - Итоговая сводка проекта

## Вклад в проект

1. Форкните репозиторий
2. Создайте feature ветку
3. Добавьте тесты для новых функций
4. Убедитесь, что все тесты проходят
5. Создайте Pull Request

Подробная документация доступна в папке `docs/`:

- [Архитектура](docs/ARCHITECTURE.md)
- [Инструкция по запуску](docs/RUN_INSTRUCTIONS.md)
- [Матрица тест-кейсов](docs/TEST_CASES_MATRIX.md)
- [Docker Help](docker/README.md)
- [Итоговая сводка проекта](SUMMARY.md)

## License

[![License: MIT](https://img.shields.io/badge/License-MIT-green.svg)](https://opensource.org/licenses/MIT)

## Authors

- **Vitaliy Popravka** - QA Automation Engineer

## Контакты

Для вопросов и предложений создавайте Issue в репозитории.
