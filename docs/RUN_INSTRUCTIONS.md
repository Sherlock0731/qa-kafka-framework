# RUN_INSTRUCTIONS — Kafka Test Automation Framework

## Предварительные требования

| Компонент | Минимальная версия | Проверка |
|-----------|-------------------|---------|
| Java (JDK) | 17 | `java -version` |
| Maven | 3.8 | `mvn -version` |
| Aiven Cloud Kafka | Free tier | Доступ к консоли |
| SSL-сертификаты | — | Скачать из консоли Aiven |
| Docker (опционально) | 20.10 | `docker -v` |

---

## Шаг 1 — SSL-сертификаты

### Получение из Aiven

1. Откройте [console.aiven.io](https://console.aiven.io)
2. Выберите ваш Kafka-сервис
3. Во вкладке **Overview** → **Connection information** скачайте:
   - `kafka.truststore.jks` — CA-сертификат брокера
   - `kafka.keystore.p12` — клиентский сертификат + ключ
   - Запомните или скопируйте **SSL Password** (один для всех)

### Размещение сертификатов

**Windows:**
```
C:\AUTO\kafka_key\kafka.truststore.jks
C:\AUTO\kafka_key\kafka.keystore.p12
```

**Linux/macOS:**
```
~/kafka_key/kafka.truststore.jks
~/kafka_key/kafka.keystore.p12
```

**Произвольный путь** — укажите в конфигурации явно (см. Шаг 2).

---

## Шаг 2 — Настройка подключения

### Вариант А: Переменные окружения (рекомендуется)

```bash
# Обязательные
export KAFKA_BOOTSTRAP_SERVERS=kafka-xxxx.j.aivencloud.com:28330
export KAFKA_SSL_TRUSTSTORE_PASSWORD=ваш_ssl_пароль
export KAFKA_SSL_KEYSTORE_PASSWORD=ваш_ssl_пароль
export KAFKA_SSL_KEY_PASSWORD=ваш_ssl_пароль

# Для Aiven API (удаление топиков, листинг)
export AIVEN_API_TOKEN=ваш_токен_aiven
export AIVEN_PROJECT_NAME=название-проекта
export AIVEN_SERVICE_NAME=название-сервиса

# Опционально: кастомные пути к сертификатам
export KAFKA_SSL_TRUSTSTORE_LOCATION=/path/to/kafka.truststore.jks
export KAFKA_SSL_KEYSTORE_LOCATION=/path/to/kafka.keystore.p12
```

Для постоянного использования добавьте в `~/.bashrc` или `~/.zshrc`.

**Windows (PowerShell):**
```powershell
$env:KAFKA_BOOTSTRAP_SERVERS = "kafka-xxxx.j.aivencloud.com:28330"
$env:KAFKA_SSL_TRUSTSTORE_PASSWORD = "ваш_пароль"
$env:KAFKA_SSL_KEYSTORE_PASSWORD = "ваш_пароль"
$env:KAFKA_SSL_KEY_PASSWORD = "ваш_пароль"
$env:AIVEN_API_TOKEN = "ваш_токен"
```

### Вариант Б: Файл local.properties

Создайте или отредактируйте `src/main/resources/config/local.properties`:

```properties
# Подключение к Kafka
kafka.bootstrap.servers=kafka-xxxx.j.aivencloud.com:28330
kafka.security.protocol=SSL

# SSL-сертификаты (Windows-пути — двойные обратные слэши)
kafka.ssl.truststore.location=C:\\AUTO\\kafka_key\\kafka.truststore.jks
kafka.ssl.truststore.password=ваш_пароль
kafka.ssl.truststore.type=JKS
kafka.ssl.keystore.location=C:\\AUTO\\kafka_key\\kafka.keystore.p12
kafka.ssl.keystore.password=ваш_пароль
kafka.ssl.keystore.type=PKCS12
kafka.ssl.key.password=ваш_пароль

# Aiven API
aiven.api.token=ваш_токен
aiven.project.name=название-проекта
aiven.service.name=название-сервиса

# Опционально: REST API (если используется)
kafka.rest.api.url=https://kafka-xxxx.j.aivencloud.com:28332
kafka.rest.api.username=avnadmin
kafka.rest.api.password=ваш_пароль
```

**Внимание:** не коммитьте `local.properties` с реальными паролями. Файл добавлен в `.gitignore`.

### Вариант В: System properties (runtime)

```bash
mvn test \
  -Dkafka.bootstrap.servers=kafka-xxxx.j.aivencloud.com:28330 \
  -Dkafka.ssl.truststore.password=ваш_пароль \
  -Dkafka.ssl.keystore.password=ваш_пароль \
  -Dkafka.ssl.key.password=ваш_пароль \
  -Daiven.api.token=ваш_токен
```

---

## Шаг 3 — Сборка проекта

```bash
cd qa-kafka-framework

# Только компиляция (без тестов)
mvn compile

# Компиляция + компиляция тестов (без запуска)
mvn test-compile

# Полная сборка без тестов
mvn package -DskipTests
```

---

## Шаг 4 — Запуск тестов

### Все тесты (последовательно)

```bash
mvn test
```

### Smoke-тесты (быстрая проверка, ~7 тестов)

```bash
mvn test -Dtest.groups=smoke
# или через профиль
mvn test -P smoke
```

### По функциональной группе (Maven профили)

```bash
mvn test -P producer           # 12 тестов: отправка сообщений
mvn test -P consumer           # 12 тестов: чтение сообщений
mvn test -P transactions       #  9 тестов: транзакции, exactly-once
mvn test -P idempotence        #  7 тестов: идемпотентность
mvn test -P offset             #  5 тестов: управление offset
mvn test -P partitioning       #  5 тестов: партиционирование
mvn test -P performance        #  4 теста:  производительность
mvn test -P ordering           #  3 теста:  порядок сообщений
mvn test -P dlq                #  3 теста:  Dead Letter Queue
mvn test -P error-handling     #  5 тестов: обработка ошибок
mvn test -P consumer-group     #  1 тест:   rebalance группы
mvn test -P critical           # все критические тесты (тег critical)
```

### Комбинирование профилей

```bash
# Группа + среда
mvn test -P producer,ci

# Несколько групп через теги
mvn test -Dtest.groups="producer | consumer"
mvn test -Dtest.groups="smoke | critical"
```

### Параллельное выполнение

```bash
# 4 потока (рекомендуется для Aiven free tier)
mvn test -P parallel -Dthread.count=4

# 2 потока (минимальный параллелизм)
mvn test -P parallel -Dthread.count=2

# Параллельно + только smoke
mvn test -P parallel,smoke -Dthread.count=4
```

**Важно при параллельном запуске:**
- Каждый тест создаёт уникальный топик (UUID-суффикс)
- Consumer group ID уникален для каждого теста (UUID-суффикс)
- `@AfterAll globalCleanup()` закрывает все ресурсы из всех потоков

### Через bash-скрипт

```bash
chmod +x run-tests.sh

# Все тесты последовательно
./run-tests.sh

# Только smoke
./run-tests.sh --groups smoke

# Параллельно с 4 потоками
./run-tests.sh --parallel 4

# Конкретная группа + окружение
./run-tests.sh --groups consumer --env ci

# Помощь
./run-tests.sh --help
```

---

## Шаг 5 — Allure-отчёт

### Генерация и запуск в браузере

```bash
# Генерация статического отчёта
mvn allure:report

# Запуск встроенного HTTP-сервера (откроет браузер автоматически)
mvn allure:serve
```

Отчёт доступен по адресу: `http://localhost:PORT/`

### Путь к результатам

```
target/allure-results/    # raw JSON-результаты
target/allure-report/     # сгенерированный HTML-отчёт
target/surefire-reports/  # JUnit XML (для CI-систем)
```

### Онлайн-отчёт (GitHub Pages)

После успешного CI: [https://sherlock0731.github.io/qa-kafka-framework/](https://sherlock0731.github.io/qa-kafka-framework/)

---

## Запуск в Docker

### Подготовка

```bash
# Сертификаты должны быть доступны при сборке
ls docker/
# Dockerfile  docker-compose.yml  docker-run.sh  README.md
```

### Docker Compose (рекомендуется)

```bash
cd docker

# Запуск с переменными окружения
KAFKA_BOOTSTRAP_SERVERS=kafka-xxxx.j.aivencloud.com:28330 \
KAFKA_SSL_TRUSTSTORE_PASSWORD=ваш_пароль \
KAFKA_SSL_KEYSTORE_PASSWORD=ваш_пароль \
KAFKA_SSL_KEY_PASSWORD=ваш_пароль \
AIVEN_API_TOKEN=ваш_токен \
docker-compose up --build

# С .env файлом
cp .env.example .env   # заполните .env своими значениями
docker-compose up --build
```

### Docker напрямую

```bash
cd docker
./docker-run.sh
```

Подробнее: [docker/README.md](../docker/README.md)

---

## CI/CD (GitHub Actions)

### Настройка секретов

В репозитории → **Settings** → **Secrets and variables** → **Actions** добавьте:

| Secret | Описание |
|--------|----------|
| `KAFKA_BOOTSTRAP_SERVERS` | Адрес брокера (host:port) |
| `KAFKA_SSL_TRUSTSTORE_PASSWORD` | Пароль truststore |
| `KAFKA_SSL_KEYSTORE_PASSWORD` | Пароль keystore |
| `KAFKA_SSL_KEY_PASSWORD` | Пароль ключа |
| `AIVEN_API_TOKEN` | Bearer-токен Aiven API |
| `AIVEN_PROJECT_NAME` | Название проекта Aiven |
| `AIVEN_SERVICE_NAME` | Название Kafka-сервиса |

Также загрузите сертификаты как зашифрованные secrets (base64) или настройте их монтирование. Подробнее: [docs/GITHUB_SECRETS_SETUP.md](GITHUB_SECRETS_SETUP.md)

### Ручной запуск пайплайна

В GitHub → **Actions** → **Run workflow** → выберите ветку и нажмите **Run workflow**.

---

## Устранение неполадок

### SSL handshake failed

```
javax.net.ssl.SSLHandshakeException: PKIX path building failed
```

**Причины и решения:**
- Неверный путь к `truststore.jks` → проверьте `kafka.ssl.truststore.location`
- Устаревший сертификат → скачайте новые сертификаты из Aiven
- Неверный тип truststore → должен быть `JKS`, keystore — `PKCS12`
- Неверный пароль → все три пароля (truststore, keystore, key) должны совпадать

### Consumer не получает сообщения (timeout)

```
WARN - Timeout reached. Polled 0 records, expected min: 10
```

**Причины и решения:**
- Rebalance не завершился → добавьте `AsyncTestHelper.waitFor(5)` после `initConsumer()` и pre-warm poll
- Consumer инициализирован после отправки → следуйте паттерну consumer-first initialization
- Недостаточный таймаут → увеличьте в `AsyncTestHelper.pollWithRetry(consumer, 60, count)` (до 60–90с для Aiven)
- Неверная переменная `kafka.consumer.auto.offset.reset` → должно быть `earliest` для новых групп

### Топик не создаётся (timeout)

```
ERROR - Failed to create topic after 5 attempts
```

**Причины и решения:**
- Rate limiting Aiven free tier → уменьшите параллелизм, добавьте `waitFor()` между тестами
- AdminClient SSL проблема → проверьте SSL-настройки в `KafkaTopicManager.createAdminClient()`
- Брокер недоступен → проверьте `KAFKA_BOOTSTRAP_SERVERS`

### Утечка памяти / OOM при параллельном запуске

- Убедитесь, что в `BaseTest` присутствует `@AfterAll globalCleanup()` с вызовом `manager.closeAll()`
- Не используйте `@Disabled` тесты без закрытия ресурсов в `finally`
- Проверьте счётчик: `consumerManager.getTrackedConsumerCount()`

### Не работает Allure-отчёт

```bash
# Проверьте наличие результатов
ls target/allure-results/

# Убедитесь, что aspectjweaver в classpath
mvn dependency:get -Dartifact=org.aspectj:aspectjweaver:1.9.21

# Принудительная регенерация
mvn allure:report -Dallure.results.directory=target/allure-results
```

---

## Полезные команды

```bash
# Проверка зависимостей на уязвимости (OWASP)
mvn verify -P security-check -DnvdApiKey=ВАШ_КЛЮЧ

# Очистка артефактов сборки
mvn clean

# Запуск конкретного тест-класса
mvn test -Dtest=ProducerTests

# Запуск конкретного метода
mvn test -Dtest=ProducerTests#testSendSingleMessage

# Запуск с подробным выводом Surefire
mvn test -P smoke -Dsurefire.useFile=false

# Параллельно только smoke с подробным логом
mvn test -P parallel,smoke -Dthread.count=2 -Dsurefire.useFile=false
```
