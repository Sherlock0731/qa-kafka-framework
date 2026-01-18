# Инструкции по запуску Kafka Test Framework

## Настройка предварительных требований

### 1. Установка Java 17
**Windows:**
```powershell
winget install EclipseAdoptium.Temurin.17.JDK
```

**macOS:**
```bash
brew install openjdk@17
```

**Linux:**
```bash
sudo apt update
sudo apt install openjdk-17-jdk
```

Проверка установки:
```bash
java -version  # Должна показать Java 17
```

### 2. Установка Maven
**Windows:**
```powershell
winget install Apache.Maven
```

**macOS:**
```bash
brew install maven
```

**Linux:**
```bash
sudo apt install maven
```

Проверка установки:
```bash
mvn -version
```

### 3. Настройка Kafka SSL сертификатов

#### Windows:
```powershell
# Создайте директорию
New-Item -ItemType Directory -Force -Path C:\kafka_key

# Скопируйте ваши сертификаты
Copy-Item kafka.truststore.jks C:\kafka_key\
Copy-Item kafka.keystore.p12 C:\kafka_key\
```

#### Linux/macOS:
```bash
# Создайте директорию
mkdir -p ~/kafka_key

# Скопируйте ваши сертификаты
cp kafka.truststore.jks ~/kafka_key/
cp kafka.keystore.p12 ~/kafka_key/
```

### 4. Настройка паролей

Создайте файл `.env` в корне проекта:
```bash
cp .env.example .env
# Отредактируйте .env своими паролями
```

**Или установите переменные окружения:**

**Windows PowerShell:**
```powershell
$env:KAFKA_SSL_TRUSTSTORE_PASSWORD="ваш_пароль"
$env:KAFKA_SSL_KEYSTORE_PASSWORD="ваш_пароль"
$env:KAFKA_REST_API_PASSWORD="ваш_пароль"
```

**Linux/macOS:**
```bash
export KAFKA_SSL_TRUSTSTORE_PASSWORD="ваш_пароль"
export KAFKA_SSL_KEYSTORE_PASSWORD="ваш_пароль"
export KAFKA_REST_API_PASSWORD="ваш_пароль"
```

## Запуск тестов

### Быстрые команды

**Запустить все тесты:**
```bash
mvn clean test
```

**Запустить smoke тесты:**
```bash
mvn test -Dgroups=smoke
```

**Запустить с определенным количеством потоков:**
```bash
mvn test -Pparallel -Dthread.count=4
```

### Запуск по категориям тестов

**Producer тесты:**
```bash
mvn test -Dgroups=producer
```

**Consumer тесты:**
```bash
mvn test -Dgroups=consumer
```

**Идемпотентность:**
```bash
mvn test -Dgroups=idempotence
```

**Упорядоченность:**
```bash
mvn test -Dgroups=ordering
```

**Управление offset:**
```bash
mvn test -Dgroups=offset
```

**Партиционирование:**
```bash
mvn test -Dgroups=partitioning
```

**DLQ тесты:**
```bash
mvn test -Dgroups=dlq
```

### Запуск нескольких групп тегов

**Логика ИЛИ (запустить если ЛЮБОЙ тег совпадает):**
```bash
mvn test -Dgroups="producer | consumer"
```

**Логика И (запустить если ВСЕ теги совпадают):**
```bash
mvn test -Dgroups="producer & critical"
```

**Логика НЕ (исключить теги):**
```bash
mvn test -Dgroups="!slow"
```

**Сложные выражения:**
```bash
mvn test -Dgroups="(producer | consumer) & smoke"
```

### Параллельное выполнение

**Параллельно с 4 потоками (по умолчанию):**
```bash
./run-tests.sh --parallel
```

**Параллельно с произвольным количеством потоков:**
```bash
./run-tests.sh --parallel 8
```

**Строгое параллельное (fork-based):**
```bash
mvn test -Pparallel-strict -Dthread.count=4
```

### Запуск для конкретного окружения

**Локальное окружение:**
```bash
mvn test -Denv=local
```

**CI окружение:**
```bash
mvn test -Denv=ci
```

## Просмотр отчетов

### Allure отчеты

**Сгенерировать и открыть отчет:**
```bash
mvn allure:report
mvn allure:serve
```

**Только сгенерировать:**
```bash
mvn allure:report
```

Затем откройте `target/allure-report/index.html` в браузере.

## Устранение неполадок

### Проблема: Не удалось подключиться по SSL

**Решение:**
```bash
# Проверьте наличие сертификатов
ls -la ~/kafka_key/

# Проверьте переменные окружения
echo $KAFKA_SSL_TRUSTSTORE_PASSWORD

# Запустите с явным указанием путей
mvn test \
  -Dkafka.ssl.truststore.location=~/kafka_key/kafka.truststore.jks \
  -Dkafka.ssl.truststore.password=$KAFKA_SSL_TRUSTSTORE_PASSWORD
```

### Проблема: Тесты зависают по таймауту

**Увеличьте таймауты:**
```bash
mvn test \
  -Dtest.timeout.seconds=60 \
  -Dtest.poll.timeout.seconds=10
```

### Проблема: Топик уже существует

**Включите очистку:**
```bash
mvn test -Dtest.cleanup.topics=true
```

## Docker запуск

### Быстрый старт

```bash
# Подготовка
mkdir kafka_key
cp /путь/к/сертификатам/* kafka_key/

# Создайте .env
cat > .env << EOF
KAFKA_SSL_TRUSTSTORE_PASSWORD=ваш_пароль
KAFKA_SSL_KEYSTORE_PASSWORD=ваш_пароль
KAFKA_REST_API_PASSWORD=ваш_пароль
EOF

# Запуск
./docker/docker-run.sh

# Smoke тесты
./docker/docker-run.sh --smoke

# Параллельно
./docker/docker-run.sh --parallel

# Просмотр отчета
./docker/docker-run.sh --report
```

Подробнее см. [docker/README.md](../docker/README.md)

---

**Версия:** 1.1.0  
**Дата:** 2026-01-18
