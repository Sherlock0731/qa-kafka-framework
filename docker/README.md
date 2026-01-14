# Docker для Kafka Test Framework

Полная поддержка контейнеризации тестов с использованием Docker и Docker Compose.

## Содержание

- [Быстрый старт](#быстрый-старт)
- [Требования](#требования)
- [Структура файлов](#структура-файлов)
- [Конфигурация](#конфигурация)
- [Запуск тестов](#запуск-тестов)
- [Просмотр отчетов](#просмотр-отчетов)
- [Troubleshooting](#troubleshooting)

## Быстрый старт

### 1. Установка Docker

**Windows:**
```powershell
# Скачайте и установите Docker Desktop
# https://www.docker.com/products/docker-desktop
```

**macOS:**
```bash
brew install --cask docker
```

**Linux (Ubuntu):**
```bash
sudo apt update
sudo apt install docker.io docker-compose
sudo usermod -aG docker $USER
# Перелогиньтесь после этого
```

### 2. Подготовка сертификатов

Создайте директорию и поместите туда ваши Kafka сертификаты:

```bash
mkdir kafka_key
# Скопируйте файлы:
# - kafka.truststore.jks
# - kafka.keystore.p12
```

### 3. Настройка переменных окружения

Создайте файл `.env` в корне проекта:

```bash
# Пароли для SSL сертификатов
KAFKA_SSL_TRUSTSTORE_PASSWORD=ваш_пароль
KAFKA_SSL_KEYSTORE_PASSWORD=ваш_пароль

# Пароли для API
KAFKA_REST_API_PASSWORD=ваш_пароль
KAFKA_SCHEMA_REGISTRY_PASSWORD=ваш_пароль

# Путь к сертификатам (опционально)
KAFKA_CERTS_PATH=./kafka_key
```

### 4. Запуск тестов

```bash
# Все тесты последовательно
./docker/docker-run.sh

# Только smoke тесты
./docker/docker-run.sh --smoke

# Параллельное выполнение
./docker/docker-run.sh --parallel

# С пересборкой образа
./docker/docker-run.sh --build
```

## Требования

- **Docker**: 20.10+
- **Docker Compose**: 2.0+
- **Память**: минимум 4GB RAM для контейнера
- **Диск**: 2GB свободного места

## Структура файлов

```
docker/
├── Dockerfile              # Многоступенчатая сборка образа
├── docker-compose.yml      # Конфигурация сервисов
├── docker-run.sh          # Скрипт запуска (Linux/macOS)
└── README.md              # Эта документация

.dockerignore              # Игнорируемые файлы при сборке
```

## Конфигурация

### Dockerfile

Двухступенчатая сборка:

**Этап 1 (Builder):**
- Базовый образ: `maven:3.9-eclipse-temurin-17`
- Загрузка зависимостей
- Компиляция проекта

**Этап 2 (Runtime):**
- Базовый образ: `eclipse-temurin:17-jre-alpine`
- Минимальный размер образа
- Безопасность (non-root пользователь)

### Docker Compose сервисы

#### 1. kafka-tests (основной)
Запускает все тесты последовательно:
```bash
docker-compose -f docker/docker-compose.yml run kafka-tests
```

#### 2. kafka-tests-smoke
Только smoke тесты:
```bash
docker-compose -f docker/docker-compose.yml run kafka-tests-smoke
```

#### 3. kafka-tests-parallel
Параллельное выполнение (8 потоков):
```bash
docker-compose -f docker/docker-compose.yml run kafka-tests-parallel
```

#### 4. allure-report
Сервер Allure отчетов:
```bash
docker-compose -f docker/docker-compose.yml up -d allure-report
```

## Запуск тестов

### Базовые команды

**Все тесты:**
```bash
./docker/docker-run.sh
```

**Smoke тесты:**
```bash
./docker/docker-run.sh --smoke
```

**Параллельное выполнение:**
```bash
./docker/docker-run.sh --parallel
```

### Дополнительные опции

**Пересборка образа:**
```bash
./docker/docker-run.sh --build
```

**Очистка volumes:**
```bash
./docker/docker-run.sh --clean
```

**Без вывода логов:**
```bash
./docker/docker-run.sh --no-logs
```

### Использование docker-compose напрямую

**Запуск конкретных тестов:**
```bash
docker-compose -f docker/docker-compose.yml run kafka-tests \
  test -Dgroups=producer
```

**Запуск с переменными:**
```bash
docker-compose -f docker/docker-compose.yml run \
  -e THREAD_COUNT=16 \
  kafka-tests-parallel
```

**Запуск в фоне:**
```bash
docker-compose -f docker/docker-compose.yml up -d kafka-tests
docker-compose -f docker/docker-compose.yml logs -f kafka-tests
```

## Просмотр отчетов

### Allure Report Server

**Запуск сервера:**
```bash
./docker/docker-run.sh --report
# или
docker-compose -f docker/docker-compose.yml up -d allure-report
```

**Доступ:**
- URL: http://localhost:5050
- UI: http://localhost:5252
- Логин: `admin`
- Пароль: `admin`

**Остановка:**
```bash
docker-compose -f docker/docker-compose.yml down allure-report
```

### Локальные отчеты

Результаты сохраняются в:
```
target/
├── allure-results/     # Сырые результаты
├── allure-reports/     # Сгенерированные отчеты
└── logs/              # Логи выполнения
```

## Управление контейнерами

### Просмотр логов

**Реального времени:**
```bash
docker-compose -f docker/docker-compose.yml logs -f kafka-tests
```

**Последние 100 строк:**
```bash
docker-compose -f docker/docker-compose.yml logs --tail=100 kafka-tests
```

### Остановка сервисов

```bash
# Остановить все
docker-compose -f docker/docker-compose.yml down

# С удалением volumes
docker-compose -f docker/docker-compose.yml down -v

# Остановить конкретный сервис
docker-compose -f docker/docker-compose.yml stop allure-report
```

### Список контейнеров

```bash
docker-compose -f docker/docker-compose.yml ps
```

## Troubleshooting

### Проблема: Недостаточно памяти

**Симптомы:**
```
OutOfMemoryError: Java heap space
```

**Решение:**
Увеличьте память в `docker-compose.yml`:
```yaml
deploy:
  resources:
    limits:
      memory: 8G
```

Или через переменную окружения:
```bash
export MAVEN_OPTS="-Xmx4096m"
./docker/docker-run.sh
```

### Проблема: Сертификаты не найдены

**Симптомы:**
```
FileNotFoundException: /app/kafka_key/kafka.truststore.jks
```

**Решение:**
1. Проверьте путь к сертификатам:
```bash
ls -la kafka_key/
```

2. Убедитесь, что путь указан правильно в `.env`:
```bash
KAFKA_CERTS_PATH=./kafka_key
```

3. Проверьте монтирование в `docker-compose.yml`:
```yaml
volumes:
  - ${KAFKA_CERTS_PATH:-./kafka_key}:/app/kafka_key:ro
```

### Проблема: Permission denied

**Симптомы:**
```
Permission denied: /app/target/logs
```

**Решение:**
Дайте права на запись:
```bash
chmod -R 777 target/
```

Или запустите от своего пользователя:
```bash
docker-compose -f docker/docker-compose.yml run \
  -u $(id -u):$(id -g) \
  kafka-tests
```

### Проблема: Образ не собирается

**Симптомы:**
```
ERROR: failed to solve: process "/bin/sh -c mvn dependency:go-offline -B"
```

**Решение:**
1. Проверьте подключение к интернету
2. Очистите Docker кеш:
```bash
docker builder prune -a
```

3. Пересоберите с --no-cache:
```bash
docker-compose -f docker/docker-compose.yml build --no-cache
```

### Проблема: Тесты зависают

**Симптомы:**
Тесты не завершаются долгое время

**Решение:**
1. Увеличьте таймауты:
```bash
docker-compose -f docker/docker-compose.yml run \
  -e TEST_TIMEOUT_SECONDS=120 \
  kafka-tests
```

2. Проверьте подключение к Kafka:
```bash
docker-compose -f docker/docker-compose.yml run kafka-tests \
  bash -c "curl -v KAFKA_BOOTSTRAP_SERVERS"
```

### Проблема: Docker Compose версии

**Симптомы:**
```
ERROR: version is obsolete
```

**Решение:**
Обновите Docker Compose:
```bash
# Linux
sudo apt update
sudo apt install docker-compose-plugin

# macOS
brew upgrade docker-compose

# Windows
# Обновите Docker Desktop
```

## Безопасность

### Не храните секреты в docker-compose.yml

❌ **Плохо:**
```yaml
environment:
  - KAFKA_SSL_TRUSTSTORE_PASSWORD=mypassword123
```

✅ **Хорошо:**
```yaml
environment:
  - KAFKA_SSL_TRUSTSTORE_PASSWORD=${KAFKA_SSL_TRUSTSTORE_PASSWORD}
```

### Используйте Docker secrets

Для продакшена используйте Docker Swarm secrets:
```yaml
secrets:
  kafka_truststore_pass:
    external: true
```

### Ограничьте ресурсы

Всегда устанавливайте лимиты:
```yaml
deploy:
  resources:
    limits:
      cpus: '4'
      memory: 4G
```

## Оптимизация

### Ускорение сборки

**1. Используйте .dockerignore:**
Уже настроено в проекте

**2. Кешируйте Maven зависимости:**
```yaml
volumes:
  - maven-cache:/root/.m2
```

**3. Используйте BuildKit:**
```bash
DOCKER_BUILDKIT=1 docker build -t kafka-tests .
```

### Ускорение выполнения тестов

**1. Параллельное выполнение:**
```bash
./docker/docker-run.sh --parallel
```

**2. Увеличьте ресурсы:**
```yaml
deploy:
  resources:
    limits:
      cpus: '8'
      memory: 8G
```

**3. Используйте SSD:**
Убедитесь, что Docker использует SSD диск

## CI/CD Integration

### GitHub Actions

```yaml
- name: Run tests in Docker
  run: |
    echo "${{ secrets.KAFKA_TRUSTSTORE_BASE64 }}" | base64 -d > kafka_key/kafka.truststore.jks
    echo "${{ secrets.KAFKA_KEYSTORE_BASE64 }}" | base64 -d > kafka_key/kafka.keystore.p12
    ./docker/docker-run.sh --parallel
```

### GitLab CI

```yaml
test:
  image: docker:latest
  services:
    - docker:dind
  script:
    - docker-compose -f docker/docker-compose.yml run kafka-tests
```

### Jenkins

```groovy
stage('Run Tests') {
    steps {
        sh './docker/docker-run.sh --parallel'
    }
}
```

## Дополнительные ресурсы

- [Официальная документация Docker](https://docs.docker.com/)
- [Docker Compose документация](https://docs.docker.com/compose/)
- [Best practices для Dockerfile](https://docs.docker.com/develop/develop-images/dockerfile_best-practices/)
- [Allure Docker Service](https://github.com/fescobar/allure-docker-service)

## Полезные команды

```bash
# Удалить все неиспользуемые образы
docker image prune -a

# Посмотреть размер образов
docker images | grep kafka-test

# Зайти в контейнер
docker-compose -f docker/docker-compose.yml run kafka-tests bash

# Скопировать файл из контейнера
docker cp kafka-test-runner:/app/target/logs/test.log ./

# Мониторинг ресурсов
docker stats kafka-test-runner

# Экспорт образа
docker save kafka-test-framework:latest | gzip > kafka-tests.tar.gz

# Импорт образа
docker load < kafka-tests.tar.gz
```

---

**Версия документации:** 1.0.0  
**Дата обновления:** 2026-01-13
