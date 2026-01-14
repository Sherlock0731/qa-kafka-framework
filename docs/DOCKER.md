# Docker - Руководство по запуску

## Обзор

Фреймворк поддерживает запуск в Docker контейнерах с помощью Docker и Docker Compose.

**Преимущества Docker:**
- Изолированная среда выполнения
- Не требуется установка Java и Maven на хост-машине
- Одинаковое окружение на всех машинах
- Легко масштабируется
- Встроенный Allure Report Viewer

## Быстрый старт

### Предварительные требования

**Установите Docker и Docker Compose:**

**Windows:**
```powershell
# Установите Docker Desktop
winget install Docker.DockerDesktop
```

**macOS:**
```bash
brew install --cask docker
```

**Linux:**
```bash
# Ubuntu/Debian
sudo apt-get update
sudo apt-get install docker.io docker-compose-plugin

# Добавьте пользователя в группу docker
sudo usermod -aG docker $USER
```

### Шаг 1: Подготовка SSL сертификатов

Создайте директорию `docker/kafka_key` и поместите туда сертификаты:

**Windows PowerShell:**
```powershell
cd qa-kafka-framework
mkdir -p docker\kafka_key
Copy-Item C:\kafka_key\kafka.truststore.jks docker\kafka_key\
Copy-Item C:\kafka_key\kafka.keystore.p12 docker\kafka_key\
```

**Linux/macOS:**
```bash
cd qa-kafka-framework
mkdir -p docker/kafka_key
cp ~/kafka_key/kafka.truststore.jks docker/kafka_key/
cp ~/kafka_key/kafka.keystore.p12 docker/kafka_key/
```

### Шаг 2: Настройка переменных окружения

Создайте файл `docker/.env`:

```bash
cd docker
cat > .env << 'EOF'
# Пароли для SSL сертификатов
KAFKA_SSL_TRUSTSTORE_PASSWORD=ваш_пароль_truststore
KAFKA_SSL_KEYSTORE_PASSWORD=ваш_пароль_keystore

# Kafka REST API пароль
KAFKA_REST_API_PASSWORD=ваш_пароль_rest_api

# Schema Registry пароль
KAFKA_SCHEMA_REGISTRY_PASSWORD=ваш_пароль_schema_registry

# Настройки тестирования (опционально)
THREAD_COUNT=1
TEST_ENV=local
TEST_CLEANUP_TOPICS=true
EOF
```

### Шаг 3: Сборка Docker образа

```bash
cd docker
docker-compose build
```

## Запуск тестов

### Вариант 1: Все тесты последовательно

```bash
cd docker
docker-compose run --rm kafka-tests mvn clean test
```

### Вариант 2: Запуск с определенными тегами

**Smoke тесты:**
```bash
docker-compose run --rm kafka-tests mvn test -Dgroups=smoke
```

**Producer тесты:**
```bash
docker-compose run --rm kafka-tests mvn test -Dgroups=producer
```

**Consumer тесты:**
```bash
docker-compose run --rm kafka-tests mvn test -Dgroups=consumer
```

**Critical тесты:**
```bash
docker-compose run --rm kafka-tests mvn test -Dgroups=critical
```

### Вариант 3: Параллельный запуск

**4 потока:**
```bash
docker-compose run --rm \
  -e THREAD_COUNT=4 \
  kafka-tests mvn test -Pparallel -Dthread.count=4
```

**8 потоков:**
```bash
docker-compose run --rm \
  -e THREAD_COUNT=8 \
  kafka-tests mvn test -Pparallel -Dthread.count=8
```

### Вариант 4: Использование run-tests.sh скрипта

```bash
# Все тесты
docker-compose run --rm kafka-tests ./run-tests.sh

# Параллельно с 4 потоками
docker-compose run --rm kafka-tests ./run-tests.sh --parallel 4

# Smoke тесты
docker-compose run --rm kafka-tests ./run-tests.sh --groups smoke

# Параллельно с группой
docker-compose run --rm kafka-tests ./run-tests.sh --parallel 4 --groups producer
```

## Просмотр отчетов

### Allure Report (веб-интерфейс)

**Запуск Allure Report сервера:**

```bash
cd docker
docker-compose --profile report up -d allure-report
```

Откройте в браузере: http://localhost:5050

**Остановка сервера:**
```bash
docker-compose --profile report down
```

### Генерация статического отчета

```bash
docker-compose run --rm kafka-tests mvn allure:report

# Отчет будет в docker/target/allure-report/
```

### Просмотр логов

**Во время выполнения тестов:**
```bash
docker-compose logs -f kafka-tests
```

**Log Viewer (опционально):**
```bash
docker-compose --profile monitoring up -d log-viewer
```

Откройте в браузере: http://localhost:8888

## Структура директорий Docker

```
docker/
├── Dockerfile              # Описание образа
├── docker-compose.yml      # Оркестрация контейнеров
├── .env                    # Переменные окружения (создать вручную)
├── kafka_key/             # SSL сертификаты (создать вручную)
│   ├── kafka.truststore.jks
│   └── kafka.keystore.p12
├── target/                # Результаты сборки Maven
│   ├── allure-results/    # Результаты тестов
│   └── allure-report/     # Allure отчет
└── logs/                  # Логи тестов
```

## Расширенное использование

### Переопределение конфигурации

**Изменить Kafka сервер:**
```bash
docker-compose run --rm \
  -e KAFKA_BOOTSTRAP_SERVERS=localhost:9092 \
  kafka-tests mvn test
```

**Изменить таймауты:**
```bash
docker-compose run --rm \
  -e TEST_TIMEOUT_SECONDS=60 \
  -e TEST_POLL_TIMEOUT_SECONDS=10 \
  kafka-tests mvn test
```

**Отключить очистку топиков:**
```bash
docker-compose run --rm \
  -e TEST_CLEANUP_TOPICS=false \
  kafka-tests mvn test
```

### Запуск конкретного теста

```bash
docker-compose run --rm kafka-tests \
  mvn test -Dtest=ProducerTests#testSendSingleMessage
```

### Отладка контейнера

**Войти в контейнер:**
```bash
docker-compose run --rm --entrypoint /bin/bash kafka-tests
```

**Внутри контейнера:**
```bash
# Проверить Maven
mvn --version

# Проверить сертификаты
ls -la /home/kafkatest/kafka_key/

# Проверить переменные окружения
env | grep KAFKA

# Запустить тесты вручную
mvn test -Dgroups=smoke
```

### Просмотр логов контейнера

```bash
# Все логи
docker-compose logs kafka-tests

# Последние 100 строк
docker-compose logs --tail=100 kafka-tests

# Следить за логами в реальном времени
docker-compose logs -f kafka-tests
```

## Docker Compose профили

Фреймворк использует профили для опциональных сервисов:

### Профиль `report` - Allure Report Server

```bash
# Запустить
docker-compose --profile report up -d

# Остановить
docker-compose --profile report down
```

**Включает:**
- Allure Report веб-сервер на порту 5050
- Автообновление отчетов каждые 3 секунды
- История последних 20 запусков

### Профиль `monitoring` - Log Viewer

```bash
# Запустить
docker-compose --profile monitoring up -d

# Остановить
docker-compose --profile monitoring down
```

**Включает:**
- Dozzle log viewer на порту 8888
- Просмотр логов всех контейнеров
- Реалтайм мониторинг

### Запуск всех профилей

```bash
docker-compose --profile report --profile monitoring up -d
```

## CI/CD интеграция

### GitHub Actions

```yaml
name: Docker Tests

on: [push, pull_request]

jobs:
  test:
    runs-on: ubuntu-latest
    
    steps:
      - uses: actions/checkout@v4
      
      - name: Create certificates
        run: |
          mkdir -p docker/kafka_key
          echo "${{ secrets.KAFKA_TRUSTSTORE_BASE64 }}" | base64 -d > docker/kafka_key/kafka.truststore.jks
          echo "${{ secrets.KAFKA_KEYSTORE_BASE64 }}" | base64 -d > docker/kafka_key/kafka.keystore.p12
      
      - name: Create .env file
        run: |
          cat > docker/.env << EOF
          KAFKA_SSL_TRUSTSTORE_PASSWORD=${{ secrets.KAFKA_SSL_TRUSTSTORE_PASSWORD }}
          KAFKA_SSL_KEYSTORE_PASSWORD=${{ secrets.KAFKA_SSL_KEYSTORE_PASSWORD }}
          KAFKA_REST_API_PASSWORD=${{ secrets.KAFKA_REST_API_PASSWORD }}
          KAFKA_SCHEMA_REGISTRY_PASSWORD=${{ secrets.KAFKA_SCHEMA_REGISTRY_PASSWORD }}
          EOF
      
      - name: Build Docker image
        run: |
          cd docker
          docker-compose build
      
      - name: Run tests
        run: |
          cd docker
          docker-compose run --rm kafka-tests mvn clean test
      
      - name: Upload Allure results
        if: always()
        uses: actions/upload-artifact@v4
        with:
          name: allure-results
          path: docker/target/allure-results
```

### GitLab CI

```yaml
docker-tests:
  image: docker:latest
  services:
    - docker:dind
  
  before_script:
    - apk add docker-compose
    - mkdir -p docker/kafka_key
    - echo "$KAFKA_TRUSTSTORE_BASE64" | base64 -d > docker/kafka_key/kafka.truststore.jks
    - echo "$KAFKA_KEYSTORE_BASE64" | base64 -d > docker/kafka_key/kafka.keystore.p12
  
  script:
    - cd docker
    - docker-compose build
    - docker-compose run --rm kafka-tests mvn clean test
  
  artifacts:
    when: always
    paths:
      - docker/target/allure-results
    expire_in: 1 week
```

## Оптимизация

### Кэширование Maven зависимостей

Docker образ использует multi-stage build для кэширования зависимостей:
- Первый раз сборка ~5-10 минут
- Повторная сборка ~1-2 минуты (если не менялись зависимости)

### Уменьшение размера образа

Образ использует:
- Alpine Linux (минимальный размер)
- JRE вместо JDK (меньший размер)
- Multi-stage build (без build артефактов в финальном образе)

**Размер образа:** ~400-500 MB

### Быстрый запуск

Для ускорения запуска тестов:

```bash
# Предварительная сборка
docker-compose build

# Быстрый запуск без rebuild
docker-compose run --rm kafka-tests mvn test
```

## Troubleshooting

### Проблема: Контейнер не может подключиться к Kafka

**Решение:**
```bash
# Проверьте сертификаты
docker-compose run --rm --entrypoint ls kafka-tests -la /home/kafkatest/kafka_key/

# Проверьте переменные окружения
docker-compose run --rm --entrypoint env kafka-tests | grep KAFKA

# Проверьте сетевое подключение
docker-compose run --rm --entrypoint ping kafka-tests -c 4 xxxxxxxxxxxx.com
```

### Проблема: Ошибка "Permission denied" на сертификаты

**Решение:**
```bash
# Установите правильные права
chmod 644 docker/kafka_key/*.jks
chmod 644 docker/kafka_key/*.p12
```

### Проблема: Maven не может скачать зависимости

**Решение:**
```bash
# Очистите кэш Docker
docker-compose down -v
docker system prune -a

# Пересоберите образ
docker-compose build --no-cache
```

### Проблема: Out of memory

**Решение:**

Увеличьте лимиты памяти в `docker-compose.yml`:

```yaml
deploy:
  resources:
    limits:
      memory: 8G
    reservations:
      memory: 4G
```

Или увеличьте MAVEN_OPTS:

```bash
docker-compose run --rm \
  -e MAVEN_OPTS="-Xmx4096m -Xms2048m" \
  kafka-tests mvn test
```

### Проблема: Тесты висят или таймаутят

**Решение:**
```bash
# Увеличьте таймауты
docker-compose run --rm \
  -e TEST_TIMEOUT_SECONDS=60 \
  -e KAFKA_CONSUMER_SESSION_TIMEOUT_MS=60000 \
  kafka-tests mvn test
```

## Полезные команды

### Управление образами

```bash
# Список образов
docker images | grep kafka

# Удалить образ
docker rmi kafka-test-framework

# Пересборка без кэша
docker-compose build --no-cache
```

### Управление контейнерами

```bash
# Список контейнеров
docker ps -a | grep kafka

# Остановить все контейнеры
docker-compose down

# Удалить контейнеры и volumes
docker-compose down -v
```

### Очистка

```bash
# Удалить остановленные контейнеры
docker container prune

# Удалить неиспользуемые образы
docker image prune -a

# Полная очистка Docker
docker system prune -a --volumes
```

## Рекомендации

1. **Используйте .env файл** для конфиденциальных данных
2. **Добавьте docker/ в .gitignore** если храните сертификаты там
3. **Регулярно обновляйте базовые образы** для безопасности
4. **Используйте профили** для опциональных сервисов
5. **Мониторьте ресурсы** во время параллельного запуска
6. **Делайте backup** результатов тестов и логов

## Примеры использования

### Разработка

```bash
# Быстрая проверка smoke тестов
docker-compose run --rm kafka-tests mvn test -Dgroups=smoke

# Отладка с оболочкой
docker-compose run --rm --entrypoint /bin/bash kafka-tests

# Генерация отчета
docker-compose run --rm kafka-tests mvn allure:report
docker-compose --profile report up -d
# Открыть http://localhost:5050
```

### CI/CD

```bash
# Полный цикл
docker-compose build
docker-compose run --rm kafka-tests mvn clean test
docker-compose run --rm kafka-tests mvn allure:report
# Сохранить артефакты из docker/target/
```

### Production monitoring

```bash
# Запуск с мониторингом
docker-compose --profile monitoring up -d
docker-compose run --rm kafka-tests mvn test -Pparallel -Dthread.count=4
# Следить за логами в http://localhost:8888
```

## Заключение

Docker обеспечивает:
- Изолированную среду выполнения
- Воспроизводимые результаты
- Легкое масштабирование
- Простую интеграцию в CI/CD

Для локальной разработки можно использовать как Docker, так и нативный Maven - на ваш выбор.
