#!/bin/bash

# Скрипт для запуска Kafka тестов в Docker
# Поддержка: Windows (Git Bash), Linux, macOS

set -e

# Цвета для вывода
RED='\033[0;31m'
GREEN='\033[0;32m'
YELLOW='\033[1;33m'
BLUE='\033[0;34m'
NC='\033[0m' # No Color

echo -e "${GREEN}=====================================${NC}"
echo -e "${GREEN}Kafka Test Framework - Docker Runner${NC}"
echo -e "${GREEN}=====================================${NC}"
echo ""

# Проверка Docker
if ! command -v docker &> /dev/null; then
    echo -e "${RED}Ошибка: Docker не установлен!${NC}"
    echo "Установите Docker Desktop: https://www.docker.com/products/docker-desktop"
    exit 1
fi

# Проверка docker-compose
if ! command -v docker-compose &> /dev/null; then
    echo -e "${RED}Ошибка: docker-compose не установлен!${NC}"
    echo "Установите docker-compose или используйте Docker Desktop"
    exit 1
fi

# Значения по умолчанию
SERVICE="kafka-tests"
BUILD=false
CLEAN=false
SHOW_LOGS=true

# Парсинг аргументов
while [[ $# -gt 0 ]]; do
    case $1 in
        --smoke)
            SERVICE="kafka-tests-smoke"
            shift
            ;;
        --parallel)
            SERVICE="kafka-tests-parallel"
            shift
            ;;
        --build)
            BUILD=true
            shift
            ;;
        --clean)
            CLEAN=true
            shift
            ;;
        --no-logs)
            SHOW_LOGS=false
            shift
            ;;
        --report)
            echo -e "${BLUE}Запуск Allure Report Server...${NC}"
            docker-compose -f docker/docker-compose.yml up -d allure-report
            echo -e "${GREEN}Allure Report доступен по адресу: http://localhost:5050${NC}"
            echo -e "${YELLOW}Логин: admin, Пароль: admin${NC}"
            exit 0
            ;;
        --help)
            echo "Использование: ./docker/docker-run.sh [ОПЦИИ]"
            echo ""
            echo "Опции:"
            echo "  --smoke         Запустить только smoke тесты"
            echo "  --parallel      Запустить тесты параллельно (8 потоков)"
            echo "  --build         Пересобрать Docker образ"
            echo "  --clean         Очистить volumes и контейнеры"
            echo "  --no-logs       Не показывать логи"
            echo "  --report        Запустить Allure Report Server"
            echo "  --help          Показать эту справку"
            echo ""
            echo "Примеры:"
            echo "  ./docker/docker-run.sh                    # Запустить все тесты"
            echo "  ./docker/docker-run.sh --smoke            # Только smoke тесты"
            echo "  ./docker/docker-run.sh --parallel --build # Пересобрать и запустить параллельно"
            echo "  ./docker/docker-run.sh --report           # Открыть Allure отчет"
            exit 0
            ;;
        *)
            echo -e "${RED}Неизвестная опция: $1${NC}"
            echo "Используйте --help для справки"
            exit 1
            ;;
    esac
done

# Проверка переменных окружения
if [ -z "$KAFKA_SSL_TRUSTSTORE_PASSWORD" ]; then
    echo -e "${YELLOW}Внимание: KAFKA_SSL_TRUSTSTORE_PASSWORD не установлен${NC}"
    echo "Установите переменную окружения или создайте файл .env"
fi

# Проверка сертификатов
CERTS_PATH="${KAFKA_CERTS_PATH:-./kafka_key}"
if [ ! -d "$CERTS_PATH" ]; then
    echo -e "${RED}Ошибка: Директория с сертификатами не найдена: $CERTS_PATH${NC}"
    echo "Создайте директорию и поместите туда:"
    echo "  - kafka.truststore.jks"
    echo "  - kafka.keystore.p12"
    exit 1
fi

# Очистка
if [ "$CLEAN" = true ]; then
    echo -e "${YELLOW}Очистка контейнеров и volumes...${NC}"
    docker-compose -f docker/docker-compose.yml down -v
    docker system prune -f
    echo -e "${GREEN}Очистка завершена${NC}"
fi

# Сборка
if [ "$BUILD" = true ]; then
    echo -e "${BLUE}Сборка Docker образа...${NC}"
    docker-compose -f docker/docker-compose.yml build $SERVICE
    echo -e "${GREEN}Сборка завершена${NC}"
    echo ""
fi

# Запуск тестов
echo -e "${BLUE}Запуск сервиса: $SERVICE${NC}"
echo -e "${YELLOW}Это может занять некоторое время...${NC}"
echo ""

if [ "$SHOW_LOGS" = true ]; then
    docker-compose -f docker/docker-compose.yml run --rm $SERVICE
else
    docker-compose -f docker/docker-compose.yml run --rm $SERVICE > /dev/null 2>&1
fi

EXIT_CODE=$?

echo ""
if [ $EXIT_CODE -eq 0 ]; then
    echo -e "${GREEN}=====================================${NC}"
    echo -e "${GREEN}Тесты выполнены успешно!${NC}"
    echo -e "${GREEN}=====================================${NC}"
    echo ""
    echo -e "${YELLOW}Результаты:${NC}"
    echo "  Логи: ./target/logs/"
    echo "  Allure: ./target/allure-results/"
    echo ""
    echo -e "${BLUE}Для просмотра отчета запустите:${NC}"
    echo "  ./docker/docker-run.sh --report"
else
    echo -e "${RED}=====================================${NC}"
    echo -e "${RED}Тесты завершились с ошибкой!${NC}"
    echo -e "${RED}=====================================${NC}"
    echo ""
    echo -e "${YELLOW}Проверьте логи в ./target/logs/${NC}"
fi

exit $EXIT_CODE
