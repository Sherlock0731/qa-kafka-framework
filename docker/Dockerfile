# Многоступенчатый Dockerfile для Kafka Test Framework
# Поддерживает Windows, Linux и macOS

# Этап 1: Сборка
FROM maven:3.9-eclipse-temurin-17 AS builder

# Установка рабочей директории
WORKDIR /build

# Копирование pom.xml для кеширования зависимостей
COPY pom.xml .

# Загрузка зависимостей (будет закешировано при повторных сборках)
RUN mvn dependency:go-offline -B

# Копирование исходного кода
COPY src ./src

# Сборка проекта (без запуска тестов)
RUN mvn clean package -DskipTests -B

# Этап 2: Runtime образ
FROM eclipse-temurin:17-jre-alpine

# Метаданные образа
LABEL maintainer="Vitaliy Popravka - QA Automation Engineer"
LABEL description="Kafka Test Automation Framework"
LABEL version="1.0.0"

# Установка необходимых утилит
RUN apk add --no-cache \
    bash \
    curl \
    ca-certificates \
    tzdata

# Создание пользователя для запуска тестов (не root)
RUN addgroup -g 1000 qauser && \
    adduser -D -u 1000 -G qauser qauser

# Установка рабочей директории
WORKDIR /app

# Копирование собранного проекта из builder
COPY --from=builder /build/target ./target
COPY --from=builder /build/pom.xml .
COPY --from=builder /build/src ./src

# Создание директорий для сертификатов и логов
RUN mkdir -p /app/kafka_key /app/target/logs && \
    chown -R qauser:qauser /app

# Копирование скриптов
COPY run-tests.sh /app/
RUN chmod +x /app/run-tests.sh && \
    chown qauser:qauser /app/run-tests.sh

# Переключение на пользователя qauser
USER qauser

# Volume для сертификатов Kafka
VOLUME ["/app/kafka_key"]

# Volume для логов и отчетов
VOLUME ["/app/target"]

# Переменные окружения по умолчанию
ENV KAFKA_SSL_TRUSTSTORE_LOCATION=/app/kafka_key/kafka.truststore.jks
ENV KAFKA_SSL_KEYSTORE_LOCATION=/app/kafka_key/kafka.keystore.p12
ENV MAVEN_OPTS="-Xmx1024m -Xms512m"
ENV JAVA_TOOL_OPTIONS="-Dfile.encoding=UTF-8"

# Healthcheck
HEALTHCHECK --interval=30s --timeout=10s --start-period=5s --retries=3 \
    CMD [ -f /app/pom.xml ] || exit 1

# Точка входа
ENTRYPOINT ["mvn"]

# Команда по умолчанию
CMD ["test"]
