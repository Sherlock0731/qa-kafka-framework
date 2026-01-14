# Test Listeners

Эта директория содержит JUnit 5 Extension listeners для расширенной функциональности тестирования.

## Listeners

### 1. AllureKafkaListener

**Назначение:** Интеграция с Allure Report для автоматического захвата деталей выполнения тестов.

**Реализует:** `TestWatcher`

**Функции:**
- Логирование успешного выполнения тестов
- Захват и прикрепление деталей ошибок
- Прикрепление информации о тесте (класс, метод, теги, поток)
- Форматированный вывод в консоль с эмодзи

**Автоматически прикрепляет к Allure:**
- Информацию о тесте (класс, метод, время, поток)
- Stack trace при ошибках
- Цепочку исключений (причины)

**Пример лога:**
```
✓ Test PASSED: ProducerTests.testSendSingleMessage
✗ Test FAILED: ConsumerTests.testReadFromBeginning
⊘ Test ABORTED: OrderingTests.testOrderingWithinPartition
⊗ Test DISABLED: DlqTests.testDlqWithRetries
```

### 2. KafkaTestExecutionListener

**Назначение:** Отслеживание времени выполнения тестов и логирование Kafka-специфичных операций.

**Реализует:** `BeforeEachCallback`, `AfterEachCallback`

**Функции:**
- Измерение времени выполнения каждого теста
- Логирование начала и завершения теста
- Прикрепление времени выполнения к Allure
- Отображение имени потока для отладки параллельного выполнения

**Пример лога:**
```
▶ Starting test: ProducerTests.testSendSingleMessage
Thread: ForkJoinPool-1-worker-1
◼ Finished test: ProducerTests.testSendSingleMessage in 2345 ms
```

## Использование

Listeners автоматически применяются ко всем тестам через аннотацию `@ExtendWith` в `BaseTest`:

```java
@ExtendWith({AllureKafkaListener.class, KafkaTestExecutionListener.class})
public abstract class BaseTest {
    // ...
}
```

Все тестовые классы, расширяющие `BaseTest`, автоматически получают эту функциональность.

## Примеры вложений в Allure

### Test Info (для всех тестов)
```
=== Test Execution Info ===

Test Class: tests.producer.ProducerTests
Test Method: testSendSingleMessage
Display Name: TC-001: Отправка одиночного сообщения в топик
Status: PASSED
Execution Time: 2026-01-13 11:15:23
Thread: ForkJoinPool-1-worker-1
Tag: producer
Tag: smoke
Tag: critical
```

### Execution Time (для всех тестов)
```
Test Execution Time: 2345 ms (2 seconds)
```

### Failure Details (только для упавших тестов)
```
=== Failure Details ===

Exception Type: org.opentest4j.AssertionFailedError
Message: expected: <10> but was: <5>

Stack Trace:
  at org.junit.jupiter.api.AssertionUtils.fail(AssertionUtils.java:55)
  at tests.consumer.ConsumerTests.testReadFromBeginning(ConsumerTests.java:45)
  ...

Caused by: java.lang.IllegalStateException
Message: Consumer not initialized
```

## Добавление собственных listeners

Чтобы добавить свой listener:

1. Создайте класс в этом пакете, реализующий один из JUnit 5 Extension интерфейсов:
   - `TestWatcher` - для реагирования на результаты тестов
   - `BeforeEachCallback` / `AfterEachCallback` - для хуков до/после теста
   - `BeforeAllCallback` / `AfterAllCallback` - для хуков до/после класса
   - `ParameterResolver` - для внедрения зависимостей

2. Добавьте аннотацию `@ExtendWith` в `BaseTest`:
   ```java
   @ExtendWith({AllureKafkaListener.class, KafkaTestExecutionListener.class, YourNewListener.class})
   ```

3. Или примените только к конкретному тестовому классу:
   ```java
   @ExtendWith(YourNewListener.class)
   public class SpecificTests extends BaseTest {
       // ...
   }
   ```

## Примеры кастомизации

### Пример: Listener для логирования Kafka метрик

```java
@Slf4j
public class KafkaMetricsListener implements AfterEachCallback {
    
    @Override
    public void afterEach(ExtensionContext context) {
        // Получить метрики из producer/consumer
        Map<String, Object> metrics = collectMetrics();
        
        // Прикрепить к Allure
        String metricsJson = new ObjectMapper().writeValueAsString(metrics);
        Allure.addAttachment("Kafka Metrics", "application/json",
            new ByteArrayInputStream(metricsJson.getBytes()), "json");
    }
}
```

### Пример: Listener для скриншотов Kafka UI (если есть)

```java
public class KafkaUIScreenshotListener implements TestWatcher {
    
    @Override
    public void testFailed(ExtensionContext context, Throwable cause) {
        // Сделать скриншот Kafka UI
        byte[] screenshot = captureKafkaUI();
        
        Allure.addAttachment("Kafka UI", "image/png",
            new ByteArrayInputStream(screenshot), "png");
    }
}
```

## Отключение listeners

Если нужно отключить listeners для конкретного теста:

```java
// Этот тест не будет использовать listeners из BaseTest
@ExtendWith({}) // Пустой массив
public class MinimalTest {
    
    @Test
    void simpleTest() {
        // Тест без listeners
    }
}
```

## Логирование

Все listeners используют SLF4J с Logback для логирования. Уровень логирования можно настроить в `logback.xml`:

```xml
<!-- Подробное логирование listeners -->
<logger name="tests.listeners" level="DEBUG" />

<!-- Только важные сообщения -->
<logger name="tests.listeners" level="INFO" />
```

## Многопоточность

Оба listener потокобезопасны и корректно работают при параллельном выполнении тестов:
- Используют thread-local хранилище JUnit 5
- Логируют имя потока для отладки
- Не разделяют изменяемое состояние между тестами

---

**Версия:** 1.0.0  
**Последнее обновление:** 2026-01-13
