package qa.autotest.framework.utils;

import org.junit.jupiter.api.*;

import static org.assertj.core.api.Assertions.*;

/**
 * Unit-тесты для RetryContext.
 * <p>
 * Покрываемые сценарии:
 * - конструктор: attempt и backoffMs сохраняются корректно
 * - markSuccess(): success=true, endTime выставляется
 * - markFailure(error): success=false, lastError сохраняется
 * - getDuration(): до mark* — время от startTime до now; после mark* — фиксированный интервал
 * - начальное состояние: success=false, lastError=null
 */
@Tag("unit")
@DisplayName("RetryContext")
class RetryContextTest {

    @Test
    @DisplayName("Конструктор сохраняет attempt и backoffMs")
    void shouldStoreAttemptAndBackoff() {
        RetryContext ctx = new RetryContext(3, 500L);

        assertThat(ctx.getAttempt()).isEqualTo(3);
        assertThat(ctx.getBackoffMs()).isEqualTo(500L);
    }

    @Test
    @DisplayName("Начальное состояние: success=false, lastError=null")
    void shouldHaveCorrectInitialState() {
        RetryContext ctx = new RetryContext(1, 100L);

        assertThat(ctx.isSuccess()).isFalse();
        assertThat(ctx.getLastError()).isNull();
    }

    @Test
    @DisplayName("markSuccess() выставляет success=true")
    void shouldSetSuccessOnMarkSuccess() {
        RetryContext ctx = new RetryContext(1, 0L);

        ctx.markSuccess();

        assertThat(ctx.isSuccess()).isTrue();
        assertThat(ctx.getLastError()).isNull();
    }

    @Test
    @DisplayName("markFailure(error) выставляет success=false и сохраняет ошибку")
    void shouldSetFailureAndStoreError() {
        RetryContext ctx = new RetryContext(2, 200L);
        RuntimeException error = new RuntimeException("broker down");

        ctx.markFailure(error);

        assertThat(ctx.isSuccess()).isFalse();
        assertThat(ctx.getLastError()).isSameAs(error);
    }

    @Test
    @DisplayName("getDuration() после markSuccess() возвращает неотрицательное значение")
    void shouldReturnNonNegativeDurationAfterMarkSuccess() {
        RetryContext ctx = new RetryContext(1, 0L);
        ctx.markSuccess();

        assertThat(ctx.getDuration()).isGreaterThanOrEqualTo(0L);
    }

    @Test
    @DisplayName("getDuration() после markFailure() возвращает неотрицательное значение")
    void shouldReturnNonNegativeDurationAfterMarkFailure() {
        RetryContext ctx = new RetryContext(1, 0L);
        ctx.markFailure(new RuntimeException("err"));

        assertThat(ctx.getDuration()).isGreaterThanOrEqualTo(0L);
    }

    @Test
    @DisplayName("getDuration() до mark* возвращает время от создания (не 0)")
    void shouldReturnElapsedTimeBeforeMark() throws InterruptedException {
        RetryContext ctx = new RetryContext(1, 0L);
        Thread.sleep(10); // дать пройти хоть чуть-чуть

        assertThat(ctx.getDuration()).isGreaterThan(0L);
    }

    @Test
    @DisplayName("markSuccess() фиксирует endTime: повторные вызовы getDuration() возвращают одинаковое значение")
    void shouldReturnStableDurationAfterMarkSuccess() {
        RetryContext ctx = new RetryContext(1, 0L);
        ctx.markSuccess();

        long d1 = ctx.getDuration();
        long d2 = ctx.getDuration();

        // После markSuccess() endTime зафиксирован — duration не растёт
        assertThat(d1).isEqualTo(d2);
    }

    @Test
    @DisplayName("Нулевой attempt допустим (первая попытка)")
    void shouldAllowZeroAttempt() {
        RetryContext ctx = new RetryContext(0, 0L);

        assertThat(ctx.getAttempt()).isZero();
        assertThatNoException().isThrownBy(ctx::markSuccess);
    }

    @Test
    @DisplayName("markFailure принимает null как error (например, InterruptedException без сообщения)")
    void shouldHandleNullError() {
        RetryContext ctx = new RetryContext(1, 0L);

        ctx.markFailure(null);

        assertThat(ctx.isSuccess()).isFalse();
        assertThat(ctx.getLastError()).isNull();
    }
}
