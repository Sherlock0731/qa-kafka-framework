package qa.autotest.framework.config;

import org.junit.jupiter.api.*;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.ValueSource;
import qa.autotest.framework.domain.exceptions.ConfigurationException;

import static org.assertj.core.api.Assertions.*;
import static org.mockito.Mockito.*;

/**
 * Unit-тесты для ConfigFactory.
 * <p>
 * Стратегия: тестируем validateRequiredProperties() напрямую через мок KafkaConfig.
 * Это позволяет полностью контролировать значения свойств без зависимости от
 * файлов конфигурации (Owner library читает реальные файлы через getConfig()).
 * <p>
 * Покрываемые сценарии:
 * - fail-fast при отсутствии kafka.bootstrap.servers
 * - накопление ВСЕХ отсутствующих properties в одном исключении
 * - условная SSL-валидация (только для SSL / SASL_SSL)
 * - Aiven all-or-nothing constraint
 * - парные зависимости REST API / Schema Registry
 * - повторный вызов getConfig() возвращает тот же singleton
 * - resetConfig() обнуляет singleton
 */
@DisplayName("ConfigFactory — fail-fast validation")
class ConfigFactoryTest {

    private static final String BOOTSTRAP = "localhost:9092";

    @AfterEach
    void resetSingleton() {
        ConfigFactory.resetConfig();
    }

    // ── Хелпер: мок с минимальным PLAINTEXT-набором ───────────────────────────
    private KafkaConfig plaintextMock() {
        KafkaConfig mock = mock(KafkaConfig.class);
        when(mock.kafkaBootstrapServers()).thenReturn(BOOTSTRAP);
        when(mock.securityProtocol()).thenReturn("PLAINTEXT");
        when(mock.aivenApiToken()).thenReturn(null);
        when(mock.aivenProjectName()).thenReturn(null);
        when(mock.aivenServiceName()).thenReturn(null);
        when(mock.kafkaRestApiUrl()).thenReturn(null);
        when(mock.kafkaRestApiPassword()).thenReturn(null);
        when(mock.schemaRegistryUrl()).thenReturn(null);
        when(mock.schemaRegistryPassword()).thenReturn(null);
        return mock;
    }

    // ═══════════════════════════════════════════════════════════════════════
    // kafka.bootstrap.servers — всегда обязателен
    // ═══════════════════════════════════════════════════════════════════════

    @Test
    @DisplayName("Бросает ConfigurationException при отсутствии bootstrap.servers")
    void shouldThrowWhenBootstrapServersMissing() {
        KafkaConfig mock = mock(KafkaConfig.class);
        when(mock.kafkaBootstrapServers()).thenReturn(null);
        when(mock.securityProtocol()).thenReturn("PLAINTEXT");
        when(mock.aivenApiToken()).thenReturn(null);
        when(mock.aivenProjectName()).thenReturn(null);
        when(mock.aivenServiceName()).thenReturn(null);
        when(mock.kafkaRestApiUrl()).thenReturn(null);
        when(mock.schemaRegistryUrl()).thenReturn(null);

        assertThatThrownBy(() -> ConfigFactory.validateRequiredProperties(mock))
                .isInstanceOf(ConfigurationException.class)
                .hasMessageContaining("kafka.bootstrap.servers");
    }

    @Test
    @DisplayName("Успешно проходит валидацию при минимальном PLAINTEXT-наборе")
    void shouldCreateConfigForPlaintext() {
        KafkaConfig mock = plaintextMock();

        assertThatNoException().isThrownBy(
                () -> ConfigFactory.validateRequiredProperties(mock));
    }

    // ═══════════════════════════════════════════════════════════════════════
    // SSL — условная валидация
    // ═══════════════════════════════════════════════════════════════════════

    @ParameterizedTest(name = "protocol={0}")
    @ValueSource(strings = {"SSL", "SASL_SSL"})
    @DisplayName("Бросает исключение при SSL-протоколе без SSL properties")
    void shouldThrowWhenSslPropertiesMissingForSecureProtocol(String protocol) {
        KafkaConfig mock = mock(KafkaConfig.class);
        when(mock.kafkaBootstrapServers()).thenReturn(BOOTSTRAP);
        when(mock.securityProtocol()).thenReturn(protocol);
        when(mock.sslTruststoreLocation()).thenReturn(null);
        when(mock.sslTruststorePassword()).thenReturn(null);
        when(mock.sslKeystoreLocation()).thenReturn(null);
        when(mock.sslKeystorePassword()).thenReturn(null);
        when(mock.sslKeyPassword()).thenReturn(null);
        when(mock.aivenApiToken()).thenReturn(null);
        when(mock.aivenProjectName()).thenReturn(null);
        when(mock.aivenServiceName()).thenReturn(null);
        when(mock.kafkaRestApiUrl()).thenReturn(null);
        when(mock.schemaRegistryUrl()).thenReturn(null);

        ConfigurationException ex = catchThrowableOfType(
                () -> ConfigFactory.validateRequiredProperties(mock),
                ConfigurationException.class);

        assertThat(ex).isNotNull();
        String msg = ex.getMessage();
        assertThat(msg).contains("kafka.ssl.truststore.location");
        assertThat(msg).contains("kafka.ssl.truststore.password");
        assertThat(msg).contains("kafka.ssl.keystore.location");
        assertThat(msg).contains("kafka.ssl.keystore.password");
        assertThat(msg).contains("kafka.ssl.key.password");
    }

    @Test
    @DisplayName("PLAINTEXT не требует SSL properties")
    void shouldNotRequireSslForPlaintext() {
        KafkaConfig mock = plaintextMock();
        // SSL properties намеренно не заданы (mock вернёт null)

        assertThatNoException().isThrownBy(
                () -> ConfigFactory.validateRequiredProperties(mock));
    }

    @Test
    @DisplayName("SSL с полным набором properties проходит валидацию")
    void shouldPassValidationWithFullSslConfig() {
        KafkaConfig mock = mock(KafkaConfig.class);
        when(mock.kafkaBootstrapServers()).thenReturn(BOOTSTRAP);
        when(mock.securityProtocol()).thenReturn("SSL");
        when(mock.sslTruststoreLocation()).thenReturn("/tmp/truststore.jks");
        when(mock.sslTruststorePassword()).thenReturn("trust-pass");
        when(mock.sslKeystoreLocation()).thenReturn("/tmp/keystore.p12");
        when(mock.sslKeystorePassword()).thenReturn("key-pass");
        when(mock.sslKeyPassword()).thenReturn("key-pass");
        when(mock.aivenApiToken()).thenReturn(null);
        when(mock.aivenProjectName()).thenReturn(null);
        when(mock.aivenServiceName()).thenReturn(null);
        when(mock.kafkaRestApiUrl()).thenReturn(null);
        when(mock.schemaRegistryUrl()).thenReturn(null);

        assertThatNoException().isThrownBy(
                () -> ConfigFactory.validateRequiredProperties(mock));
    }

    // ═══════════════════════════════════════════════════════════════════════
    // Aiven — all-or-nothing constraint
    // ═══════════════════════════════════════════════════════════════════════

    @Test
    @DisplayName("Бросает исключение при частичной Aiven-конфигурации (только token)")
    void shouldThrowWhenOnlyAivenTokenPresent() {
        KafkaConfig mock = plaintextMock();
        when(mock.aivenApiToken()).thenReturn("token-abc");
        when(mock.aivenProjectName()).thenReturn(null);
        when(mock.aivenServiceName()).thenReturn(null);

        ConfigurationException ex = catchThrowableOfType(
                () -> ConfigFactory.validateRequiredProperties(mock),
                ConfigurationException.class);

        assertThat(ex).isNotNull();
        assertThat(ex.getMessage())
                .contains("aiven.project.name")
                .contains("aiven.service.name");
    }

    @Test
    @DisplayName("Бросает исключение при частичной Aiven-конфигурации (только project + service)")
    void shouldThrowWhenAivenTokenMissing() {
        KafkaConfig mock = plaintextMock();
        when(mock.aivenApiToken()).thenReturn(null);
        when(mock.aivenProjectName()).thenReturn("my-project");
        when(mock.aivenServiceName()).thenReturn("my-kafka");

        ConfigurationException ex = catchThrowableOfType(
                () -> ConfigFactory.validateRequiredProperties(mock),
                ConfigurationException.class);

        assertThat(ex).isNotNull();
        assertThat(ex.getMessage()).contains("aiven.api.token");
    }

    @Test
    @DisplayName("Полная Aiven-конфигурация проходит валидацию")
    void shouldPassValidationWithFullAivenConfig() {
        KafkaConfig mock = plaintextMock();
        when(mock.aivenApiToken()).thenReturn("token-abc");
        when(mock.aivenProjectName()).thenReturn("my-project");
        when(mock.aivenServiceName()).thenReturn("my-kafka");

        assertThatNoException().isThrownBy(
                () -> ConfigFactory.validateRequiredProperties(mock));
    }

    @Test
    @DisplayName("Отсутствие всех Aiven properties допустимо")
    void shouldPassValidationWithoutAivenConfig() {
        KafkaConfig mock = plaintextMock();

        assertThatNoException().isThrownBy(
                () -> ConfigFactory.validateRequiredProperties(mock));
    }

    // ═══════════════════════════════════════════════════════════════════════
    // REST API / Schema Registry — парные зависимости
    // ═══════════════════════════════════════════════════════════════════════

    @Test
    @DisplayName("REST API URL без пароля бросает исключение")
    void shouldThrowWhenRestApiUrlSetButPasswordMissing() {
        KafkaConfig mock = plaintextMock();
        when(mock.kafkaRestApiUrl()).thenReturn("https://rest.kafka.example.com");
        when(mock.kafkaRestApiPassword()).thenReturn(null);

        ConfigurationException ex = catchThrowableOfType(
                () -> ConfigFactory.validateRequiredProperties(mock),
                ConfigurationException.class);

        assertThat(ex).isNotNull();
        assertThat(ex.getMessage()).contains("kafka.rest.api.password");
    }

    @Test
    @DisplayName("Schema Registry URL без пароля бросает исключение")
    void shouldThrowWhenSchemaRegistryUrlSetButPasswordMissing() {
        KafkaConfig mock = plaintextMock();
        when(mock.schemaRegistryUrl()).thenReturn("https://schema-registry.example.com");
        when(mock.schemaRegistryPassword()).thenReturn(null);

        ConfigurationException ex = catchThrowableOfType(
                () -> ConfigFactory.validateRequiredProperties(mock),
                ConfigurationException.class);

        assertThat(ex).isNotNull();
        assertThat(ex.getMessage()).contains("kafka.schema.registry.password");
    }

    @Test
    @DisplayName("REST API URL вместе с паролем проходит валидацию")
    void shouldPassWhenRestApiUrlAndPasswordPresent() {
        KafkaConfig mock = plaintextMock();
        when(mock.kafkaRestApiUrl()).thenReturn("https://rest.kafka.example.com");
        when(mock.kafkaRestApiPassword()).thenReturn("secret");

        assertThatNoException().isThrownBy(
                () -> ConfigFactory.validateRequiredProperties(mock));
    }

    // ═══════════════════════════════════════════════════════════════════════
    // Накопление ошибок — все missing properties в одном exception
    // ═══════════════════════════════════════════════════════════════════════

    @Test
    @DisplayName("Накапливает все отсутствующие SSL properties в одном исключении")
    void shouldAccumulateAllMissingSslProperties() {
        KafkaConfig mock = mock(KafkaConfig.class);
        when(mock.kafkaBootstrapServers()).thenReturn(BOOTSTRAP);
        when(mock.securityProtocol()).thenReturn("SSL");
        when(mock.sslTruststoreLocation()).thenReturn(null);
        when(mock.sslTruststorePassword()).thenReturn(null);
        when(mock.sslKeystoreLocation()).thenReturn(null);
        when(mock.sslKeystorePassword()).thenReturn(null);
        when(mock.sslKeyPassword()).thenReturn(null);
        when(mock.aivenApiToken()).thenReturn(null);
        when(mock.aivenProjectName()).thenReturn(null);
        when(mock.aivenServiceName()).thenReturn(null);
        when(mock.kafkaRestApiUrl()).thenReturn(null);
        when(mock.schemaRegistryUrl()).thenReturn(null);

        ConfigurationException ex = catchThrowableOfType(
                () -> ConfigFactory.validateRequiredProperties(mock),
                ConfigurationException.class);

        assertThat(ex).isNotNull();
        long mentionCount = java.util.stream.Stream.of(
                "kafka.ssl.truststore.location",
                "kafka.ssl.truststore.password",
                "kafka.ssl.keystore.location",
                "kafka.ssl.keystore.password",
                "kafka.ssl.key.password"
        ).filter(p -> ex.getMessage().contains(p)).count();

        assertThat(mentionCount)
                .as("Все 5 SSL properties должны быть в сообщении об ошибке")
                .isEqualTo(5);
    }

    // ═══════════════════════════════════════════════════════════════════════
    // Singleton behaviour — тестируем через getConfig() с реальным конфигом
    // ═══════════════════════════════════════════════════════════════════════

    @Test
    @DisplayName("Повторный вызов getConfig() возвращает тот же экземпляр")
    void shouldReturnSameInstanceOnSubsequentCalls() {
        KafkaConfig first = ConfigFactory.getConfig();
        KafkaConfig second = ConfigFactory.getConfig();

        assertThat(first).isSameAs(second);
    }

    @Test
    @DisplayName("После resetConfig() singleton обнуляется")
    void shouldCreateNewInstanceAfterReset() {
        KafkaConfig first = ConfigFactory.getConfig();

        ConfigFactory.resetConfig();
        KafkaConfig second = ConfigFactory.getConfig();

        assertThat(first).isNotSameAs(second);
    }
}
