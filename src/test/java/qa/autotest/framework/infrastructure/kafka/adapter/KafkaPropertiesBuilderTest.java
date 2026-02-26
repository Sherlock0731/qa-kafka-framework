package qa.autotest.framework.infrastructure.kafka.adapter;

import org.apache.kafka.clients.CommonClientConfigs;
import org.junit.jupiter.api.*;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.ValueSource;
import qa.autotest.framework.config.KafkaConfig;
import qa.autotest.framework.infrastructure.KafkaPropertiesBuilder;

import java.util.Properties;

import static org.assertj.core.api.Assertions.*;
import static org.mockito.Mockito.*;

/**
 * Unit-тесты для KafkaPropertiesBuilder.
 * <p>
 * Нет I/O, нет Kafka-брокера — чистая логика трансформации конфига в Properties.
 * <p>
 * Покрываемые сценарии:
 * - buildBaseProperties: всегда содержит bootstrap.servers
 * - configureSecurity для SSL: добавляет все 7 SSL-ключей
 * - configureSecurity для SASL_SSL: аналогично SSL
 * - configureSecurity для PLAINTEXT: не добавляет ни одного SSL-ключа
 * - configureSecurity для SASL_PLAINTEXT: не добавляет ни одного SSL-ключа
 * - ssl.key.password: не добавляется если null
 * - isSecureProtocol: true для SSL/SASL_SSL, false для остальных
 * - SSL-ключи не перезаписывают уже существующие значения bootstrap.servers
 */
@Tag("unit")
@DisplayName("KafkaPropertiesBuilder")
class KafkaPropertiesBuilderTest {

    // ── SSL fixture values ─────────────────────────────────────────────────
    private static final String BOOTSTRAP = "kafka.example.com:9092";
    private static final String TRUSTSTORE_PATH = "/etc/kafka/truststore.jks";
    private static final String TRUSTSTORE_PASS = "trust-secret";
    private static final String TRUSTSTORE_TYPE = "JKS";
    private static final String KEYSTORE_PATH = "/etc/kafka/keystore.p12";
    private static final String KEYSTORE_PASS = "key-secret";
    private static final String KEYSTORE_TYPE = "PKCS12";
    private static final String KEY_PASS = "key-secret";

    // ── helpers ────────────────────────────────────────────────────────────

    private KafkaConfig plaintextConfig() {
        KafkaConfig cfg = mock(KafkaConfig.class);
        when(cfg.kafkaBootstrapServers()).thenReturn(BOOTSTRAP);
        when(cfg.securityProtocol()).thenReturn("PLAINTEXT");
        return cfg;
    }

    private KafkaConfig sslConfig(String protocol) {
        KafkaConfig cfg = mock(KafkaConfig.class);
        when(cfg.kafkaBootstrapServers()).thenReturn(BOOTSTRAP);
        when(cfg.securityProtocol()).thenReturn(protocol);
        when(cfg.sslTruststoreLocation()).thenReturn(TRUSTSTORE_PATH);
        when(cfg.sslTruststorePassword()).thenReturn(TRUSTSTORE_PASS);
        when(cfg.sslTruststoreType()).thenReturn(TRUSTSTORE_TYPE);
        when(cfg.sslKeystoreLocation()).thenReturn(KEYSTORE_PATH);
        when(cfg.sslKeystorePassword()).thenReturn(KEYSTORE_PASS);
        when(cfg.sslKeystoreType()).thenReturn(KEYSTORE_TYPE);
        when(cfg.sslKeyPassword()).thenReturn(KEY_PASS);
        return cfg;
    }

    // ═══════════════════════════════════════════════════════════════════════
    // buildBaseProperties
    // ═══════════════════════════════════════════════════════════════════════

    @Test
    @DisplayName("buildBaseProperties содержит bootstrap.servers")
    void shouldSetBootstrapServersInBaseProperties() {
        Properties props = KafkaPropertiesBuilder.buildBaseProperties(plaintextConfig());

        assertThat(props.getProperty(CommonClientConfigs.BOOTSTRAP_SERVERS_CONFIG))
                .isEqualTo(BOOTSTRAP);
    }

    @Test
    @DisplayName("buildBaseProperties возвращает новый экземпляр Properties (не изменяет чужой объект)")
    void shouldReturnNewPropertiesInstance() {
        KafkaConfig cfg = plaintextConfig();

        Properties p1 = KafkaPropertiesBuilder.buildBaseProperties(cfg);
        Properties p2 = KafkaPropertiesBuilder.buildBaseProperties(cfg);

        assertThat(p1).isNotSameAs(p2);
    }

    // ═══════════════════════════════════════════════════════════════════════
    // configureSecurity — SSL / SASL_SSL
    // ═══════════════════════════════════════════════════════════════════════

    @ParameterizedTest(name = "protocol={0}")
    @ValueSource(strings = {"SSL", "SASL_SSL"})
    @DisplayName("configureSecurity добавляет все SSL-ключи для SSL/SASL_SSL протоколов")
    void shouldAddAllSslPropertiesForSecureProtocols(String protocol) {
        Properties props = new Properties();

        KafkaPropertiesBuilder.configureSecurity(props, sslConfig(protocol));

        assertThat(props).containsKeys(
                CommonClientConfigs.SECURITY_PROTOCOL_CONFIG,
                "ssl.truststore.location",
                "ssl.truststore.password",
                "ssl.truststore.type",
                "ssl.keystore.location",
                "ssl.keystore.password",
                "ssl.keystore.type",
                "ssl.key.password"
        );
    }

    @ParameterizedTest(name = "protocol={0}")
    @ValueSource(strings = {"SSL", "SASL_SSL"})
    @DisplayName("configureSecurity выставляет корректные значения SSL-свойств")
    void shouldSetCorrectSslPropertyValues(String protocol) {
        Properties props = new Properties();

        KafkaPropertiesBuilder.configureSecurity(props, sslConfig(protocol));

        assertThat(props.getProperty("ssl.truststore.location")).isEqualTo(TRUSTSTORE_PATH);
        assertThat(props.getProperty("ssl.truststore.password")).isEqualTo(TRUSTSTORE_PASS);
        assertThat(props.getProperty("ssl.truststore.type")).isEqualTo(TRUSTSTORE_TYPE);
        assertThat(props.getProperty("ssl.keystore.location")).isEqualTo(KEYSTORE_PATH);
        assertThat(props.getProperty("ssl.keystore.password")).isEqualTo(KEYSTORE_PASS);
        assertThat(props.getProperty("ssl.keystore.type")).isEqualTo(KEYSTORE_TYPE);
        assertThat(props.getProperty("ssl.key.password")).isEqualTo(KEY_PASS);
    }

    @ParameterizedTest(name = "protocol={0}")
    @ValueSource(strings = {"SSL", "SASL_SSL"})
    @DisplayName("configureSecurity выставляет security.protocol из конфига")
    void shouldSetSecurityProtocolFromConfig(String protocol) {
        Properties props = new Properties();

        KafkaPropertiesBuilder.configureSecurity(props, sslConfig(protocol));

        assertThat(props.getProperty(CommonClientConfigs.SECURITY_PROTOCOL_CONFIG))
                .isEqualTo(protocol);
    }

    // ═══════════════════════════════════════════════════════════════════════
    // configureSecurity — PLAINTEXT / SASL_PLAINTEXT (нет SSL)
    // ═══════════════════════════════════════════════════════════════════════

    @ParameterizedTest(name = "protocol={0}")
    @ValueSource(strings = {"PLAINTEXT", "SASL_PLAINTEXT"})
    @DisplayName("configureSecurity не добавляет SSL-ключи для незащищённых протоколов")
    void shouldNotAddSslPropertiesForNonSecureProtocols(String protocol) {
        KafkaConfig cfg = mock(KafkaConfig.class);
        when(cfg.securityProtocol()).thenReturn(protocol);
        Properties props = new Properties();

        KafkaPropertiesBuilder.configureSecurity(props, cfg);

        assertThat(props).doesNotContainKey("ssl.truststore.location");
        assertThat(props).doesNotContainKey("ssl.keystore.location");
        assertThat(props).doesNotContainKey(CommonClientConfigs.SECURITY_PROTOCOL_CONFIG);
        // SSL-методы конфига не должны вызываться
        verify(cfg, never()).sslTruststoreLocation();
        verify(cfg, never()).sslKeystoreLocation();
    }

    @Test
    @DisplayName("configureSecurity не изменяет уже существующие свойства в Properties (bootstrap.servers)")
    void shouldNotOverwriteExistingProperties() {
        Properties props = KafkaPropertiesBuilder.buildBaseProperties(sslConfig("SSL"));
        String originalBootstrap = props.getProperty(CommonClientConfigs.BOOTSTRAP_SERVERS_CONFIG);

        KafkaPropertiesBuilder.configureSecurity(props, sslConfig("SSL"));

        assertThat(props.getProperty(CommonClientConfigs.BOOTSTRAP_SERVERS_CONFIG))
                .isEqualTo(originalBootstrap);
    }

    // ═══════════════════════════════════════════════════════════════════════
    // ssl.key.password — null handling
    // ═══════════════════════════════════════════════════════════════════════

    @Test
    @DisplayName("ssl.key.password не добавляется если возвращает null")
    void shouldNotAddKeyPasswordWhenNull() {
        KafkaConfig cfg = sslConfig("SSL");
        when(cfg.sslKeyPassword()).thenReturn(null); // override: null

        Properties props = new Properties();
        KafkaPropertiesBuilder.configureSecurity(props, cfg);

        assertThat(props).doesNotContainKey("ssl.key.password");
    }

    @Test
    @DisplayName("ssl.key.password добавляется если задан")
    void shouldAddKeyPasswordWhenPresent() {
        Properties props = new Properties();
        KafkaPropertiesBuilder.configureSecurity(props, sslConfig("SSL"));

        assertThat(props.getProperty("ssl.key.password")).isEqualTo(KEY_PASS);
    }

    // ═══════════════════════════════════════════════════════════════════════
    // isSecureProtocol
    // ═══════════════════════════════════════════════════════════════════════

    @ParameterizedTest(name = "protocol={0}")
    @ValueSource(strings = {"SSL", "SASL_SSL"})
    @DisplayName("isSecureProtocol возвращает true для SSL и SASL_SSL")
    void shouldReturnTrueForSecureProtocols(String protocol) {
        KafkaConfig cfg = mock(KafkaConfig.class);
        when(cfg.securityProtocol()).thenReturn(protocol);

        assertThat(KafkaPropertiesBuilder.isSecureProtocol(cfg)).isTrue();
    }

    @ParameterizedTest(name = "protocol={0}")
    @ValueSource(strings = {"PLAINTEXT", "SASL_PLAINTEXT", "plaintext", "ssl"})
    @DisplayName("isSecureProtocol возвращает false для незащищённых протоколов и некорректных строк")
    void shouldReturnFalseForNonSecureProtocols(String protocol) {
        KafkaConfig cfg = mock(KafkaConfig.class);
        when(cfg.securityProtocol()).thenReturn(protocol);

        assertThat(KafkaPropertiesBuilder.isSecureProtocol(cfg)).isFalse();
    }
}
