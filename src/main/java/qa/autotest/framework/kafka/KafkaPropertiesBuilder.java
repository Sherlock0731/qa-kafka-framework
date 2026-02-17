package qa.autotest.framework.kafka;

import org.apache.kafka.clients.CommonClientConfigs;
import qa.autotest.framework.config.KafkaConfig;

import java.util.Properties;

public class KafkaPropertiesBuilder {

    /**
     * Private constructor to prevent instantiation of utility class.
     *
     * @throws UnsupportedOperationException always
     */
    private KafkaPropertiesBuilder() {
        throw new UnsupportedOperationException("Utility class");
    }

    /**
     * Builds base Kafka client properties common to both producers and consumers.
     *
     * <p>Currently includes only bootstrap servers configuration. Producer and
     * consumer-specific properties (serializers, deserializers, timeouts, etc.)
     * should be added by the caller.</p>
     *
     * <h3>Properties Set</h3>
     * <ul>
     *   <li>{@code bootstrap.servers} - Kafka broker addresses</li>
     * </ul>
     *
     * @param config Kafka configuration containing connection settings
     * @return new Properties object with base configuration
     * @throws NullPointerException if config is null
     */
    public static Properties buildBaseProperties(KafkaConfig config) {
        Properties props = new Properties();
        props.put(CommonClientConfigs.BOOTSTRAP_SERVERS_CONFIG,
                config.kafkaBootstrapServers());
        return props;
    }

    /**
     * Configures security properties (SSL/TLS) for Kafka client connection.
     *
     * <p>Applies SSL configuration when security protocol is "SSL" or "SASL_SSL".
     * For other protocols (PLAINTEXT, SASL_PLAINTEXT), no properties are added.</p>
     *
     * <h3>SSL Properties Configured</h3>
     * <table border="1">
     *   <tr>
     *     <th>Property</th>
     *     <th>Description</th>
     *     <th>Required</th>
     *   </tr>
     *   <tr>
     *     <td>security.protocol</td>
     *     <td>SSL or SASL_SSL</td>
     *     <td>Yes</td>
     *   </tr>
     *   <tr>
     *     <td>ssl.truststore.location</td>
     *     <td>Path to truststore file</td>
     *     <td>Yes</td>
     *   </tr>
     *   <tr>
     *     <td>ssl.truststore.password</td>
     *     <td>Truststore password</td>
     *     <td>Yes</td>
     *   </tr>
     *   <tr>
     *     <td>ssl.truststore.type</td>
     *     <td>Truststore format (JKS, PKCS12)</td>
     *     <td>Yes</td>
     *   </tr>
     *   <tr>
     *     <td>ssl.keystore.location</td>
     *     <td>Path to keystore file (client cert)</td>
     *     <td>Yes</td>
     *   </tr>
     *   <tr>
     *     <td>ssl.keystore.password</td>
     *     <td>Keystore password</td>
     *     <td>Yes</td>
     *   </tr>
     *   <tr>
     *     <td>ssl.keystore.type</td>
     *     <td>Keystore format (JKS, PKCS12)</td>
     *     <td>Yes</td>
     *   </tr>
     *   <tr>
     *     <td>ssl.key.password</td>
     *     <td>Private key password</td>
     *     <td>No (if differs from keystore password)</td>
     *   </tr>
     * </table>
     *
     * <p><b>Mutual TLS:</b> When keystore is configured, enables client authentication
     * (mutual TLS). Server validates client certificate in addition to client
     * validating server certificate.</p>
     *
     * <h3>Example: SSL Configuration</h3>
     * <pre>{@code
     * Properties props = new Properties();
     * props.put(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, StringSerializer.class);
     *
     * // Add SSL configuration
     * KafkaPropertiesBuilder.configureSecurity(props, config);
     *
     * // Result: props now contains all SSL settings if config has SSL enabled
     * }</pre>
     *
     * <h3>Example: PLAINTEXT (No SSL)</h3>
     * <pre>{@code
     * // If config.securityProtocol() = "PLAINTEXT"
     * Properties props = new Properties();
     * KafkaPropertiesBuilder.configureSecurity(props, config);
     * // Result: No SSL properties added, props unchanged
     * }</pre>
     *
     * @param props  existing Properties object to modify (must not be null)
     * @param config Kafka configuration containing SSL settings
     * @throws NullPointerException if props or config is null
     * @see #isSecureProtocol(KafkaConfig)
     */
    public static void configureSecurity(Properties props, KafkaConfig config) {
        if (!isSecureProtocol(config)) {
            return; // No SSL configuration needed
        }

        // Set security protocol
        props.put(CommonClientConfigs.SECURITY_PROTOCOL_CONFIG, config.securityProtocol());

        // Configure truststore (server certificate validation)
        props.put("ssl.truststore.location", config.sslTruststoreLocation());
        props.put("ssl.truststore.password", config.sslTruststorePassword());
        props.put("ssl.truststore.type", config.sslTruststoreType());

        // Configure keystore (client certificate for mutual TLS)
        props.put("ssl.keystore.location", config.sslKeystoreLocation());
        props.put("ssl.keystore.password", config.sslKeyPassword());
        props.put("ssl.keystore.type", config.sslKeystoreType());

        // Optional: separate password for private key (if different from keystore password)
        if (config.sslKeyPassword() != null) {
            props.put("ssl.key.password", config.sslKeyPassword());
        }
    }

    /**
     * Checks if the configured security protocol requires SSL/TLS configuration.
     *
     * <p>Returns true for protocols that use SSL encryption:</p>
     * <ul>
     *   <li><b>SSL</b> - SSL/TLS encryption only</li>
     *   <li><b>SASL_SSL</b> - SASL authentication over SSL/TLS</li>
     * </ul>
     *
     * <p>Returns false for protocols without encryption:</p>
     * <ul>
     *   <li><b>PLAINTEXT</b> - No encryption or authentication</li>
     *   <li><b>SASL_PLAINTEXT</b> - SASL authentication without encryption</li>
     * </ul>
     *
     * @param config Kafka configuration to check
     * @return true if SSL configuration is required, false otherwise
     * @throws NullPointerException if config is null
     */
    public static boolean isSecureProtocol(KafkaConfig config) {
        String protocol = config.securityProtocol();
        return "SSL".equals(protocol) || "SASL_SSL".equals(protocol);
    }
}
