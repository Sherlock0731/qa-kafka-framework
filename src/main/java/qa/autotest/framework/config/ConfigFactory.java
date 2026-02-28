package qa.autotest.framework.config;

import lombok.extern.slf4j.Slf4j;
import qa.autotest.framework.domain.exceptions.ConfigurationException;

import java.util.ArrayList;
import java.util.List;

/**
 * Factory class for creating and managing KafkaConfig instances.
 * Implements thread-safe singleton pattern.
 * <p>
 * After creating the Owner-managed {@link KafkaConfig} proxy, this factory
 * performs a <em>fail-fast</em> validation of every required property before
 * returning the instance to callers. The goal is to surface misconfiguration
 * at the earliest possible moment — during test suite initialization — rather
 * than letting a {@code null} propagate silently into Kafka client internals
 * where the resulting {@code NullPointerException} carries no context about
 * which property was missing.
 * <p>
 * <strong>Required vs optional properties</strong>
 * <ul>
 *   <li>{@code kafka.bootstrap.servers} — always required; no Kafka connection
 *       is possible without it.</li>
 *   <li>SSL properties — required only when
 *       {@code kafka.security.protocol} is {@code SSL} or {@code SASL_SSL}.
 *       They are silently ignored for {@code PLAINTEXT} environments.</li>
 *   <li>Aiven API properties — required only when the Aiven REST API is in
 *       active use (topic cleanup, admin operations). Validated together as a
 *       group: if any one is present the others must be too.</li>
 *   <li>REST API / Schema Registry credentials — optional; validated only
 *       when the respective URL is configured.</li>
 * </ul>
 */
@Slf4j
public class ConfigFactory {

    private static volatile KafkaConfig config;

    private ConfigFactory() {
        // Private constructor to prevent instantiation
    }

    /**
     * Returns the singleton {@link KafkaConfig} instance, creating and
     * validating it on first call.
     * <p>
     * Uses double-checked locking for thread-safe lazy initialization.
     *
     * @return validated {@link KafkaConfig} instance
     * @throws ConfigurationException if any required property is missing
     */
    public static KafkaConfig getConfig() {
        if (config == null) {
            synchronized (ConfigFactory.class) {
                if (config == null) {
                    String env = System.getProperty("env", System.getenv("ENV"));
                    if (env == null) {
                        env = "local";
                    }

                    System.setProperty("env", env);
                    log.info("Initializing configuration for environment: {}", env);

                    KafkaConfig candidate = org.aeonbits.owner.ConfigFactory.create(KafkaConfig.class);

                    validateRequiredProperties(candidate);

                    config = candidate;

                    log.info("Configuration initialized successfully for environment: {}", env);
                    log.debug("Security protocol: {}", config.securityProtocol());
                    log.debug("Thread count: {}", config.threadCount());
                }
            }
        }
        return config;
    }

    /**
     * Resets the singleton (useful for isolated unit tests that supply
     * their own configuration).
     */
    public static void resetConfig() {
        synchronized (ConfigFactory.class) {
            config = null;
            log.info("Configuration reset");
        }
    }

    /**
     * Performs fail-fast validation of all required properties.
     * <p>
     * Collects every missing value into a single list and throws one
     * {@link ConfigurationException} that names all absent properties at
     * once, so engineers fix the entire configuration in a single iteration
     * rather than discovering missing values one by one.
     *
     * @param cfg freshly created {@link KafkaConfig} proxy to validate
     * @throws ConfigurationException listing all missing required properties
     */
    static void validateRequiredProperties(KafkaConfig cfg) {
        List<String> missing = new ArrayList<>();

        requireNonBlank(cfg.kafkaBootstrapServers(), "kafka.bootstrap.servers", missing);

        String protocol = cfg.securityProtocol();
        if (isSslProtocol(protocol)) {
            requireNonBlank(cfg.sslTruststoreLocation(),  "kafka.ssl.truststore.location",  missing);
            requireNonBlank(cfg.sslTruststorePassword(),  "kafka.ssl.truststore.password",  missing);
            requireNonBlank(cfg.sslKeystoreLocation(),    "kafka.ssl.keystore.location",    missing);
            requireNonBlank(cfg.sslKeystorePassword(),    "kafka.ssl.keystore.password",    missing);
            requireNonBlank(cfg.sslKeyPassword(),         "kafka.ssl.key.password",         missing);
        }

        // If any Aiven property is set, all three must be present.
        boolean anyAiven = isPresent(cfg.aivenApiToken())
                || isPresent(cfg.aivenProjectName())
                || isPresent(cfg.aivenServiceName());
        if (anyAiven) {
            requireNonBlank(cfg.aivenApiToken(),      "aiven.api.token",      missing);
            requireNonBlank(cfg.aivenProjectName(),   "aiven.project.name",   missing);
            requireNonBlank(cfg.aivenServiceName(),   "aiven.service.name",   missing);
        }

        if (isPresent(cfg.kafkaRestApiUrl())) {
            requireNonBlank(cfg.kafkaRestApiPassword(), "kafka.rest.api.password", missing);
        }

        if (isPresent(cfg.schemaRegistryUrl())) {
            requireNonBlank(cfg.schemaRegistryPassword(), "kafka.schema.registry.password", missing);
        }

        if (!missing.isEmpty()) {
            throw new ConfigurationException(missing);
        }

        log.debug("Configuration validation passed. Protocol: {}, SSL: {}",
                protocol, isSslProtocol(protocol));
    }

    private static boolean isSslProtocol(String protocol) {
        return protocol != null
                && (protocol.equalsIgnoreCase("SSL")
                ||  protocol.equalsIgnoreCase("SASL_SSL"));
    }

    private static boolean isPresent(String value) {
        return value != null && !value.isBlank();
    }

    private static void requireNonBlank(String value, String propertyKey, List<String> missing) {
        if (!isPresent(value)) {
            missing.add(propertyKey);
        }
    }
}
