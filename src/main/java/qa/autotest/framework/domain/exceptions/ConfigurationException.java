package qa.autotest.framework.domain.exceptions;

import qa.autotest.framework.config.ConfigFactory;
import qa.autotest.framework.config.KafkaConfig;

import java.util.Collections;
import java.util.List;

/**
 * Thrown by {@link ConfigFactory} when one or more required configuration
 * properties are absent or blank after the Owner-managed {@link KafkaConfig}
 * proxy has been created.
 * <p>
 * All missing properties are collected before throwing so that engineers see
 * the complete list of gaps in a single run, rather than fixing one property
 * at a time.
 *
 * <h3>Example output</h3>
 * <pre>
 * ConfigurationException: Framework startup aborted — 3 required property(ies) are missing:
 *   [1] kafka.bootstrap.servers
 *   [2] kafka.ssl.truststore.location
 *   [3] kafka.ssl.keystore.location
 *
 * How to supply these values (highest priority first):
 *   1. JVM system property : -Dkafka.bootstrap.servers=host:port
 *   2. Environment variable: KAFKA_BOOTSTRAP_SERVERS=host:port
 *   3. Environment file    : src/main/resources/config/{env}.properties
 *   4. Default file        : src/main/resources/config/default.properties
 * </pre>
 *
 * @see ConfigFactory#validateRequiredProperties(KafkaConfig)
 */
public class ConfigurationException extends RuntimeException {

    /**
     * Unmodifiable snapshot of every property key that was missing.
     */
    private final List<String> missingProperties;

    /**
     * @param missingProperties non-empty list of property keys that had no value
     */
    public ConfigurationException(List<String> missingProperties) {
        super(buildMessage(missingProperties));
        this.missingProperties = Collections.unmodifiableList(missingProperties);
    }

    /**
     * Returns an unmodifiable view of every property key that was absent.
     *
     * @return list of missing property keys, never {@code null}, never empty
     */
    public List<String> getMissingProperties() {
        return missingProperties;
    }

    private static String buildMessage(List<String> missing) {
        StringBuilder sb = new StringBuilder();
        sb.append("Framework startup aborted — ")
                .append(missing.size())
                .append(" required property(ies) are missing:\n");

        for (int i = 0; i < missing.size(); i++) {
            sb.append("  [").append(i + 1).append("] ").append(missing.get(i)).append("\n");
        }

        sb.append("\nHow to supply these values (highest priority first):\n")
                .append("  1. JVM system property : -D<key>=<value>\n")
                .append("  2. Environment variable: <KEY_WITH_DOTS_AS_UNDERSCORES>=<value>\n")
                .append("  3. Environment file    : src/main/resources/config/{env}.properties\n")
                .append("  4. Default file        : src/main/resources/config/default.properties");

        return sb.toString();
    }
}
