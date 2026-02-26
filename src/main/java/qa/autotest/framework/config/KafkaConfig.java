package qa.autotest.framework.config;

import org.aeonbits.owner.Config;
import qa.autotest.framework.domain.exceptions.ConfigurationException;

/**
 * Configuration interface for Kafka test environment properties
 * Uses Owner library for configuration management
 * <p>
 * Configuration priority (highest to lowest):
 * 1. System properties (-Dkey=value)
 * 2. Environment variables
 * 3. Environment-specific properties (local.properties, ci.properties, etc.)
 * 4. Default properties (default.properties)
 * <p>
 * All default values are defined in src/main/resources/config/default.properties
 */
@Config.LoadPolicy(Config.LoadType.MERGE)
@Config.Sources({
        "system:properties",
        "system:env",
        "classpath:config/${env}.properties",
        "classpath:config/default.properties"
})
public interface KafkaConfig extends Config {

    /**
     * Environment name (local, dev, staging, prod, ci)
     */
    @Key("env")
    @DefaultValue("local")
    String environment();

    // ==================== Kafka Connection Properties ====================

    /**
     * Kafka bootstrap servers — comma-separated list of {@code host:port} pairs.
     * <p>
     * <strong>REQUIRED.</strong> No default is provided. The framework cannot
     * establish any Kafka connection without this value.
     * {@link ConfigFactory} will throw {@link ConfigurationException} at
     * startup if this property is absent or blank.
     */
    @Key("kafka.bootstrap.servers")
    String kafkaBootstrapServers();

    /**
     * Security protocol (SSL, SASL_SSL, PLAINTEXT)
     */
    @Key("kafka.security.protocol")
    @DefaultValue("SSL")
    String securityProtocol();

    /**
     * SSL Truststore location — absolute path to the JKS truststore file.
     * <p>
     * <strong>REQUIRED when {@code kafka.security.protocol} is {@code SSL} or
     * {@code SASL_SSL}.</strong> Ignored for {@code PLAINTEXT}.
     * {@link ConfigFactory} validates this as part of the SSL property group.
     */
    @Key("kafka.ssl.truststore.location")
    String sslTruststoreLocation();

    /**
     * SSL Truststore password.
     * <p>
     * <strong>REQUIRED when {@code kafka.security.protocol} is {@code SSL} or
     * {@code SASL_SSL}.</strong> Ignored for {@code PLAINTEXT}.
     */
    @Key("kafka.ssl.truststore.password")
    String sslTruststorePassword();

    /**
     * SSL Keystore location — absolute path to the PKCS12 keystore file.
     * <p>
     * <strong>REQUIRED when {@code kafka.security.protocol} is {@code SSL} or
     * {@code SASL_SSL}.</strong> Ignored for {@code PLAINTEXT}.
     */
    @Key("kafka.ssl.keystore.location")
    String sslKeystoreLocation();

    /**
     * SSL Keystore password.
     * <p>
     * <strong>REQUIRED when {@code kafka.security.protocol} is {@code SSL} or
     * {@code SASL_SSL}.</strong> Ignored for {@code PLAINTEXT}.
     */
    @Key("kafka.ssl.keystore.password")
    String sslKeystorePassword();

    /**
     * SSL private key password (may equal the keystore password for Aiven-issued certs).
     * <p>
     * <strong>REQUIRED when {@code kafka.security.protocol} is {@code SSL} or
     * {@code SASL_SSL}.</strong> Ignored for {@code PLAINTEXT}.
     */
    @Key("kafka.ssl.key.password")
    String sslKeyPassword();

    /**
     * SSL Keystore type (JKS, PKCS12)
     */
    @Key("kafka.ssl.keystore.type")
    @DefaultValue("PKCS12")
    String sslKeystoreType();

    /**
     * SSL Truststore type (JKS, PKCS12)
     */
    @Key("kafka.ssl.truststore.type")
    @DefaultValue("JKS")
    String sslTruststoreType();

    // ==================== API Configuration ====================

    /**
     * Kafka REST API base URL.
     * <p>
     * <strong>OPTIONAL.</strong> When set, {@code kafka.rest.api.password} also
     * becomes required. Leave blank if the REST API is not used.
     */
    @Key("kafka.rest.api.url")
    String kafkaRestApiUrl();

    /**
     * Kafka REST API username.
     * <p>
     * <strong>OPTIONAL.</strong> May be blank if the REST API uses token-based
     * auth or is not configured.
     */
    @Key("kafka.rest.api.username")
    String kafkaRestApiUsername();

    /**
     * Kafka REST API password.
     * <p>
     * <strong>REQUIRED when {@code kafka.rest.api.url} is set.</strong>
     * {@link ConfigFactory} enforces this as a paired dependency.
     */
    @Key("kafka.rest.api.password")
    String kafkaRestApiPassword();

    /**
     * Schema Registry URL.
     * <p>
     * <strong>OPTIONAL.</strong> When set, {@code kafka.schema.registry.password}
     * also becomes required. Leave blank if Avro / Schema Registry is not used.
     */
    @Key("kafka.schema.registry.url")
    String schemaRegistryUrl();

    /**
     * Schema Registry username.
     * <p>
     * <strong>OPTIONAL.</strong> May be blank if not applicable.
     */
    @Key("kafka.schema.registry.username")
    String schemaRegistryUsername();

    /**
     * Schema Registry password.
     * <p>
     * <strong>REQUIRED when {@code kafka.schema.registry.url} is set.</strong>
     * {@link ConfigFactory} enforces this as a paired dependency.
     */
    @Key("kafka.schema.registry.password")
    String schemaRegistryPassword();

    // ==================== Producer Configuration ====================

    /**
     * Producer acknowledgment mode (all, 1, 0)
     */
    @Key("kafka.producer.acks")
    @DefaultValue("all")
    String producerAcks();

    /**
     * Producer retry attempts
     */
    @Key("kafka.producer.retries")
    @DefaultValue("3")
    Integer producerRetries();

    /**
     * Enable idempotent producer
     */
    @Key("kafka.producer.enable.idempotence")
    @DefaultValue("true")
    Boolean producerEnableIdempotence();

    /**
     * Max in-flight requests per connection
     */
    @Key("kafka.producer.max.in.flight.requests.per.connection")
    @DefaultValue("5")
    Integer producerMaxInFlightRequests();

    /**
     * Producer batch size
     */
    @Key("kafka.producer.batch.size")
    @DefaultValue("16384")
    Integer producerBatchSize();

    /**
     * Producer linger time (ms)
     */
    @Key("kafka.producer.linger.ms")
    @DefaultValue("10")
    Integer producerLingerMs();

    /**
     * Producer request timeout (ms)
     */
    @Key("kafka.producer.request.timeout.ms")
    @DefaultValue("30000")
    Integer producerRequestTimeoutMs();

    // ==================== Consumer Configuration ====================

    /**
     * Consumer group ID base (will be appended with unique ID per test)
     */
    @Key("kafka.consumer.group.id.base")
    @DefaultValue("qa-test-group")
    String consumerGroupIdBase();

    /**
     * Auto offset reset strategy (earliest, latest, none)
     */
    @Key("kafka.consumer.auto.offset.reset")
    @DefaultValue("earliest")
    String consumerAutoOffsetReset();

    /**
     * Enable auto commit
     */
    @Key("kafka.consumer.enable.auto.commit")
    @DefaultValue("false")
    Boolean consumerEnableAutoCommit();

    /**
     * Auto commit interval (ms)
     */
    @Key("kafka.consumer.auto.commit.interval.ms")
    @DefaultValue("5000")
    Integer consumerAutoCommitIntervalMs();

    /**
     * Session timeout (ms)
     */
    @Key("kafka.consumer.session.timeout.ms")
    @DefaultValue("30000")
    Integer consumerSessionTimeoutMs();

    /**
     * Max poll interval (ms)
     */
    @Key("kafka.consumer.max.poll.interval.ms")
    @DefaultValue("300000")
    Integer consumerMaxPollIntervalMs();

    /**
     * Max poll records
     */
    @Key("kafka.consumer.max.poll.records")
    @DefaultValue("500")
    Integer consumerMaxPollRecords();

    /**
     * Fetch min bytes
     */
    @Key("kafka.consumer.fetch.min.bytes")
    @DefaultValue("1")
    Integer consumerFetchMinBytes();

    /**
     * Fetch max wait (ms)
     */
    @Key("kafka.consumer.fetch.max.wait.ms")
    @DefaultValue("500")
    Integer consumerFetchMaxWaitMs();

    // ==================== Test Configuration ====================

    /**
     * Default topic prefix for test topics
     */
    @Key("kafka.test.topic.prefix")
    @DefaultValue("qa-test")
    String testTopicPrefix();

    /**
     * Default number of partitions for test topics
     */
    @Key("kafka.test.topic.partitions")
    @DefaultValue("3")
    Integer testTopicPartitions();

    /**
     * Default replication factor for test topics
     */
    @Key("kafka.test.topic.replication.factor")
    @DefaultValue("1")
    Short testTopicReplicationFactor();

    /**
     * DLQ topic suffix
     */
    @Key("kafka.test.dlq.suffix")
    @DefaultValue("-dlq")
    String dlqTopicSuffix();

    /**
     * Test timeout for async operations (seconds)
     */
    @Key("test.timeout.seconds")
    @DefaultValue("30")
    Integer testTimeoutSeconds();

    /**
     * Poll timeout for async operations (seconds)
     */
    @Key("test.poll.timeout.seconds")
    @DefaultValue("5")
    Integer testPollTimeoutSeconds();

    /**
     * Thread count for parallel execution
     */
    @Key("thread.count")
    @DefaultValue("1")
    Integer threadCount();

    /**
     * Enable detailed logging
     */
    @Key("logging.detailed")
    @DefaultValue("true")
    Boolean detailedLogging();

    /**
     * Clean up topics after tests
     */
    @Key("test.cleanup.topics")
    @DefaultValue("true")
    Boolean cleanupTopics();

    /**
     * Enable global test topic cleanup via Aiven REST API after the entire
     * test suite finishes (triggered by {@code GlobalCleanupListener}).
     * <p>
     * When {@code true} (default), {@code GlobalCleanupListener} will call
     * the Aiven API at the end of the test plan to delete all topics whose
     * names start with {@link #testTopicPrefix()}.  This is the "deep clean"
     * that removes topics left behind by tests that failed before
     * {@code @AfterEach} could run.
     * <p>
     * Set to {@code false} to disable the global Aiven API sweep entirely
     * (e.g. when running against a local Kafka cluster where Aiven API is
     * not available, or when you intentionally want to inspect topics after
     * the run).
     * <p>
     * This flag is independent of {@link #cleanupTopics()}: the per-test
     * {@code @AfterEach} cleanup obeys {@code test.cleanup.topics}, while
     * the end-of-suite Aiven API sweep obeys this flag.
     */
    @Key("test.cleanup.aiven.api.enabled")
    @DefaultValue("true")
    Boolean cleanupViaAivenApiEnabled();

    // ==================== Aiven API Configuration ====================

    /**
     * Aiven API base URL.
     * <p>
     * Has a sensible default; override only when using a private Aiven deployment.
     */
    @Key("aiven.api.url")
    @DefaultValue("https://api.aiven.io/v1")
    String aivenApiUrl();

    /**
     * Aiven API personal access token.
     * <p>
     * <strong>REQUIRED as a group with {@code aiven.project.name} and
     * {@code aiven.service.name}.</strong> If any one of the three Aiven
     * properties is set, all three must be present.
     * {@link ConfigFactory} enforces this all-or-nothing constraint.
     * Leave all three blank to disable Aiven API integration.
     */
    @Key("aiven.api.token")
    String aivenApiToken();

    /**
     * Aiven project name that owns the Kafka service.
     * <p>
     * <strong>REQUIRED as a group</strong> — see {@link #aivenApiToken()}.
     */
    @Key("aiven.project.name")
    String aivenProjectName();

    /**
     * Aiven Kafka service name within the project.
     * <p>
     * <strong>REQUIRED as a group</strong> — see {@link #aivenApiToken()}.
     */
    @Key("aiven.service.name")
    String aivenServiceName();
}
