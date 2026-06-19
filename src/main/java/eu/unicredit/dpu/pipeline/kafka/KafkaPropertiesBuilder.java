package eu.unicredit.dpu.pipeline.kafka;

import eu.unicredit.dpu.pipeline.config.EnvProvider;
import eu.unicredit.dpu.pipeline.config.EnvironmentConfig;
import eu.unicredit.dpu.pipeline.config.KafkaConfig;
import eu.unicredit.dpu.pipeline.config.SchemaRegistryConfig;
import io.confluent.kafka.serializers.AbstractKafkaSchemaSerDeConfig;
import io.confluent.kafka.serializers.KafkaAvroDeserializerConfig;
import org.apache.kafka.clients.CommonClientConfigs;
import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.common.config.SaslConfigs;
import org.apache.kafka.common.config.SslConfigs;
import org.apache.kafka.common.serialization.StringDeserializer;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Properties;

/**
 * Builds the {@link Properties} objects required by:
 * <ul>
 *   <li>the Kafka consumer (used by the Flink KafkaSource)</li>
 *   <li>the Confluent Schema Registry client</li>
 * </ul>
 *
 * <p>Credentials are never stored in the YAML — they are read at runtime
 * from environment variables:
 * <ul>
 *   <li>{@code KAFKA_USERNAME} — SASL username</li>
 *   <li>{@code KAFKA_PASSWORD} — SASL password</li>
 *   <li>{@code SCHEMA_REGISTRY_USERNAME} — Schema Registry basic-auth user (optional)</li>
 *   <li>{@code SCHEMA_REGISTRY_PASSWORD} — Schema Registry basic-auth password (optional)</li>
 * </ul>
 */
public class KafkaPropertiesBuilder {

    private static final Logger LOG = LoggerFactory.getLogger(KafkaPropertiesBuilder.class);

    // ── Environment variable names ────────────────────────────────
    public static final String ENV_KAFKA_USERNAME = "KAFKA_USERNAME";
    public static final String ENV_KAFKA_PASSWORD = "KAFKA_PASSWORD";
    public static final String ENV_SR_USERNAME    = "SCHEMA_REGISTRY_USERNAME";
    public static final String ENV_SR_PASSWORD    = "SCHEMA_REGISTRY_PASSWORD";

    private final EnvironmentConfig envConfig;
    private final EnvProvider envProvider;

    public KafkaPropertiesBuilder(EnvironmentConfig envConfig) {
        this(envConfig, EnvProvider.system());
    }

    public KafkaPropertiesBuilder(EnvironmentConfig envConfig, EnvProvider envProvider) {
        this.envConfig   = envConfig;
        this.envProvider = envProvider;
    }

    // ── Public API ────────────────────────────────────────────────

    /**
     * Builds the full set of consumer properties, including SASL/SSL
     * authentication and Confluent Schema Registry settings.
     */
    public Properties buildConsumerProperties() {
        KafkaConfig kafka = envConfig.getKafka();
        SchemaRegistryConfig sr = envConfig.getSchemaRegistry();

        Properties props = new Properties();

        // ── Core consumer settings ────────────────────────────────
        props.setProperty(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG,    kafka.getBootstrapServers());
        props.setProperty(ConsumerConfig.GROUP_ID_CONFIG,             kafka.getConsumerGroupId());
        props.setProperty(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG,    kafka.getAutoOffsetReset());
        props.setProperty(ConsumerConfig.SESSION_TIMEOUT_MS_CONFIG,   String.valueOf(kafka.getSessionTimeoutMs()));
        props.setProperty(ConsumerConfig.HEARTBEAT_INTERVAL_MS_CONFIG,String.valueOf(kafka.getHeartbeatIntervalMs()));
        props.setProperty(ConsumerConfig.MAX_POLL_RECORDS_CONFIG,     String.valueOf(kafka.getMaxPollRecords()));

        // Disable auto-commit — Flink manages offsets via checkpoints
        props.setProperty(ConsumerConfig.ENABLE_AUTO_COMMIT_CONFIG, "false");

        // Deserializers — key: String, value: Avro (via Schema Registry)
        props.setProperty(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG,
                StringDeserializer.class.getName());
        props.setProperty(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG,
                io.confluent.kafka.serializers.KafkaAvroDeserializer.class.getName());

        // ── Schema Registry ───────────────────────────────────────
        props.setProperty(AbstractKafkaSchemaSerDeConfig.SCHEMA_REGISTRY_URL_CONFIG, sr.getUrl());
        props.setProperty(KafkaAvroDeserializerConfig.SPECIFIC_AVRO_READER_CONFIG, "true");

        // ── Security (SASL/SSL) ───────────────────────────────────
        props.setProperty(CommonClientConfigs.SECURITY_PROTOCOL_CONFIG, kafka.getSecurityProtocol());
        props.setProperty(SaslConfigs.SASL_MECHANISM, kafka.getSaslMechanism());

        // Build JAAS config from env vars — never hardcode credentials
        String kafkaUsername = requireEnv(ENV_KAFKA_USERNAME);
        String kafkaPassword = requireEnv(ENV_KAFKA_PASSWORD);
        props.setProperty(SaslConfigs.SASL_JAAS_CONFIG, buildJaasConfig(
                kafka.getSaslMechanism(), kafkaUsername, kafkaPassword));

        // ── Schema Registry basic auth (optional) ─────────────────
        String srUser = System.getenv(ENV_SR_USERNAME);
        String srPass = System.getenv(ENV_SR_PASSWORD);
        if (srUser != null && !srUser.isBlank()) {
            props.setProperty(AbstractKafkaSchemaSerDeConfig.BASIC_AUTH_CREDENTIALS_SOURCE, "USER_INFO");
            props.setProperty(AbstractKafkaSchemaSerDeConfig.USER_INFO_CONFIG, srUser + ":" + srPass);
            LOG.debug("Schema Registry basic auth configured for user '{}'.", srUser);
        }

        LOG.info("Kafka consumer properties built — brokers: {}, topic: {}, group: {}",
                kafka.getBootstrapServers(), kafka.getTopic(), kafka.getConsumerGroupId());
        return props;
    }

    /**
     * Builds a minimal set of Schema Registry client properties,
     * useful for direct schema introspection or health checks.
     */
    public Properties buildSchemaRegistryProperties() {
        SchemaRegistryConfig sr = envConfig.getSchemaRegistry();
        Properties props = new Properties();
        props.setProperty(AbstractKafkaSchemaSerDeConfig.SCHEMA_REGISTRY_URL_CONFIG, sr.getUrl());

        String srUser = System.getenv(ENV_SR_USERNAME);
        String srPass = System.getenv(ENV_SR_PASSWORD);
        if (srUser != null && !srUser.isBlank()) {
            props.setProperty(AbstractKafkaSchemaSerDeConfig.BASIC_AUTH_CREDENTIALS_SOURCE, "USER_INFO");
            props.setProperty(AbstractKafkaSchemaSerDeConfig.USER_INFO_CONFIG, srUser + ":" + srPass);
        }
        return props;
    }

    // ── Private helpers ───────────────────────────────────────────

    private String buildJaasConfig(String mechanism, String username, String password) {
        if ("SCRAM-SHA-512".equals(mechanism) || "SCRAM-SHA-256".equals(mechanism)) {
            return String.format(
                    "org.apache.kafka.common.security.scram.ScramLoginModule required " +
                    "username=\"%s\" password=\"%s\";",
                    username, password);
        }
        if ("PLAIN".equals(mechanism)) {
            return String.format(
                    "org.apache.kafka.common.security.plain.PlainLoginModule required " +
                    "username=\"%s\" password=\"%s\";",
                    username, password);
        }
        throw new IllegalArgumentException("Unsupported SASL mechanism: " + mechanism);
    }

    private String requireEnv(String name) {
        String value = System.getenv(name);
        if (value == null || value.isBlank()) {
            throw new IllegalStateException(
                    "Required environment variable '" + name + "' is not set. " +
                    "Set it before starting the pipeline.");
        }
        return value;
    }
}
