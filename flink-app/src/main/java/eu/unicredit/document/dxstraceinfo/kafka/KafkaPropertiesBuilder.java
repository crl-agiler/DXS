package eu.unicredit.document.dxstraceinfo.kafka;

import eu.unicredit.document.dxstraceinfo.config.ConfigApp;
import eu.unicredit.document.dxstraceinfo.config.ConfigKafka;
import eu.unicredit.document.dxstraceinfo.config.ConfigSchemaRegistry;
import eu.unicredit.document.dxstraceinfo.credentials.Credentials;
import io.confluent.kafka.serializers.AbstractKafkaSchemaSerDeConfig;
import io.confluent.kafka.serializers.KafkaAvroDeserializer;
import io.confluent.kafka.serializers.KafkaAvroDeserializerConfig;
import java.util.Properties;
import org.apache.kafka.clients.CommonClientConfigs;
import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.common.config.SslConfigs;
import org.apache.kafka.common.serialization.StringDeserializer;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class KafkaPropertiesBuilder {


  private static final Logger LOG = LoggerFactory.getLogger(KafkaPropertiesBuilder.class);

  private final ConfigApp appConfig;

  /**
   * Builds the {@link Properties} objects required by the pipeline.
   *
   * <ul>
   *   <li>the Kafka consumer (used by the Flink KafkaSource)</li>
   *   <li>the Confluent Schema Registry client</li>
   * </ul>
   *
   * <p>Credentials are never stored in the YAML. They are read at runtime
   * from environment variables:
   * <ul>
   *   <li>{@code KAFKA_USERNAME} — SASL username</li>
   *   <li>{@code KAFKA_PASSWORD} — SASL password</li>
   *   <li>{@code SCHEMA_REGISTRY_USERNAME} — Schema Registry basic-auth user (optional)</li>
   *   <li>{@code SCHEMA_REGISTRY_PASSWORD} — Schema Registry basic-auth password (optional)</li>
   * </ul>
   */

  public KafkaPropertiesBuilder(
      ConfigApp appConfig) {

    this.appConfig = appConfig;
  }

  // ── Public API ────────────────────────────────────────────────

  /**
   * Builds the full set of consumer properties, including SASL/SSL
   * authentication and Confluent Schema Registry settings.
   */
  public Properties buildConsumerProperties() {

    ConfigKafka kafka = appConfig.getKafkaConfig();

    ConfigSchemaRegistry sr =
        appConfig.getSchemaRegistryConfig();

    Properties props = new Properties();

    // ── Core consumer settings ────────────────────────────────

    if (kafka.getOtherKafkaProperties() != null) {
      kafka.getOtherKafkaProperties().forEach(props::setProperty);
    }

    props.setProperty(
        ConsumerConfig.ENABLE_AUTO_COMMIT_CONFIG,
        "false");

    props.setProperty(
        ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG,
        StringDeserializer.class.getName());

    props.setProperty(
        ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG,
        KafkaAvroDeserializer.class.getName());

    // ── Schema Registry ───────────────────────────────────────

    props.setProperty(
        AbstractKafkaSchemaSerDeConfig.SCHEMA_REGISTRY_URL_CONFIG,
        sr.getUrl());

    props.setProperty(
        KafkaAvroDeserializerConfig.SPECIFIC_AVRO_READER_CONFIG,
        "true");

    // ── Security (SASL/SSL) ───────────────────────────────────

    String securityProtocol =
        props.getProperty(
            CommonClientConfigs.SECURITY_PROTOCOL_CONFIG,
            "PLAINTEXT");

    if (kafka.isSecure()) {

      props.setProperty(
          CommonClientConfigs.SECURITY_PROTOCOL_CONFIG,
          "SSL");

      props.setProperty(
          SslConfigs.SSL_KEYSTORE_LOCATION_CONFIG,
          kafka.getCertDir() + "/keystore.jks");

      props.setProperty(
          SslConfigs.SSL_KEYSTORE_PASSWORD_CONFIG,
          "changeit");

      props.setProperty(
          SslConfigs.SSL_TRUSTSTORE_LOCATION_CONFIG,
          kafka.getCertDir() + "/truststore.jks");

      props.setProperty(
          SslConfigs.SSL_TRUSTSTORE_PASSWORD_CONFIG,
          "changeit");

      LOG.info(
          "Kafka SSL enabled using certificates from {}",
          kafka.getCertDir());

    } else {

      LOG.info(
          "Kafka SSL disabled (PLAINTEXT mode)");

    }

    LOG.info(
        "Kafka consumer properties built — brokers: {}, topic: {}, group: {}",
        kafka.getBootstrapServers(),
        kafka.getTopic(),
        kafka.getConsumerGroupId());

    return props;
  }

  /**
   * Builds a minimal set of Schema Registry client properties,
   * useful for direct schema introspection or health checks.
   */
  public Properties buildSchemaRegistryProperties(Credentials credentials) {

    ConfigSchemaRegistry sr =
        appConfig.getSchemaRegistryConfig();

    Properties props =
        new Properties();

    props.setProperty(
        AbstractKafkaSchemaSerDeConfig.SCHEMA_REGISTRY_URL_CONFIG,
        sr.getUrl());

    props.setProperty("basic.auth.credentials.source", "USER_INFO");
    props.setProperty("basic.auth.user.info", credentials.getUsername() + ":" + credentials.getPassword());

    if (sr.isSecure()) {

      props.put(
          "schema.registry.ssl.truststore.location",
          sr.getCertDir() + "/truststore.jks");

      props.put(
          "schema.registry.ssl.truststore.password",
          "changeit");
    }

    return props;
  }
}
