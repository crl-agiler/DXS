package eu.unicredit.document.dxstraceinfo.kafka;

import eu.unicredit.document.dxstraceinfo.avro.DossierTraceinfoEvent;
import eu.unicredit.document.dxstraceinfo.config.ConfigApp;
import eu.unicredit.document.dxstraceinfo.config.ConfigKafka;
import java.util.HashMap;
import java.util.Map;
import java.util.Properties;
import org.apache.flink.connector.kafka.source.KafkaSource;
import org.apache.flink.connector.kafka.source.enumerator.initializer.OffsetsInitializer;
import org.apache.flink.formats.avro.registry.confluent.ConfluentRegistryAvroDeserializationSchema;
import org.apache.kafka.clients.consumer.OffsetResetStrategy;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class DxsKafkaSourceFactory {

  private static final Logger LOG =
      LoggerFactory.getLogger(DxsKafkaSourceFactory.class);

  private final ConfigApp appConfig;

  public DxsKafkaSourceFactory(ConfigApp appConfig) {
    this.appConfig = appConfig;
  }

  /**
   * Builds and returns the configured KafkaSource.
   */
  public KafkaSource<DossierTraceinfoEvent> build() {

    ConfigKafka kafkaConfig =
        appConfig.getKafkaConfig();

    String schemaRegistryUrl =
        appConfig.getSchemaRegistryConfig().getUrl();

    LOG.info(
        "Building KafkaSource - topic: {}, group: {}, brokers: {}, schemaRegistry: {}",
        kafkaConfig.getTopic(),
        kafkaConfig.getConsumerGroupId(),
        kafkaConfig.getBootstrapServers(),
        schemaRegistryUrl);

    KafkaPropertiesBuilder propsBuilder =
        new KafkaPropertiesBuilder(appConfig);

    Properties consumerProps =
        propsBuilder.buildConsumerProperties();

    Map<String, Object> srConfig =
        toMap(propsBuilder.buildSchemaRegistryProperties());

    return KafkaSource.<DossierTraceinfoEvent>builder()
        .setBootstrapServers(
            kafkaConfig.getBootstrapServers())
        .setTopics(
            kafkaConfig.getTopic())
        .setGroupId(
            kafkaConfig.getConsumerGroupId())
        .setStartingOffsets(
            buildOffsetsInitializer(
                kafkaConfig.getStartingOffset()))
        .setValueOnlyDeserializer(
            ConfluentRegistryAvroDeserializationSchema.forSpecific(
                DossierTraceinfoEvent.class,
                schemaRegistryUrl,
                srConfig))
        .setProperties(consumerProps)
        .build();
  }

  private static Map<String, Object> toMap(Properties properties) {
    Map<String, Object> map = new HashMap<>();
    properties.forEach((k, v) -> map.put(k.toString(), v));
    return map;
  }

  private OffsetsInitializer buildOffsetsInitializer(
      String startingOffset) {

    switch (startingOffset.toLowerCase()) {

      case "earliest":
        return OffsetsInitializer.earliest();

      case "latest":
        return OffsetsInitializer.latest();

      case "committed_or_latest":
        return OffsetsInitializer.committedOffsets(
            OffsetResetStrategy.LATEST);

      case "committed_or_earliest":
      default:
        return OffsetsInitializer.committedOffsets(
            OffsetResetStrategy.EARLIEST);
    }
  }
}