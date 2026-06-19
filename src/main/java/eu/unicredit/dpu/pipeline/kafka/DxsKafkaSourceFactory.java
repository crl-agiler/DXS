package eu.unicredit.dpu.pipeline.kafka;

import eu.unicredit.dpu.pipeline.config.EnvironmentConfig;
import eu.unicredit.dpu.pipeline.config.KafkaConfig;
import org.apache.avro.generic.GenericRecord;
import org.apache.flink.connector.kafka.source.KafkaSource;
import org.apache.flink.connector.kafka.source.enumerator.initializer.OffsetsInitializer;
import org.apache.flink.formats.avro.registry.confluent.ConfluentRegistryAvroDeserializationSchema;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Properties;

/**
 * Factory that builds the Flink {@link KafkaSource} for the DXS traceinfo topic.
 *
 * <p>The source uses:
 * <ul>
 *   <li>Confluent Avro deserialisation with Schema Registry lookup</li>
 *   <li>Consumer group ID assigned by CKF for this environment</li>
 *   <li>{@code COMMITTED} offset initialisation — Flink resumes from the last
 *       committed offset on restart; falls back to {@code EARLIEST} on first run</li>
 * </ul>
 */
public class DxsKafkaSourceFactory {

    private static final Logger LOG = LoggerFactory.getLogger(DxsKafkaSourceFactory.class);

    private final EnvironmentConfig envConfig;

    public DxsKafkaSourceFactory(EnvironmentConfig envConfig) {
        this.envConfig = envConfig;
    }

    /**
     * Builds and returns the configured {@link KafkaSource}.
     *
     * @return a KafkaSource emitting {@link GenericRecord} instances
     *         deserialised from the DXS Avro events
     */
    public KafkaSource<GenericRecord> build() {
        KafkaConfig kafka = envConfig.getKafka();
        String schemaRegistryUrl = envConfig.getSchemaRegistry().getUrl();

        LOG.info("Building KafkaSource — topic: {}, group: {}, brokers: {}",
                kafka.getTopic(), kafka.getConsumerGroupId(), kafka.getBootstrapServers());

        Properties consumerProps = new KafkaPropertiesBuilder(envConfig).buildConsumerProperties();

        return KafkaSource.<GenericRecord>builder()
                .setBootstrapServers(kafka.getBootstrapServers())
                .setTopics(kafka.getTopic())
                .setGroupId(kafka.getConsumerGroupId())
                // Resume from last committed offset; use EARLIEST on first run
                .setStartingOffsets(OffsetsInitializer.committedOffsets(
                        org.apache.kafka.clients.consumer.OffsetResetStrategy.EARLIEST))
                .setValueOnlyDeserializer(
                        ConfluentRegistryAvroDeserializationSchema.forGeneric(
                                org.apache.avro.Schema.create(org.apache.avro.Schema.Type.NULL), // placeholder — resolved at runtime from SR
                                schemaRegistryUrl))
                .setProperties(consumerProps)
                .build();
    }
}
