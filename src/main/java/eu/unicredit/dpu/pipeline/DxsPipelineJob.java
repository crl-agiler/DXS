package eu.unicredit.dpu.pipeline;

import eu.unicredit.dpu.pipeline.config.ConfigLoader;
import eu.unicredit.dpu.pipeline.config.EnvironmentConfig;
import eu.unicredit.dpu.pipeline.config.FlinkConfig;
import eu.unicredit.dpu.pipeline.config.PipelineConfig;
import eu.unicredit.dpu.pipeline.kafka.DxsKafkaSourceFactory;
import eu.unicredit.dpu.pipeline.model.TransformedEvent;
import eu.unicredit.dpu.pipeline.schema.SchemaRegistryClientFactory;
import eu.unicredit.dpu.pipeline.transform.DossierTransformFunction;
import org.apache.avro.generic.GenericRecord;
import org.apache.flink.api.common.eventtime.WatermarkStrategy;
import org.apache.flink.connector.kafka.source.KafkaSource;
import org.apache.flink.contrib.streaming.state.EmbeddedRocksDBStateBackend;
import org.apache.flink.streaming.api.CheckpointingMode;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.CheckpointConfig;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.time.Duration;

/**
 * Entry point for the DXS → DPU streaming pipeline.
 *
 * <p>This class is responsible for:
 * <ol>
 *   <li>Loading the YAML configuration for the target environment</li>
 *   <li>Verifying connectivity to Kafka and Schema Registry</li>
 *   <li>Configuring the Flink execution environment (checkpointing, state backend)</li>
 *   <li>Building the Kafka source and wiring the pipeline graph</li>
 *   <li>Submitting the job to the Flink cluster</li>
 * </ol>
 *
 * <h3>Usage</h3>
 * <pre>
 *   # Local run (SIT, config from classpath)
 *   PIPELINE_ENV=sit \
 *   KAFKA_USERNAME=your_user \
 *   KAFKA_PASSWORD=your_password \
 *   java -jar dxs-dpu-pipeline-1.0.0-SNAPSHOT.jar
 *
 *   # With explicit config file
 *   java -jar dxs-dpu-pipeline-1.0.0-SNAPSHOT.jar \
 *        --config /opt/pipeline/config/pipeline-config.yaml
 *
 *   # Flink cluster submission (PRD)
 *   flink run -p 12 dxs-dpu-pipeline-1.0.0-SNAPSHOT.jar \
 *        --config /opt/pipeline/config/pipeline-config.yaml
 * </pre>
 */
public class DxsPipelineJob {

    private static final Logger LOG = LoggerFactory.getLogger(DxsPipelineJob.class);

    public static void main(String[] args) throws Exception {

        // ── 1. Parse CLI arguments ────────────────────────────────
        String configPath = parseConfigArg(args);

        // ── 2. Load configuration ─────────────────────────────────
        ConfigLoader configLoader = new ConfigLoader();
        PipelineConfig pipelineConfig = configLoader.load(configPath);

        String activeEnv = pipelineConfig.resolveActiveEnvironment();
        EnvironmentConfig envConfig = pipelineConfig.activeEnvironment();

        LOG.info("════════════════════════════════════════════════════");
        LOG.info("  DXS DPU Streaming Pipeline — environment: {}", activeEnv.toUpperCase());
        LOG.info("  Topic:     {}", envConfig.getKafka().getTopic());
        LOG.info("  Group ID:  {}", envConfig.getKafka().getConsumerGroupId());
        LOG.info("  Brokers:   {}", envConfig.getKafka().getBootstrapServers());
        LOG.info("  SR URL:    {}", envConfig.getSchemaRegistry().getUrl());
        LOG.info("════════════════════════════════════════════════════");

        // ── 3. Verify Schema Registry connectivity ────────────────
        new SchemaRegistryClientFactory(envConfig).createAndVerify();

        // ── 4. Configure Flink execution environment ──────────────
        StreamExecutionEnvironment env = buildFlinkEnvironment(
                envConfig.getFlink(), pipelineConfig.getPipeline());

        // ── 5. Build Kafka source ─────────────────────────────────
        KafkaSource<GenericRecord> kafkaSource =
                new DxsKafkaSourceFactory(envConfig).build();

        // ── 6. Wire the pipeline ──────────────────────────────────
        PipelineConfig.PipelineSettings settings = pipelineConfig.getPipeline();

        DataStream<GenericRecord> rawStream = env
                .fromSource(
                        kafkaSource,
                        WatermarkStrategy
                                .<GenericRecord>forBoundedOutOfOrderness(
                                        Duration.ofMillis(settings.getWatermarkOutOfOrdernessMs()))
                                .withTimestampAssigner((record, ts) -> extractEventTimestamp(record)),
                        "DXS Kafka Source — " + activeEnv)
                .setParallelism(envConfig.getFlink().getParallelism());

        // ── 7. Transform stage — stateless flatten + outcome mapping ──
        // Design decision (confirmed with DXS, see Open Questions doc):
        //   - no physical deletes at any entity level → safe to upsert blindly
        //   - transactional producer in rollout → no duplicates expected
        // Per-row "did this specific document's status really change" filtering
        // is NOT done here — still being clarified with DXS/Business, and if
        // confirmed will be a separate batch stage over Iceberg snapshots,
        // not a stateful operator in this streaming job.
        DataStream<TransformedEvent> transformed = rawStream
                .process(new DossierTransformFunction())
                .name("Transform — flatten + outcome mapping")
                .uid("transform-flatten-outcome");

        // ── TODO next: split into per-entity streams and wire Iceberg sinks ──
        //   transformed.getSideOutput(DOSSIER_TAG).sinkTo(icebergDossierSink);
        //   transformed.getSideOutput(DOCUMENT_GROUP_TAG).sinkTo(icebergDocumentGroupSink);
        //   transformed.getSideOutput(DOCUMENT_TAG).sinkTo(icebergDocumentSink);
        //   transformed.getSideOutput(SIGNER_TAG).sinkTo(icebergSignerSink);

        transformed.print(); // temporary — remove once Iceberg sinks are wired

        // ── 8. Execute ────────────────────────────────────────────
        env.execute("DXS DPU Streaming Pipeline [" + activeEnv + "]");
    }

    // ── Private helpers ───────────────────────────────────────────

    /**
     * Configures the Flink {@link StreamExecutionEnvironment} with:
     * <ul>
     *   <li>Parallelism aligned to Kafka partition count</li>
     *   <li>Exactly-once checkpointing with RocksDB state backend</li>
     *   <li>Checkpoint persistence to GCS</li>
     * </ul>
     */
    private static StreamExecutionEnvironment buildFlinkEnvironment(
            FlinkConfig flinkConfig,
            PipelineConfig.PipelineSettings settings) {

        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setParallelism(flinkConfig.getParallelism());

        // ── Checkpointing ─────────────────────────────────────────
        env.enableCheckpointing(
                flinkConfig.getCheckpointIntervalMs(),
                CheckpointingMode.EXACTLY_ONCE);

        CheckpointConfig checkpointConfig = env.getCheckpointConfig();
        checkpointConfig.setCheckpointTimeout(flinkConfig.getCheckpointTimeoutMs());
        checkpointConfig.setMinPauseBetweenCheckpoints(flinkConfig.getMinPauseBetweenCheckpointsMs());
        // Retain checkpoints on job cancellation — allows savepoint-less recovery

        checkpointConfig.setExternalizedCheckpointCleanup(
                CheckpointConfig.ExternalizedCheckpointCleanup.RETAIN_ON_CANCELLATION);

        // ── State backend ─────────────────────────────────────────
        if ("rocksdb".equalsIgnoreCase(flinkConfig.getStateBackend())) {
            env.setStateBackend(new EmbeddedRocksDBStateBackend(true));
            LOG.info("State backend: EmbeddedRocksDB (incremental checkpoints enabled)");
        }

        // ── Checkpoint storage (GCS) ──────────────────────────────
        if (flinkConfig.getCheckpointDir() != null && !flinkConfig.getCheckpointDir().isBlank()) {
            env.getCheckpointConfig().setCheckpointStorage(flinkConfig.getCheckpointDir());
            LOG.info("Checkpoint storage: {}", flinkConfig.getCheckpointDir());
        }

        LOG.info("Flink environment configured — parallelism: {}, checkpoint interval: {}ms",
                flinkConfig.getParallelism(), flinkConfig.getCheckpointIntervalMs());
        return env;
    }

    /**
     * Extracts the {@code eventTimestamp} field from a DXS Avro record.
     * Falls back to the current system time if the field is absent or null
     * (e.g. during initial load replay of older schema events).
     */
    private static long extractEventTimestamp(GenericRecord record) {
        try {
            Object ts = record.get("eventTimestamp");
            if (ts instanceof Long) {
                return (Long) ts;
            }
        } catch (Exception e) {
            LOG.warn("Could not extract eventTimestamp from record — using system time. Cause: {}", e.getMessage());
        }
        return System.currentTimeMillis();
    }

    /**
     * Parses the {@code --config <path>} argument from the CLI args array.
     *
     * @return the config file path, or null if not provided
     */
    private static String parseConfigArg(String[] args) {
        for (int i = 0; i < args.length - 1; i++) {
            if ("--config".equals(args[i])) {
                return args[i + 1];
            }
        }
        return null;
    }
}
