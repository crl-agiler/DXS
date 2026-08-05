package eu.unicredit.document.dxstraceinfo;

import static org.apache.flink.runtime.jobgraph.tasks.CheckpointCoordinatorConfiguration.MINIMAL_CHECKPOINT_TIME;

import eu.unicredit.document.dxstraceinfo.avro.DossierTraceinfoEvent;
import eu.unicredit.document.dxstraceinfo.config.AppCliArguments;
import eu.unicredit.document.dxstraceinfo.config.ConfigApp;
import eu.unicredit.document.dxstraceinfo.config.ConfigIcebergCatalog;
import eu.unicredit.document.dxstraceinfo.config.GcpConfigAppRetriever;
import eu.unicredit.document.dxstraceinfo.credentials.Credentials;
import eu.unicredit.document.dxstraceinfo.credentials.CredentialsRetriever;
import eu.unicredit.document.dxstraceinfo.credentials.GcpCredentialsRetriever;
import eu.unicredit.document.dxstraceinfo.credentials.MalformedCredentialsException;
import eu.unicredit.document.dxstraceinfo.factory.SplitContextFactory;
import eu.unicredit.document.dxstraceinfo.kafka.DxsKafkaSourceFactory;
import eu.unicredit.document.dxstraceinfo.sink.IcebergSink;
import eu.unicredit.document.dxstraceinfo.transform.SplitContext;
import eu.unicredit.document.dxstraceinfo.transform.SplitTransformLogic;
import java.io.IOException;
import java.time.Duration;
import java.util.Collections;
import java.util.List;
import lombok.Builder;
import lombok.NonNull;
import org.apache.flink.api.common.eventtime.WatermarkStrategy;
import org.apache.flink.connector.kafka.source.KafkaSource;
import org.apache.flink.streaming.api.CheckpointingMode;
import org.apache.flink.streaming.api.datastream.KeyedStream;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.CheckpointConfig;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.table.data.RowData;
import org.apache.flink.util.OutputTag;
import org.apache.hadoop.conf.Configuration;
import org.apache.iceberg.catalog.TableIdentifier;
import org.apache.iceberg.flink.CatalogLoader;
import org.apache.iceberg.flink.TableLoader;
import org.apache.iceberg.flink.sink.FlinkSink;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

@Builder(toBuilder = true)
public class App {

  public static final String APP_VERSION = "0.0.12";
  public static final String APP_MAJOR_VERSION =
      APP_VERSION.split("\\.")[0];

  public static final String DP_STORAGE_AREA_NAME =
      "dxs-traceinfo-gcs-storage-area";

  public static final String DP_APP_NAME =
      "dxs-traceinfo-flink-wl-streaming";

  private static final Logger LOG =
      LoggerFactory.getLogger(App.class);

  private static final OutputTag<RowData> DISCARD_TAG =
      new OutputTag<>("dxs-traceinfo-discard-log") {
      };

  private final @NonNull ConfigApp appConfig;
  private final @NonNull CredentialsRetriever credentialsRetriever;

  public static App dxsTraceInfoApp(
      ConfigApp appConfig) {

    return App.builder()
        .appConfig(appConfig)
        .credentialsRetriever(
            new GcpCredentialsRetriever())
        .build();
  }

  public static void main(String[] args)
      throws Exception {

    AppCliArguments argv =
        AppCliArguments.parse(args);

    ConfigApp appConfig =
        new GcpConfigAppRetriever()
            .getConfig(
                argv.getBucketName(),
                argv.getBaseConfigPath(),
                argv.getEnvConfigPath());

    App app = dxsTraceInfoApp(appConfig);

    StreamExecutionEnvironment env =
        StreamExecutionEnvironment
            .getExecutionEnvironment();

    app.setupFlinkJob(env);
    app.runFlinkJob(env);
  }

  public void setupFlinkJob(
      StreamExecutionEnvironment env)
      throws Exception {

    configureEnvironment(env);

    KafkaSource<DossierTraceinfoEvent> kafkaSource =
        buildKafkaSource();

    KeyedStream<DossierTraceinfoEvent, String> rawStream =
        env.fromSource(
                kafkaSource,
                createWatermarkStrategy(),
                "DXS Kafka Source")
            .keyBy(DossierTraceinfoEvent::getMasterDossierId);

    CatalogLoader catalogLoader =
        createCatalogLoader();

    List<SplitContext<?>> splitContexts =
        new SplitContextFactory(catalogLoader)
            .create();

    SingleOutputStreamOperator<RowData> transformed =
        rawStream
            .process(
                new SplitTransformLogic(
                    splitContexts,
                    DISCARD_TAG))
            .name(
                "SplitTransformLogic")
            .uid(
                "transform-transform-logic");

    new IcebergSink(splitContexts, appConfig.getFlinkConfig())
        .sinkFrom(transformed);

    configureDiscardSink(
        transformed,
        catalogLoader);

    LOG.info("FLINK STREAM SET UP");
  }

  private void configureEnvironment(
      StreamExecutionEnvironment env) {

    env.setParallelism(
        appConfig.getFlinkConfig()
            .getBaseParallelism());

    env.getConfig()
        .enableObjectReuse();

    long checkpointMs =
        appConfig
            .getFlinkConfig()
            .getCheckpointInterval()
            .toMillis();

    if (checkpointMs <
        MINIMAL_CHECKPOINT_TIME) {

      LOG.warn(
          "Checkpoint disabled [{} < {}]",
          checkpointMs,
          MINIMAL_CHECKPOINT_TIME);

      return;
    }

    String checkpointPath =
        String.format(
            "gs://%s/%s/%s/%s/%s/checkpoints",
            appConfig.getBucketName(),
            APP_MAJOR_VERSION,
            DP_STORAGE_AREA_NAME,
            DP_APP_NAME,
            APP_VERSION);

    env.enableCheckpointing(checkpointMs);

    env.getCheckpointConfig()
        .setCheckpointingMode(
            CheckpointingMode.EXACTLY_ONCE);

    env.getCheckpointConfig()
        .setCheckpointStorage(
            checkpointPath);

    env.getCheckpointConfig()
        .setCheckpointTimeout(
            appConfig
                .getFlinkConfig()
                .getCheckpointTimeout()
                .toMillis());

    env.getCheckpointConfig()
        .setMinPauseBetweenCheckpoints(
            appConfig
                .getFlinkConfig()
                .getMinPauseBetweenCheckpoints()
                .toMillis());

    env.getCheckpointConfig()
        .setTolerableCheckpointFailureNumber(2);

    env.getCheckpointConfig()
        .setExternalizedCheckpointCleanup(
            CheckpointConfig
                .ExternalizedCheckpointCleanup
                .RETAIN_ON_CANCELLATION);
  }

  private KafkaSource<DossierTraceinfoEvent> buildKafkaSource()
      throws IOException,
      MalformedCredentialsException {

    Credentials ignored =
        credentialsRetriever.getCredentials(
            appConfig.getProjectId(),
            appConfig
                .getSchemaRegistryConfig()
                .getSecretId());

    return new DxsKafkaSourceFactory(
        appConfig)
        .build();
  }

  private CatalogLoader createCatalogLoader() {

    ConfigIcebergCatalog catalog =
        appConfig.getConfigIcebergCatalog();

    return CatalogLoader.hadoop(
        catalog.getCatalogName(),
        new Configuration(),
        Collections.singletonMap(
            "warehouse",
            catalog.getCatalogLocation()));
  }


  private void configureDiscardSink(
      SingleOutputStreamOperator<RowData> stream,
      CatalogLoader catalogLoader) {

    TableLoader discardTableLoader =
        TableLoader.fromCatalog(
            catalogLoader,
            TableIdentifier.of(
                DISCARD_TAG.getId()));

    FlinkSink
        .forRowData(
            stream.getSideOutput(
                DISCARD_TAG))
        .tableLoader(
            discardTableLoader)
        .upsert(false)
        .append();
  }

  private WatermarkStrategy<DossierTraceinfoEvent> createWatermarkStrategy() {

    return WatermarkStrategy
        .<DossierTraceinfoEvent>
            forBoundedOutOfOrderness(
            Duration.ofSeconds(30))
        .withTimestampAssigner(
            (event, ts) ->
                event
                    .getEventTimestamp()
                    .toEpochMilli());
  }

  public void runFlinkJob(
      StreamExecutionEnvironment env)
      throws Exception {

    env.execute(
        appConfig
            .getFlinkConfig()
            .getJobName());
  }
}
