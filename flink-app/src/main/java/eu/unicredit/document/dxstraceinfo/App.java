package eu.unicredit.document.dxstraceinfo;

import eu.unicredit.document.dxstraceinfo.api.SingleSourceLogicPipeline;
import eu.unicredit.document.dxstraceinfo.avro.DossierTraceinfoEvent;
import eu.unicredit.document.dxstraceinfo.config.AppCliArguments;
import eu.unicredit.document.dxstraceinfo.config.ConfigApp;
import eu.unicredit.document.dxstraceinfo.config.ConfigIcebergCatalog;
import eu.unicredit.document.dxstraceinfo.config.GcpConfigAppRetriever;
import eu.unicredit.document.dxstraceinfo.credentials.CredentialsRetriever;
import eu.unicredit.document.dxstraceinfo.credentials.GcpCredentialsRetriever;
import eu.unicredit.document.dxstraceinfo.factory.SplitContextFactory;
import eu.unicredit.document.dxstraceinfo.transform.SplitContext;
import lombok.Builder;
import lombok.NonNull;
import org.apache.flink.streaming.api.CheckpointingMode;
import org.apache.flink.streaming.api.environment.CheckpointConfig;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.table.data.RowData;
import org.apache.flink.util.OutputTag;
import org.apache.hadoop.conf.Configuration;
import org.apache.iceberg.flink.CatalogLoader;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.io.InputStream;
import java.util.Collections;
import java.util.List;
import java.util.Properties;

import static org.apache.flink.runtime.jobgraph.tasks.CheckpointCoordinatorConfiguration.MINIMAL_CHECKPOINT_TIME;

@Builder(toBuilder = true)
public class App {

    public static final String DP_STORAGE_AREA_NAME =
            "dxs-traceinfo-gcs-storage-area";

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
    }

    public void setupFlinkJob(
            StreamExecutionEnvironment env)
            throws Exception {
        CatalogLoader catalogLoader = createCatalogLoader();
        configureEnvironment(env);
        List<SplitContext<?>> splitContexts =
                new SplitContextFactory(catalogLoader)
                        .create();
        String jobName = appConfig
                .getFlinkConfig()
                .getJobName();
        SingleSourceLogicPipeline<DossierTraceinfoEvent, RowData> pipeline = SingleSourceLogicPipeline.<DossierTraceinfoEvent, RowData>builder()
                .source(new DSXKafkaSource(appConfig, credentialsRetriever))
                .processor(new DSXProcessor(DISCARD_TAG, splitContexts))
                .sink(new DXSSink(appConfig, splitContexts, catalogLoader, DISCARD_TAG))
                .jobName(jobName).build();
        pipeline.run(env);
        LOG.info("FLINK STREAM SET UP");
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

    private void configureEnvironment(
            StreamExecutionEnvironment env) throws IOException {

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
        InputStream is = App.class.getResourceAsStream("build.properties");
        Properties properties = new Properties(3);
        properties.load(is);
        String version = properties.getProperty("version");
        String major = version.split("\\.")[0];
        String artifactId = properties.getProperty("artifactId");
        String checkpointPath =
                String.format(
                        "gs://%s/%s/%s/%s/%s/checkpoints",
                        appConfig.getBucketName(),
                        major,
                        DP_STORAGE_AREA_NAME,
                        artifactId,
                        version);

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
}
