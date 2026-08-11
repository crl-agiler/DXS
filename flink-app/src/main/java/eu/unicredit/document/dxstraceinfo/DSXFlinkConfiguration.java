package eu.unicredit.document.dxstraceinfo;

import eu.unicredit.document.dxstraceinfo.api.FlinkConfiguration;
import eu.unicredit.document.dxstraceinfo.config.ConfigApp;
import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.apache.flink.streaming.api.CheckpointingMode;
import org.apache.flink.streaming.api.environment.CheckpointConfig;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;

import java.io.InputStream;
import java.util.Properties;

import static org.apache.flink.runtime.jobgraph.tasks.CheckpointCoordinatorConfiguration.MINIMAL_CHECKPOINT_TIME;

@RequiredArgsConstructor
@Slf4j
public class DSXFlinkConfiguration implements FlinkConfiguration {

    private static final String DP_STORAGE_AREA_NAME =
            "dxs-traceinfo-gcs-storage-area";

    private final ConfigApp appConfig;

    @Override
    public void configure(StreamExecutionEnvironment env) throws Exception {
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

            log.warn(
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

        CheckpointConfig checkpointConfig = env.getCheckpointConfig();
        checkpointConfig
                .setCheckpointingMode(
                        CheckpointingMode.EXACTLY_ONCE);

        checkpointConfig
                .setCheckpointStorage(
                        checkpointPath);

        checkpointConfig
                .setCheckpointTimeout(
                        appConfig
                                .getFlinkConfig()
                                .getCheckpointTimeout()
                                .toMillis());

        checkpointConfig
                .setMinPauseBetweenCheckpoints(
                        appConfig
                                .getFlinkConfig()
                                .getMinPauseBetweenCheckpoints()
                                .toMillis());

        checkpointConfig
                .setTolerableCheckpointFailureNumber(2);

        checkpointConfig
                .setExternalizedCheckpointCleanup(
                        CheckpointConfig
                                .ExternalizedCheckpointCleanup
                                .RETAIN_ON_CANCELLATION);
    }

}
