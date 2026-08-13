package eu.unicredit.document.dxstraceinfo;

import eu.unicredit.document.dxstraceinfo.api.FlinkConfiguration;
import eu.unicredit.document.dxstraceinfo.config.ConfigApp;
import eu.unicredit.document.dxstraceinfo.config.ConfigFlink;
import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.apache.flink.api.common.ExecutionConfig;
import org.apache.flink.streaming.api.CheckpointingMode;
import org.apache.flink.streaming.api.environment.CheckpointConfig;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;

import javax.enterprise.context.ApplicationScoped;
import javax.inject.Inject;
import java.io.InputStream;
import java.util.Properties;

import static org.apache.flink.runtime.jobgraph.tasks.CheckpointCoordinatorConfiguration.MINIMAL_CHECKPOINT_TIME;

@ApplicationScoped
@RequiredArgsConstructor(onConstructor_ = @Inject)
@Slf4j
public class DSXFlinkConfiguration implements FlinkConfiguration {

    private static final String DP_STORAGE_AREA_NAME = "dxs-traceinfo-gcs-storage-area";
    private final ConfigApp appConfig;

    @Override
    public void configure(StreamExecutionEnvironment env) throws Exception {
        ConfigFlink flinkConfig = appConfig.getFlinkConfig();
        ExecutionConfig config = env.getConfig();

        env.setParallelism(flinkConfig.getBaseParallelism());

        config.enableObjectReuse();

        long checkpointMs = flinkConfig.getCheckpointInterval().toMillis();

        if (checkpointMs < MINIMAL_CHECKPOINT_TIME) {
            log.warn("Checkpoint disabled [{} < {}]", checkpointMs, MINIMAL_CHECKPOINT_TIME);
            return;
        }

        InputStream is = this.getClass().getResourceAsStream("build.properties");
        Properties properties = new Properties(3);
        properties.load(is);
        String version = properties.getProperty("version");
        String major = version.split("\\.")[0];
        String artifactId = properties.getProperty("artifactId");
        String checkpointPath = String.format("gs://%s/%s/%s/%s/%s/checkpoints", appConfig.getBucketName(), major, DP_STORAGE_AREA_NAME, artifactId, version);

        env.enableCheckpointing(checkpointMs);

        CheckpointConfig checkpointConfig = env.getCheckpointConfig();
        checkpointConfig.setCheckpointingMode(CheckpointingMode.EXACTLY_ONCE);

        checkpointConfig.setCheckpointStorage(checkpointPath);

        long checkpointTimeout = flinkConfig.getCheckpointTimeout().toMillis();
        checkpointConfig.setCheckpointTimeout(checkpointTimeout);

        long minPauseBetweenCheckpoints = flinkConfig.getMinPauseBetweenCheckpoints().toMillis();
        checkpointConfig.setMinPauseBetweenCheckpoints(minPauseBetweenCheckpoints);

        checkpointConfig.setTolerableCheckpointFailureNumber(2);

        CheckpointConfig.ExternalizedCheckpointCleanup checkpointCleanup = CheckpointConfig.ExternalizedCheckpointCleanup.RETAIN_ON_CANCELLATION;
        checkpointConfig.setExternalizedCheckpointCleanup(checkpointCleanup);
    }

}
