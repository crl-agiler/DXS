package eu.unicredit.document.dxstraceinfo.config;

import eu.unicredit.document.dxstraceinfo.api.DXSContext;
import eu.unicredit.document.dxstraceinfo.api.FlinkConfiguration;
import lombok.extern.slf4j.Slf4j;
import org.apache.flink.api.common.ExecutionConfig;
import org.apache.flink.streaming.api.CheckpointingMode;
import org.apache.flink.streaming.api.environment.CheckpointConfig;

import static org.apache.flink.runtime.jobgraph.tasks.CheckpointCoordinatorConfiguration.MINIMAL_CHECKPOINT_TIME;


@Slf4j
public class DSXFlinkConfiguration implements FlinkConfiguration {

    private static final String DP_STORAGE_AREA_NAME = "dxs-traceinfo-gcs-storage-area";
    @Override
    public void configure(DXSContext context) throws Exception {

        ConfigApp appConfig = context.config();
        ConfigFlink flinkConfig = appConfig.getFlinkConfig();
        ExecutionConfig config = context.streamingExecutionEnv().getConfig();
        var env = context.streamingExecutionEnv();
        env.setParallelism(flinkConfig.getBaseParallelism());

        config.enableObjectReuse();

        long checkpointMs = flinkConfig.getCheckpointInterval().toMillis();

        if (checkpointMs < MINIMAL_CHECKPOINT_TIME) {
            log.warn("Checkpoint disabled [{} < {}]", checkpointMs, MINIMAL_CHECKPOINT_TIME);
            return;
        }
        ArtifactInformation artifactInformation = context.artifactInformation();
        String checkpointPath = String.format("gs://%s/%s/%s/%s/%s/checkpoints", appConfig.getBucketName(), artifactInformation.getMajor(), DP_STORAGE_AREA_NAME, artifactInformation.getArtifactName(), artifactInformation.getVersion());

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
