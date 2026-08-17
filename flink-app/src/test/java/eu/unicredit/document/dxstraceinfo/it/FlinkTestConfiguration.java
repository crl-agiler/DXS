package eu.unicredit.document.dxstraceinfo.it;

import eu.unicredit.document.dxstraceinfo.api.DXSContext;
import eu.unicredit.document.dxstraceinfo.api.FlinkConfiguration;
import org.apache.flink.api.common.restartstrategy.RestartStrategies;
import org.apache.flink.streaming.api.CheckpointingMode;
import org.apache.flink.streaming.api.environment.CheckpointConfig;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;

import static eu.unicredit.document.dxstraceinfo.it.FlinkDSXTraceInfoIT.checkpointDir;

public class FlinkTestConfiguration implements FlinkConfiguration {
    private static final long CHECKPOINT_INTERVAL_MS = 5_000L;
    private static final long CHECKPOINT_TIMEOUT_MS = 120_000L;

    @Override
    public void configure(DXSContext context) throws Exception {
        StreamExecutionEnvironment streamExecutionEnvironment = context.streamingExecutionEnv();
        streamExecutionEnvironment.enableCheckpointing(
                CHECKPOINT_INTERVAL_MS);

        CheckpointConfig checkpointConfig =
                streamExecutionEnvironment.getCheckpointConfig();

        checkpointConfig.setCheckpointingMode(
                CheckpointingMode.EXACTLY_ONCE);

        checkpointConfig.setCheckpointTimeout(
                CHECKPOINT_TIMEOUT_MS);

        checkpointConfig.setMinPauseBetweenCheckpoints(0L);
        checkpointConfig.setMaxConcurrentCheckpoints(1);
        checkpointConfig.setTolerableCheckpointFailureNumber(0);

        checkpointConfig.setCheckpointStorage(
                checkpointDir
                        .toAbsolutePath()
                        .normalize()
                        .toUri()
                        .toString());

        streamExecutionEnvironment.setRestartStrategy(
                RestartStrategies.noRestart());
    }
}
