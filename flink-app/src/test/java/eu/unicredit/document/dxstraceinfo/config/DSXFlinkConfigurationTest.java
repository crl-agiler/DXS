package eu.unicredit.document.dxstraceinfo.config;

import eu.unicredit.document.dxstraceinfo.api.DXSContext;
import org.apache.flink.api.common.ExecutionConfig;
import org.apache.flink.streaming.api.environment.CheckpointConfig;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.junit.jupiter.api.Test;

import java.time.Duration;

import static org.apache.flink.runtime.jobgraph.tasks.CheckpointCoordinatorConfiguration.MINIMAL_CHECKPOINT_TIME;
import static org.mockito.Mockito.anyLong;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.never;
import static org.mockito.Mockito.startsWith;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

class DSXFlinkConfigurationTest {

    @Test
    void shouldDisableCheckpointWhenIntervalIsTooSmall()
            throws Exception {

        ConfigApp appConfig = mock(ConfigApp.class);
        ConfigFlink flinkConfig = mock(ConfigFlink.class);

        DXSContext context = mock(DXSContext.class);

        StreamExecutionEnvironment env =
                mock(StreamExecutionEnvironment.class);

        ExecutionConfig executionConfig =
                mock(ExecutionConfig.class);

        when(context.config()).thenReturn(appConfig);
        when(appConfig.getFlinkConfig()).thenReturn(flinkConfig);

        when(context.streamingExecutionEnv()).thenReturn(env);

        when(env.getConfig()).thenReturn(executionConfig);

        when(flinkConfig.getBaseParallelism()).thenReturn(2);
        when(flinkConfig.getCheckpointInterval())
                .thenReturn(
                        Duration.ofMillis(
                                MINIMAL_CHECKPOINT_TIME - 1
                        )
                );

        DSXFlinkConfiguration configuration =
                new DSXFlinkConfiguration();

        configuration.configure(context);

        verify(env).setParallelism(2);
        verify(executionConfig).enableObjectReuse();

        verify(env, never())
                .enableCheckpointing(anyLong());

        verify(env, never())
                .getCheckpointConfig();
    }

    @Test
    void shouldConfigureCheckpointing()
            throws Exception {

        ConfigApp appConfig = mock(ConfigApp.class);
        ConfigFlink flinkConfig = mock(ConfigFlink.class);
        AppCliArguments appCliArguments = mock(AppCliArguments.class);

        DXSContext context = mock(DXSContext.class);

        StreamExecutionEnvironment env =
                mock(StreamExecutionEnvironment.class);

        ExecutionConfig executionConfig =
                mock(ExecutionConfig.class);

        CheckpointConfig checkpointConfig =
                mock(CheckpointConfig.class);

        when(context.config()).thenReturn(appConfig);
        when(context.args()).thenReturn(appCliArguments);
        when(appConfig.getFlinkConfig()).thenReturn(flinkConfig);

        when(context.streamingExecutionEnv()).thenReturn(env);
        when(context.artifactInformation()).thenReturn(new ArtifactInformation("dsx-traceinfo", "1.0.0", "1"));
        when(env.getConfig()).thenReturn(executionConfig);
        when(env.getCheckpointConfig())
                .thenReturn(checkpointConfig);

        when(appCliArguments.getBucketName())
                .thenReturn("test-bucket");

        when(flinkConfig.getBaseParallelism())
                .thenReturn(4);

        when(flinkConfig.getCheckpointInterval())
                .thenReturn(
                        Duration.ofMinutes(1)
                );

        when(flinkConfig.getCheckpointTimeout())
                .thenReturn(
                        Duration.ofMinutes(5)
                );

        when(flinkConfig.getMinPauseBetweenCheckpoints())
                .thenReturn(
                        Duration.ofSeconds(30)
                );

        DSXFlinkConfiguration configuration =
                new DSXFlinkConfiguration();

        configuration.configure(context);

        verify(env).setParallelism(4);

        verify(executionConfig)
                .enableObjectReuse();

        verify(env)
                .enableCheckpointing(
                        Duration.ofMinutes(1).toMillis()
                );

        verify(checkpointConfig)
                .setCheckpointTimeout(
                        Duration.ofMinutes(5).toMillis()
                );

        verify(checkpointConfig)
                .setMinPauseBetweenCheckpoints(
                        Duration.ofSeconds(30).toMillis()
                );

        verify(checkpointConfig)
                .setTolerableCheckpointFailureNumber(2);

        verify(checkpointConfig)
                .setCheckpointingMode(
                        org.apache.flink.streaming.api
                                .CheckpointingMode.EXACTLY_ONCE
                );

        verify(checkpointConfig)
                .setExternalizedCheckpointCleanup(
                        CheckpointConfig
                                .ExternalizedCheckpointCleanup
                                .RETAIN_ON_CANCELLATION
                );

        verify(checkpointConfig)
                .setCheckpointStorage(
                        startsWith("gs://test-bucket/")
                );
    }
}