package eu.unicredit.dpu.pipeline;

import eu.unicredit.dpu.pipeline.config.ConfigLoader;
import eu.unicredit.dpu.pipeline.config.EnvironmentConfig;
import eu.unicredit.dpu.pipeline.config.PipelineConfig;
import org.junit.jupiter.api.Test;

import static org.junit.jupiter.api.Assertions.*;

class ConfigLoaderTest {

    @Test
    void shouldLoadDefaultClasspathConfig() throws Exception {
        PipelineConfig config = new ConfigLoader().load();
        assertNotNull(config);
        assertNotNull(config.getEnvironments());
        assertFalse(config.getEnvironments().isEmpty());
    }

    @Test
    void shouldResolveAllFourEnvironments() throws Exception {
        PipelineConfig config = new ConfigLoader().load();
        for (String env : new String[]{"sit", "uat", "ppd", "prd"}) {
            EnvironmentConfig envConfig = config.getEnvironments().get(env);
            assertNotNull(envConfig, "Missing config for env: " + env);
            assertNotNull(envConfig.getKafka().getBootstrapServers());
            assertNotNull(envConfig.getKafka().getTopic());
            assertNotNull(envConfig.getKafka().getConsumerGroupId());
            assertNotNull(envConfig.getSchemaRegistry().getUrl());
            assertTrue(envConfig.getFlink().getParallelism() > 0);
        }
    }

    @Test
    void prdShouldHave12Partitions() throws Exception {
        PipelineConfig config = new ConfigLoader().load();
        assertEquals(12, config.getEnvironments().get("prd").getKafka().getPartitions());
    }

    @Test
    void sitUatPpdShouldHave3Partitions() throws Exception {
        PipelineConfig config = new ConfigLoader().load();
        for (String env : new String[]{"sit", "uat", "ppd"}) {
            assertEquals(3, config.getEnvironments().get(env).getKafka().getPartitions(),
                    "Expected 3 partitions for env: " + env);
        }
    }

    @Test
    void flinkParallelismShouldMatchPartitionCount() throws Exception {
        PipelineConfig config = new ConfigLoader().load();
        config.getEnvironments().forEach((env, cfg) -> assertEquals(
                cfg.getKafka().getPartitions(),
                cfg.getFlink().getParallelism(),
                "Flink parallelism must equal Kafka partitions for env: " + env));
    }

    @Test
    void shouldResolveActiveEnvironmentFromDefault() throws Exception {
        PipelineConfig config = new ConfigLoader().load();
        String active = config.resolveActiveEnvironment();
        assertNotNull(active);
        assertTrue(config.getEnvironments().containsKey(active),
                "Active env '" + active + "' not found in environments map");
    }
}
