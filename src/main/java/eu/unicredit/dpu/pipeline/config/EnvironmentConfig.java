package eu.unicredit.dpu.pipeline.config;

import lombok.Data;

/**
 * Full configuration for a single deployment environment (SIT / UAT / PPD / PRD).
 */
@Data
public class EnvironmentConfig {

    private KafkaConfig kafka;
    private SchemaRegistryConfig schemaRegistry;
    private FlinkConfig flink;
}
