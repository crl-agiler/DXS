package eu.unicredit.dpu.pipeline.config;

import lombok.Data;

/**
 * Confluent Schema Registry configuration for a single environment.
 */
@Data
public class SchemaRegistryConfig {

    /** Schema Registry base URL (e.g. http://ckfdevlsr01.internal.unicreditgroup.eu:8081). */
    private String url;
}
