package eu.unicredit.dpu.pipeline.config;

import lombok.Data;

import java.util.Map;

/**
 * Root POJO for the pipeline-config.yaml file.
 *
 * <pre>
 * environment: sit
 * environments:
 *   sit: ...
 *   uat: ...
 *   ppd: ...
 *   prd: ...
 * pipeline:
 *   watermarkOutOfOrdernessMs: 30000
 *   csvExportTime: "07:30"
 *   schemaSubject: "DossierTraceinfoEvent"
 * </pre>
 */
@Data
public class PipelineConfig {

    /**
     * Active environment name — can be overridden at runtime
     * via the PIPELINE_ENV environment variable.
     */
    private String environment;

    /** Per-environment configuration blocks. */
    private Map<String, EnvironmentConfig> environments;

    /** Environment-independent pipeline settings. */
    private PipelineSettings pipeline;

    /**
     * Returns the {@link EnvironmentConfig} for the active environment.
     *
     * @throws IllegalArgumentException if the active environment key is not found.
     */
    public EnvironmentConfig activeEnvironment() {
        String env = resolveActiveEnvironment();
        EnvironmentConfig cfg = environments.get(env);
        if (cfg == null) {
            throw new IllegalArgumentException(
                    "No configuration found for environment '" + env +
                    "'. Valid values: " + environments.keySet());
        }
        return cfg;
    }

    /**
     * Resolves the active environment name, giving precedence to the
     * PIPELINE_ENV environment variable over the YAML default.
     */
    public String resolveActiveEnvironment() {
        String envOverride = System.getenv("PIPELINE_ENV");
        return (envOverride != null && !envOverride.isBlank()) ? envOverride.trim().toLowerCase() : environment;
    }

    // ── Inner class ──────────────────────────────────────────────
    @Data
    public static class PipelineSettings {

        /** Watermark out-of-orderness tolerance in milliseconds. */
        private long watermarkOutOfOrdernessMs = 30000L;

        /** Time of day for the CSV delta export (HH:mm). */
        private String csvExportTime = "07:30";

        /** Schema Registry subject name for the DXS Avro schema. */
        private String schemaSubject = "DossierTraceinfoEvent";
    }
}
