package eu.unicredit.dpu.pipeline.config;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.dataformat.yaml.YAMLFactory;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.io.InputStream;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;

/**
 * Loads and validates the pipeline configuration from a YAML file.
 *
 * <p>Resolution order for the config file path:
 * <ol>
 *   <li>CLI argument {@code --config /path/to/file.yaml}</li>
 *   <li>Environment variable {@code PIPELINE_CONFIG_PATH}</li>
 *   <li>Classpath resource {@code pipeline-config.yaml} (default)</li>
 * </ol>
 *
 * <p>The active environment is resolved by {@link PipelineConfig#resolveActiveEnvironment()},
 * which checks the {@code PIPELINE_ENV} environment variable before falling back to
 * the {@code environment} field in the YAML.
 */
public class ConfigLoader {

    private static final Logger LOG = LoggerFactory.getLogger(ConfigLoader.class);

    private static final String DEFAULT_CLASSPATH_RESOURCE = "pipeline-config.yaml";
    private static final String ENV_VAR_CONFIG_PATH        = "PIPELINE_CONFIG_PATH";

    private final ObjectMapper yamlMapper;

    public ConfigLoader() {
        this.yamlMapper = new ObjectMapper(new YAMLFactory());
    }

    // ── Public API ───────────────────────────────────────────────

    /**
     * Loads the configuration using the default resolution order.
     */
    public PipelineConfig load() throws IOException {
        return load(null);
    }

    /**
     * Loads the configuration from the given path (may be null to trigger
     * the default resolution order).
     *
     * @param explicitPath explicit file path passed via CLI {@code --config}; may be null
     */
    public PipelineConfig load(String explicitPath) throws IOException {
        PipelineConfig config;

        if (explicitPath != null && !explicitPath.isBlank()) {
            config = loadFromFile(Paths.get(explicitPath));
        } else {
            String envPath = System.getenv(ENV_VAR_CONFIG_PATH);
            if (envPath != null && !envPath.isBlank()) {
                config = loadFromFile(Paths.get(envPath));
            } else {
                config = loadFromClasspath(DEFAULT_CLASSPATH_RESOURCE);
            }
        }

        validate(config);
        LOG.info("Configuration loaded — active environment: '{}'",
                config.resolveActiveEnvironment());
        return config;
    }

    // ── Private helpers ──────────────────────────────────────────

    private PipelineConfig loadFromFile(Path path) throws IOException {
        LOG.info("Loading config from file: {}", path.toAbsolutePath());
        if (!Files.exists(path)) {
            throw new IOException("Config file not found: " + path.toAbsolutePath());
        }
        try (InputStream is = Files.newInputStream(path)) {
            return yamlMapper.readValue(is, PipelineConfig.class);
        }
    }

    private PipelineConfig loadFromClasspath(String resource) throws IOException {
        LOG.info("Loading config from classpath resource: {}", resource);
        InputStream is = Thread.currentThread()
                               .getContextClassLoader()
                               .getResourceAsStream(resource);
        if (is == null) {
            throw new IOException("Classpath resource not found: " + resource);
        }
        try (is) {
            return yamlMapper.readValue(is, PipelineConfig.class);
        }
    }

    private void validate(PipelineConfig config) {
        if (config.getEnvironments() == null || config.getEnvironments().isEmpty()) {
            throw new IllegalStateException("Config is missing 'environments' block.");
        }

        String activeEnv = config.resolveActiveEnvironment();
        EnvironmentConfig envCfg = config.getEnvironments().get(activeEnv);
        if (envCfg == null) {
            throw new IllegalStateException(
                    "No environment config found for '" + activeEnv + "'. " +
                    "Available: " + config.getEnvironments().keySet());
        }

        KafkaConfig kafka = envCfg.getKafka();
        if (kafka == null || kafka.getBootstrapServers() == null || kafka.getBootstrapServers().isBlank()) {
            throw new IllegalStateException("Kafka bootstrapServers is required for env '" + activeEnv + "'.");
        }
        if (kafka.getTopic() == null || kafka.getTopic().isBlank()) {
            throw new IllegalStateException("Kafka topic is required for env '" + activeEnv + "'.");
        }
        if (kafka.getConsumerGroupId() == null || kafka.getConsumerGroupId().isBlank()) {
            throw new IllegalStateException("Kafka consumerGroupId is required for env '" + activeEnv + "'.");
        }

        SchemaRegistryConfig sr = envCfg.getSchemaRegistry();
        if (sr == null || sr.getUrl() == null || sr.getUrl().isBlank()) {
            throw new IllegalStateException("SchemaRegistry URL is required for env '" + activeEnv + "'.");
        }

        LOG.debug("Config validation passed for environment '{}'.", activeEnv);
    }
}
