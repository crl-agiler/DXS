package eu.unicredit.dpu.pipeline.schema;

import eu.unicredit.dpu.pipeline.config.EnvironmentConfig;
import eu.unicredit.dpu.pipeline.kafka.KafkaPropertiesBuilder;
import io.confluent.kafka.schemaregistry.client.CachedSchemaRegistryClient;
import io.confluent.kafka.schemaregistry.client.SchemaRegistryClient;
import io.confluent.kafka.schemaregistry.client.rest.exceptions.RestClientException;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.util.Collection;
import java.util.HashMap;
import java.util.Map;
import java.util.Properties;

/**
 * Factory for a {@link SchemaRegistryClient} configured for the active
 * UniCredit CKF Confluent Schema Registry instance.
 *
 * <p>The client uses an in-memory cache (capacity: 100 schemas) to avoid
 * repeated HTTP calls to the registry on each deserialization.
 */
public class SchemaRegistryClientFactory {

    private static final Logger LOG = LoggerFactory.getLogger(SchemaRegistryClientFactory.class);

    private static final int IDENTITY_MAP_CAPACITY = 100;

    private final EnvironmentConfig envConfig;

    public SchemaRegistryClientFactory(EnvironmentConfig envConfig) {
        this.envConfig = envConfig;
    }

    // ── Public API ────────────────────────────────────────────────

    /**
     * Creates and returns a configured {@link SchemaRegistryClient}.
     */
    public SchemaRegistryClient create() {
        String url = envConfig.getSchemaRegistry().getUrl();
        LOG.info("Creating Schema Registry client — url: {}", url);

        Properties srProps = new KafkaPropertiesBuilder(envConfig).buildSchemaRegistryProperties();
        Map<String, Object> configs = propertiesToMap(srProps);

        return new CachedSchemaRegistryClient(url, IDENTITY_MAP_CAPACITY, configs);
    }

    /**
     * Creates the client and performs a connectivity health check by
     * listing the registered subjects.
     *
     * @throws IOException        if the Registry cannot be reached
     * @throws RestClientException if the Registry returns an error response
     */
    public SchemaRegistryClient createAndVerify() throws IOException, RestClientException {
        SchemaRegistryClient client = create();
        LOG.info("Verifying Schema Registry connectivity...");

        Collection<String> subjects = client.getAllSubjects();
        LOG.info("Schema Registry is reachable — {} subject(s) registered.", subjects.size());

        String subject = "DossierTraceinfoEvent-value";
        if (subjects.contains(subject)) {
            LOG.info("DXS subject '{}' is present in the registry.", subject);
        } else {
            LOG.warn("DXS subject '{}' was NOT found in the registry. " +
                     "Schema may not have been registered yet.", subject);
        }

        return client;
    }

    // ── Private helpers ───────────────────────────────────────────

    private Map<String, Object> propertiesToMap(Properties props) {
        Map<String, Object> map = new HashMap<>();
        props.forEach((k, v) -> map.put(k.toString(), v));
        return map;
    }
}
