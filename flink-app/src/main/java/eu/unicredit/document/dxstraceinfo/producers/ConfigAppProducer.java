package eu.unicredit.document.dxstraceinfo.producers;

import eu.unicredit.document.dxstraceinfo.config.AppCliArguments;
import eu.unicredit.document.dxstraceinfo.config.ConfigApp;
import eu.unicredit.document.dxstraceinfo.config.ConfigAppRetriever;
import eu.unicredit.document.dxstraceinfo.config.GcpConfigAppRetriever;
import eu.unicredit.document.dxstraceinfo.validation.ObjectValidator;
import org.apache.commons.lang3.StringUtils;

import javax.enterprise.inject.Produces;
import javax.inject.Inject;
import javax.inject.Singleton;
import java.util.Objects;

/**
 * Produces the complete, merged and validated application configuration.
 */
@Singleton
public final class ConfigAppProducer {

    private final AppCliArguments appCliArguments;
    private final ObjectValidator objectValidator;

    @Inject
    public ConfigAppProducer(
            AppCliArguments appCliArguments,
            ObjectValidator objectValidator) {

        this.appCliArguments = Objects.requireNonNull(
                appCliArguments,
                "appCliArguments cannot be null"
        );

        this.objectValidator = Objects.requireNonNull(
                objectValidator,
                "objectValidator cannot be null"
        );
    }

    /**
     * Loads the base and environment configurations from GCS,
     * merges them, validates the result and exposes it as a CDI bean.
     *
     * @return complete and validated application configuration
     */
    @Produces
    @Singleton
    public ConfigApp produceConfigApp() {
        String bucket = requireNotBlank(
                appCliArguments.getBucketName(),
                "appCliArguments.bucket"
        );

        String baseConfigPath = requireNotBlank(
                appCliArguments.getBaseConfigPath(),
                "appCliArguments.baseConfigPath"
        );

        String envConfigPath = requireNotBlank(
                appCliArguments.getEnvConfigPath(),
                "appCliArguments.envConfigPath"
        );

        ConfigAppRetriever retriever =
                GcpConfigAppRetriever.of(
                        objectValidator,
                        bucket,
                        baseConfigPath,
                        envConfigPath
                );

        return Objects.requireNonNull(
                retriever.getConfig(),
                "ConfigAppRetriever returned null"
        );
    }

    private static String requireNotBlank(
            String value,
            String propertyName) {

        if (StringUtils.isBlank(value)) {
            throw new IllegalStateException(
                    propertyName + " cannot be null, empty or blank"
            );
        }

        return value;
    }
}