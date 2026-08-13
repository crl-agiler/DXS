package eu.unicredit.document.dxstraceinfo.config;

import eu.unicredit.document.dxstraceinfo.validation.ObjectValidator;

import javax.enterprise.context.ApplicationScoped;
import javax.inject.Inject;
import java.util.Objects;

@ApplicationScoped
public class ConfigAppRetrieverFactory {

    private final ObjectValidator objectValidator;

    @Inject
    public ConfigAppRetrieverFactory(
            ObjectValidator objectValidator) {
        this.objectValidator = Objects.requireNonNull(
                objectValidator,
                "objectValidator cannot be null"
        );
    }

    public ConfigAppRetriever create(
            String bucket,
            String baseConfigPath,
            String envConfigPath) {

        return GcpConfigAppRetriever.of(
                objectValidator,
                bucket,
                baseConfigPath,
                envConfigPath
        );
    }
}