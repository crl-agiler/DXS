package eu.unicredit.document.dxstraceinfo;

import eu.unicredit.document.dxstraceinfo.api.AdditionalContextProperty;
import eu.unicredit.document.dxstraceinfo.api.ConfigAppRetriever;
import eu.unicredit.document.dxstraceinfo.api.DXSContext;
import eu.unicredit.document.dxstraceinfo.api.FlinkConfiguration;
import eu.unicredit.document.dxstraceinfo.api.Pipeline;
import eu.unicredit.document.dxstraceinfo.config.AppCliArguments;
import eu.unicredit.document.dxstraceinfo.config.ConfigApp;
import eu.unicredit.document.dxstraceinfo.config.ConfigSchemaRegistry;
import eu.unicredit.document.dxstraceinfo.credentials.Credentials;
import eu.unicredit.document.dxstraceinfo.credentials.CredentialsRetriever;
import eu.unicredit.document.dxstraceinfo.parser.ArgsParser;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.junit.jupiter.api.Test;
import org.mockito.MockedStatic;

import java.util.List;

import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.Mockito.*;

class DXSApplicationTest {

    @Test
    void shouldCreateBuilder() {
        assertNotNull(DXSApplication.builder());
    }

    @Test
    void shouldExecutePipeline() throws Exception {

        Pipeline pipeline = mock(Pipeline.class);
        DXSContext context = mock(DXSContext.class);

        DXSApplication application =
                new DXSApplication(pipeline, context);

        application.execute();

        verify(pipeline).run(context);
    }

    @Test
    void shouldBootstrapApplication() throws Exception {

        Pipeline pipeline = mock(Pipeline.class);
        ArgsParser argsParser = mock(ArgsParser.class);
        ConfigAppRetriever configRetriever = mock(ConfigAppRetriever.class);
        CredentialsRetriever credentialsRetriever = mock(CredentialsRetriever.class);
        FlinkConfiguration flinkConfiguration = mock(FlinkConfiguration.class);
        AdditionalContextProperty property = mock(AdditionalContextProperty.class);

        AppCliArguments cliArguments = mock(AppCliArguments.class);
        ConfigApp config = mock(ConfigApp.class);
        ConfigSchemaRegistry schemaRegistryConfig = mock(ConfigSchemaRegistry.class);
        Credentials credentials = mock(Credentials.class);

        StreamExecutionEnvironment env =
                mock(StreamExecutionEnvironment.class);

        when(argsParser.parse(any()))
                .thenReturn(cliArguments);

        when(cliArguments.getBucketName())
                .thenReturn("bucket");

        when(cliArguments.getBaseConfigPath())
                .thenReturn("base");

        when(cliArguments.getEnvConfigPath())
                .thenReturn("env");

        when(configRetriever.getConfig(
                "bucket",
                "base",
                "env"))
                .thenReturn(config);

        when(config.getProjectId())
                .thenReturn("test-project");

        when(config.getSchemaRegistryConfig())
                .thenReturn(schemaRegistryConfig);

        when(schemaRegistryConfig.getSecretId())
                .thenReturn("secret-id");

        when(credentialsRetriever.getCredentials(
                "test-project",
                "secret-id"))
                .thenReturn(credentials);

        when(property.name())
                .thenReturn("CatalogLoader");

        when(property.instance(any()))
                .thenReturn("catalog-loader");

        try (MockedStatic<StreamExecutionEnvironment> mocked =
                     mockStatic(StreamExecutionEnvironment.class)) {

            mocked.when(StreamExecutionEnvironment::getExecutionEnvironment)
                    .thenReturn(env);

            DXSApplication application =
                    DXSApplication.builder()
                            .pipeline(pipeline)
                            .argsParser(argsParser)
                            .configAppRetriever(configRetriever)
                            .credentialsRetriever(credentialsRetriever)
                            .flinkConfiguration(flinkConfiguration)
                            .additionalContextProperties(List.of(property))
                            .bootstrap(
                                    new String[]{
                                            "--env",
                                            "TEST"
                                    }
                            );

            assertNotNull(application);

            verify(argsParser).parse(any());

            verify(configRetriever)
                    .getConfig(
                            "bucket",
                            "base",
                            "env"
                    );

            verify(credentialsRetriever)
                    .getCredentials(
                            "test-project",
                            "secret-id"
                    );

            verify(flinkConfiguration)
                    .configure(any());

            verify(property).name();
            verify(property).instance(any());
        }
    }

    @Test
    void shouldSetContext() {

        DXSContext context = mock(DXSContext.class);

        DXSApplication.DXSApplicationBuilder builder =
                DXSApplication.builder()
                        .context(context);

        assertNotNull(builder);
    }

    @Test
    void shouldReturnNonEmptyToString() {

        String value =
                DXSApplication.builder()
                        .toString();

        assertNotNull(value);
        assertFalse(value.isEmpty());
    }
}