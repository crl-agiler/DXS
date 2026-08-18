package eu.unicredit.document.dxstraceinfo.api;

import eu.unicredit.document.dxstraceinfo.config.AppCliArguments;
import eu.unicredit.document.dxstraceinfo.config.pojo.ArtifactInformation;
import eu.unicredit.document.dxstraceinfo.config.pojo.ConfigApp;
import eu.unicredit.document.dxstraceinfo.credentials.Credentials;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.junit.jupiter.api.Test;

import java.util.Optional;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertSame;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.mockito.Mockito.mock;

class DXSContextImplTest {

    @Test
    void shouldReturnConstructorDependencies() {

        StreamExecutionEnvironment env =
                mock(StreamExecutionEnvironment.class);

        ConfigApp config =
                mock(ConfigApp.class);

        AppCliArguments arguments =
                mock(AppCliArguments.class);

        Credentials credentials =
                mock(Credentials.class);

        ArtifactInformation artifactInformation =
                mock(ArtifactInformation.class);

        DXSContextImpl context =
                new DXSContextImpl(
                        env,
                        config,
                        arguments,
                        credentials,
                        artifactInformation
                );

        assertSame(env, context.streamingExecutionEnv());
        assertSame(config, context.config());
        assertSame(arguments, context.args());
        assertSame(credentials, context.credentials());
        assertSame(
                artifactInformation,
                context.artifactInformation()
        );
    }

    @Test
    void shouldAddAndRetrieveTypedContextProperty() {

        DXSContextImpl context =
                new DXSContextImpl(
                        mock(StreamExecutionEnvironment.class),
                        mock(ConfigApp.class),
                        mock(AppCliArguments.class),
                        mock(Credentials.class),
                        mock(ArtifactInformation.class)
                );

        context.add(
                "CatalogLoader",
                "iceberg-catalog"
        );

        Optional<String> result =
                context.get(
                        "CatalogLoader",
                        String.class
                );

        assertTrue(result.isPresent());
        assertEquals(
                "iceberg-catalog",
                result.orElseThrow()
        );
    }

    @Test
    void shouldAddAndRetrieveUntypedContextProperty() {

        DXSContextImpl context =
                new DXSContextImpl(
                        mock(StreamExecutionEnvironment.class),
                        mock(ConfigApp.class),
                        mock(AppCliArguments.class),
                        mock(Credentials.class),
                        mock(ArtifactInformation.class)
                );

        context.add(
                "BaseParallelism",
                8
        );

        Optional<Integer> result =
                context.get("BaseParallelism");

        assertTrue(result.isPresent());
        assertEquals(
                Integer.valueOf(8),
                result.orElseThrow()
        );
    }

    @Test
    void shouldReturnEmptyOptionalWhenTypedPropertyIsMissing() {

        DXSContextImpl context =
                new DXSContextImpl(
                        mock(StreamExecutionEnvironment.class),
                        mock(ConfigApp.class),
                        mock(AppCliArguments.class),
                        mock(Credentials.class),
                        mock(ArtifactInformation.class)
                );

        assertTrue(
                context.get(
                                "MissingProperty",
                                String.class
                        )
                        .isEmpty()
        );
    }

    @Test
    void shouldReturnEmptyOptionalWhenUntypedPropertyIsMissing() {

        DXSContextImpl context =
                new DXSContextImpl(
                        mock(StreamExecutionEnvironment.class),
                        mock(ConfigApp.class),
                        mock(AppCliArguments.class),
                        mock(Credentials.class),
                        mock(ArtifactInformation.class)
                );

        assertTrue(
                context.get("MissingProperty")
                        .isEmpty()
        );
    }

    @Test
    void shouldNotOverrideExistingContextProperty() {

        DXSContextImpl context =
                new DXSContextImpl(
                        mock(StreamExecutionEnvironment.class),
                        mock(ConfigApp.class),
                        mock(AppCliArguments.class),
                        mock(Credentials.class),
                        mock(ArtifactInformation.class)
                );

        context.add(
                "ErrorHandler",
                "default-handler"
        );

        context.add(
                "ErrorHandler",
                "test-handler"
        );

        assertEquals(
                "default-handler",
                context.get(
                                "ErrorHandler",
                                String.class
                        )
                        .orElseThrow()
        );
    }

    @Test
    void shouldThrowClassCastExceptionWhenTypeDoesNotMatch() {

        DXSContextImpl context =
                new DXSContextImpl(
                        mock(StreamExecutionEnvironment.class),
                        mock(ConfigApp.class),
                        mock(AppCliArguments.class),
                        mock(Credentials.class),
                        mock(ArtifactInformation.class)
                );

        context.add(
                "CheckpointInterval",
                "PT5M"
        );

        assertThrows(
                ClassCastException.class,
                () -> context.get(
                        "CheckpointInterval",
                        Integer.class
                ).orElseThrow()
        );
    }
}