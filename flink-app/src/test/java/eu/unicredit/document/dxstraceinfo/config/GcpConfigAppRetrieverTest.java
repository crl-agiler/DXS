package eu.unicredit.document.dxstraceinfo.config;

import eu.unicredit.document.dxstraceinfo.config.pojo.ConfigApp;
import eu.unicredit.document.dxstraceinfo.tools.GcsUtils;
import eu.unicredit.document.dxstraceinfo.tools.YamlUtils;
import eu.unicredit.document.dxstraceinfo.validation.ObjectValidator;
import org.junit.jupiter.api.Test;
import org.mockito.MockedStatic;
import org.mockito.Mockito;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertSame;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.mockStatic;

class GcpConfigAppRetrieverTest {

    private final GcpConfigAppRetriever retriever = new GcpConfigAppRetriever();

    @Test
    void shouldThrowExceptionWhenParamsAreNull() {
        IllegalArgumentException ex = assertThrows(
                IllegalArgumentException.class,
                () -> retriever.getConfig((String[]) null)
        );

        assertEquals(
                "Expected 3 parameters [bucket, baseConfigPath, envConfigPath]",
                ex.getMessage()
        );
    }

    @Test
    void shouldThrowExceptionWhenLessThanThreeParamsAreProvided() {
        IllegalArgumentException ex = assertThrows(
                IllegalArgumentException.class,
                () -> retriever.getConfig("bucket", "config.yaml")
        );

        assertEquals(
                "Expected 3 parameters [bucket, baseConfigPath, envConfigPath]",
                ex.getMessage()
        );
    }

    @Test
    void shouldLoadAndValidateConfiguration() throws Exception {

        ConfigApp expectedConfig = Mockito.mock(ConfigApp.class);

        try (MockedStatic<GcsUtils> gcsMock = mockStatic(GcsUtils.class);
             MockedStatic<YamlUtils> yamlMock = mockStatic(YamlUtils.class);
             MockedStatic<ObjectValidator> validatorMock = mockStatic(ObjectValidator.class)) {

            ObjectValidator validator = mock(ObjectValidator.class);

            gcsMock.when(() ->
                            GcsUtils.downloadFileFromGcsAsUtf8String("bucket", "base.yaml"))
                    .thenReturn("baseYaml");

            gcsMock.when(() ->
                            GcsUtils.downloadFileFromGcsAsUtf8String("bucket", "env.yaml"))
                    .thenReturn("envYaml");

            yamlMock.when(() ->
                            YamlUtils.merge(
                                    "baseYaml",
                                    "envYaml",
                                    ConfigApp.class))
                    .thenReturn(expectedConfig);

            validatorMock.when(ObjectValidator::getInstance)
                    .thenReturn(validator);

            ConfigApp result = retriever.getConfig(
                    "bucket",
                    "base.yaml",
                    "env.yaml"
            );

            assertSame(expectedConfig, result);

            Mockito.verify(validator).validate(expectedConfig);
        }
    }
}