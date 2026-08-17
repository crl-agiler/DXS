package eu.unicredit.document.dxstraceinfo.config;

import eu.unicredit.document.dxstraceinfo.tools.GcsUtils;
import eu.unicredit.document.dxstraceinfo.tools.YamlUtils;
import eu.unicredit.document.dxstraceinfo.validation.ObjectValidator;
import org.junit.jupiter.api.Test;
import org.mockito.MockedStatic;

import static org.junit.jupiter.api.Assertions.assertSame;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.mockStatic;
import static org.mockito.Mockito.verify;

class GcpConfigAppRetrieverTest {

    @Test
    void shouldReadMergeValidateAndReturnConfig()
            throws Exception {

        String bucket = "bucket";
        String basePath = "base.yml";
        String envPath = "prod.yml";

        String baseYaml = "base-config";
        String envYaml = "env-config";

        ConfigApp expectedConfig = mock(ConfigApp.class);
        ObjectValidator validator = mock(ObjectValidator.class);

        try (MockedStatic<GcsUtils> gcsMock =
                     mockStatic(GcsUtils.class);
             MockedStatic<YamlUtils> yamlMock =
                     mockStatic(YamlUtils.class);
             MockedStatic<ObjectValidator> validatorMock =
                     mockStatic(ObjectValidator.class)) {

            gcsMock.when(() ->
                            GcsUtils.downloadFileFromGcsAsUtf8String(
                                    bucket,
                                    basePath))
                    .thenReturn(baseYaml);

            gcsMock.when(() ->
                            GcsUtils.downloadFileFromGcsAsUtf8String(
                                    bucket,
                                    envPath))
                    .thenReturn(envYaml);

            yamlMock.when(() ->
                            YamlUtils.merge(
                                    baseYaml,
                                    envYaml,
                                    ConfigApp.class))
                    .thenReturn(expectedConfig);

            validatorMock.when(ObjectValidator::getInstance)
                    .thenReturn(validator);

            GcpConfigAppRetriever retriever =
                    new GcpConfigAppRetriever();

            ConfigApp result =
                    retriever.getConfig(
                            bucket,
                            basePath,
                            envPath);

            assertSame(expectedConfig, result);

            gcsMock.verify(() ->
                    GcsUtils.downloadFileFromGcsAsUtf8String(
                            bucket,
                            basePath));

            gcsMock.verify(() ->
                    GcsUtils.downloadFileFromGcsAsUtf8String(
                            bucket,
                            envPath));

            yamlMock.verify(() ->
                    YamlUtils.merge(
                            baseYaml,
                            envYaml,
                            ConfigApp.class));

            verify(validator).validate(expectedConfig);
        }
    }
}
