package eu.unicredit.document.dxstraceinfo.config;

import eu.unicredit.document.dxstraceinfo.tools.JavaUtils;
import lombok.Builder;
import lombok.NonNull;
import org.junit.platform.commons.util.StringUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

@Builder
public class MockConfigAppRetriever
    implements ConfigAppRetriever {

  private static final Logger LOG =
      LoggerFactory.getLogger(
          MockConfigAppRetriever.class);

  @NonNull
  private final String schemaRegistryUrl;

  @NonNull
  private final String catalogLocation;

  @NonNull
  private final String kafkaBootstrapServers;

  @Override
  public ConfigApp getConfig(
      String bucket,
      String baseConfigPath,
      String envConfigPath) {

    final String pathPrefix =
        StringUtils.isNotBlank(bucket) ?
            bucket + "/"
            : "";


    LOG.info("bucket={}", bucket);
    LOG.info("baseConfigPath={}", baseConfigPath);
    LOG.info("envConfigPath={}", envConfigPath);
    LOG.info("resolved base={}", pathPrefix + baseConfigPath);
    LOG.info("resolved env={}", pathPrefix + envConfigPath);
    LOG.info(
        "base resource={}",
        MockConfigAppRetriever.class
            .getClassLoader()
            .getResource(pathPrefix + baseConfigPath));

    LOG.info(
        "env resource={}",
        MockConfigAppRetriever.class
            .getClassLoader()
            .getResource(pathPrefix + envConfigPath));


    final String baseConfigYaml =
        JavaUtils.readStringUtf8FromResources(
            pathPrefix + baseConfigPath);

    final String envConfigYaml =
        JavaUtils.readStringUtf8FromResources(
                pathPrefix + envConfigPath)

            .replace(
                "{{schemaRegistryUrl}}",
                schemaRegistryUrl)
            .replace(
                "{{kafkaBootstrapServers}}",
                kafkaBootstrapServers)
            .replace("{{catalogLocation}}",
                catalogLocation);

    LOG.info(
        "Environment config:\n{}",
        envConfigYaml);


    return ConfigAppRetriever
        .parseAndValidateYaml(
            bucket,
            baseConfigYaml,
            envConfigYaml);
  }
}
