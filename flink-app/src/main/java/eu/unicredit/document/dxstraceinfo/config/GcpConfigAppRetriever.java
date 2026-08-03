package eu.unicredit.document.dxstraceinfo.config;

import eu.unicredit.document.dxstraceinfo.tools.GcsUtils;
import eu.unicredit.document.dxstraceinfo.tools.ParsingException;
import javax.validation.ValidationException;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class GcpConfigAppRetriever implements ConfigAppRetriever {

  private static final Logger LOG = LoggerFactory.getLogger(GcpConfigAppRetriever.class);

  @Override
  public ConfigApp getConfig(
      String bucket, String baseConfigPath, String envConfigPath
  ) throws ParsingException, ValidationException {
    final String baseAppConfigAsYaml = GcsUtils.downloadFileFromGcsAsUtf8String(bucket, baseConfigPath);
    LOG.info("Base config read from GCS: \n```\n{}\n```\n", baseAppConfigAsYaml);
    final String envAppConfigAsYaml = GcsUtils.downloadFileFromGcsAsUtf8String(bucket, envConfigPath);
    LOG.info("Environment config read from GCS: \n```\n{}\n```\n", envAppConfigAsYaml);

    return ConfigAppRetriever.parseAndValidateYaml(bucket, baseAppConfigAsYaml, envAppConfigAsYaml);
  }

}
