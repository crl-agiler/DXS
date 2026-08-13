package eu.unicredit.document.dxstraceinfo.config;

import eu.unicredit.document.dxstraceinfo.tools.GcsUtils;
import eu.unicredit.document.dxstraceinfo.tools.ParsingException;
import javax.validation.ValidationException;

import eu.unicredit.document.dxstraceinfo.tools.YamlUtils;
import eu.unicredit.document.dxstraceinfo.validation.ObjectValidator;
import lombok.RequiredArgsConstructor;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

@RequiredArgsConstructor(staticName = "of")
public class GcpConfigAppRetriever implements ConfigAppRetriever {

  private static final Logger LOG = LoggerFactory.getLogger(GcpConfigAppRetriever.class);
  private final ObjectValidator objectValidator;
  private final String bucket;
  private final String baseConfigPath;
  private final String envConfigPath;

  @Override
  public ConfigApp getConfig() throws ParsingException, ValidationException {
    final String baseAppConfigAsYaml = GcsUtils.downloadFileFromGcsAsUtf8String(bucket, baseConfigPath);
    LOG.info("Base config read from GCS: \n```\n{}\n```\n", baseAppConfigAsYaml);
    final String envAppConfigAsYaml = GcsUtils.downloadFileFromGcsAsUtf8String(bucket, envConfigPath);
    LOG.info("Environment config read from GCS: \n```\n{}\n```\n", envAppConfigAsYaml);
    ConfigApp configApp = YamlUtils.merge(baseAppConfigAsYaml, envAppConfigAsYaml, ConfigApp.class);
    objectValidator.validate(configApp);
    return configApp;
  }

}
