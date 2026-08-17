package eu.unicredit.document.dxstraceinfo.config;

import eu.unicredit.document.dxstraceinfo.api.ConfigAppRetriever;
import eu.unicredit.document.dxstraceinfo.tools.GcsUtils;
import eu.unicredit.document.dxstraceinfo.tools.ParsingException;
import eu.unicredit.document.dxstraceinfo.tools.YamlUtils;
import eu.unicredit.document.dxstraceinfo.validation.ObjectValidator;
import org.apache.commons.lang3.ArrayUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import javax.validation.ValidationException;

public class GcpConfigAppRetriever implements ConfigAppRetriever {

  private static final Logger LOG = LoggerFactory.getLogger(GcpConfigAppRetriever.class);

    @Override
    public ConfigApp getConfig(String... params) throws ParsingException, ValidationException {
      if (params == null || params.length < 3) {
        throw new IllegalArgumentException(
                "Expected 3 parameters [bucket, baseConfigPath, envConfigPath]"
        );
      }
      LOG.info("Received params: {}", (Object) params);
      String bucket = params[0];
      String baseConfigPath = params[1];
      String envConfigPath = params[2];
      final String baseAppConfigAsYaml = GcsUtils.downloadFileFromGcsAsUtf8String(bucket, baseConfigPath);
      LOG.info("Base config read from GCS: \n```\n{}\n```\n", baseAppConfigAsYaml);
      final String envAppConfigAsYaml = GcsUtils.downloadFileFromGcsAsUtf8String(bucket, envConfigPath);
      LOG.info("Environment config read from GCS: \n```\n{}\n```\n", envAppConfigAsYaml);
      ConfigApp configApp = YamlUtils.merge(baseAppConfigAsYaml, envAppConfigAsYaml, ConfigApp.class);
      ObjectValidator.getInstance().validate(configApp);
      return configApp;
  }

}
