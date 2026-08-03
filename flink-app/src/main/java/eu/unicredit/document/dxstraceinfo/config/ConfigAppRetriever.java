package eu.unicredit.document.dxstraceinfo.config;

import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.JsonNode;
import eu.unicredit.document.dxstraceinfo.tools.JsonUtils;
import eu.unicredit.document.dxstraceinfo.tools.ParsingException;
import eu.unicredit.document.dxstraceinfo.tools.YamlUtils;
import javax.validation.ValidationException;

public interface ConfigAppRetriever {

  /**
   * Parse and validate appConfig represented as YAML string.
   */
  static ConfigApp parseAndValidateYaml(
      String bucket, String baseAppConfigAsYaml, String envAppConfigAsYaml
  ) throws ParsingException, ValidationException {
    final ConfigApp appConfig = ConfigAppRetriever.merge(baseAppConfigAsYaml, envAppConfigAsYaml);
    appConfig.validate();
    return appConfig
        .toBuilder()
        .bucketName(bucket)
        .build();

  }

  /**
   * Merge two yaml.
   * - ARRAYS with elements containing "name" field will be merged like this:
   * For each child array element, select the corresponding parent array element by "name",
   * and merge them (child overwrite parent). If not matched, add child array element nonetheless.
   * - For any parent array that is also present in child,
   * elements that are not present in child array are never selected.
   * - For any parent array that is not present in child, all parent array elements are always added.
   */
  static ConfigApp merge(String parent, String child) {
    // Convert both POJOs to JsonNode
    JsonNode parentNode = YamlUtils.fromYaml(parent, JsonNode.class);
    JsonNode childNode = YamlUtils.fromYaml(child, JsonNode.class);

    // Merge JsonNodes
    JsonNode mergedNode = JsonUtils.mergeJsonNodesWithArrayMergeByName(parentNode, childNode);

    // Convert the merged JsonNode back to a POJO
    try {
      return YamlUtils.mapper.treeToValue(mergedNode, ConfigApp.class);
    } catch (JsonProcessingException e) {
      throw new ParsingException("Cannot convert from JsonNode to ConfigApp: \n```\n" + mergedNode + "\n```\n", e);
    }
  }


  /**
   * Retrieve App Config from a File System (i.e. GCP Cloud Storage)
   */
  ConfigApp getConfig(
      String bucket, String baseConfigPath, String envConfigPath) throws ParsingException, ValidationException;

}
