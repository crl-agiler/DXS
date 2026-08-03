package eu.unicredit.document.dxstraceinfo.config;

import com.fasterxml.jackson.annotation.JsonClassDescription;
import com.fasterxml.jackson.annotation.JsonIgnore;
import com.fasterxml.jackson.annotation.JsonIgnoreProperties;
import com.fasterxml.jackson.annotation.JsonProperty;
import com.fasterxml.jackson.annotation.JsonPropertyDescription;
import com.fasterxml.jackson.annotation.JsonTypeName;
import eu.unicredit.document.dxstraceinfo.tools.YamlUtils;
import java.io.Serializable;
import javax.validation.Valid;
import javax.validation.constraints.NotEmpty;
import lombok.Builder;
import lombok.Value;
import lombok.extern.jackson.Jacksonized;

// lombok annotations
@Value
@Jacksonized
@Builder
// jackson annotations
@JsonIgnoreProperties(ignoreUnknown = true)
@JsonClassDescription("Configurations for a source table")
@JsonTypeName(ConfigPlaceholder.TYPE)
public class ConfigPlaceholder implements Serializable {

  /**
   * This fields embeds the type name of the serialized entity. Add a version semantic if needed.
   * Note: use @Valid for fields with custom types.
   */
  @JsonIgnore
  public static final String TYPE = "placeholder_config";

  @NotEmpty
  @Valid
  @JsonProperty(value = "name", required = true)
  @JsonPropertyDescription("Placeholder name")
  String name;

  @Valid
  @NotEmpty
  @JsonProperty(value = "value", required = true)
  @JsonPropertyDescription("Placeholder value")
  String value;

  @Override
  public String toString() {
    return YamlUtils.toYaml(this);
  }
}
