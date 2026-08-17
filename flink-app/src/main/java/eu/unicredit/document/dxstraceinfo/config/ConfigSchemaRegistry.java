package eu.unicredit.document.dxstraceinfo.config;

import com.fasterxml.jackson.annotation.JsonClassDescription;
import com.fasterxml.jackson.annotation.JsonIgnore;
import com.fasterxml.jackson.annotation.JsonIgnoreProperties;
import com.fasterxml.jackson.annotation.JsonProperty;
import com.fasterxml.jackson.annotation.JsonPropertyDescription;
import com.fasterxml.jackson.annotation.JsonTypeName;
import eu.unicredit.document.dxstraceinfo.tools.YamlUtils;
import lombok.Builder;
import lombok.Value;
import lombok.extern.jackson.Jacksonized;

import javax.validation.Valid;
import javax.validation.constraints.NotNull;
import java.io.Serializable;

// lombok annotations
@Value
@Jacksonized
@Builder
// jackson annotations
@JsonIgnoreProperties(ignoreUnknown = true)
@JsonClassDescription("Configurations for Confluent Schema Registry")
@JsonTypeName(ConfigSchemaRegistry.TYPE)
public class ConfigSchemaRegistry implements Serializable {

  /**
   * This fields embeds the type name of the serialized entity. Add a version semantic if needed.
   * Note: use @Valid for fields with custom types.
   */
  @JsonIgnore
  public static final String TYPE = "confluent-schema-registry_config";

  @NotNull
  @Valid
  @JsonProperty(value = "secretId", required = true)
  @JsonPropertyDescription("GCP Secret ID for this Confluent Schema Registry")
  String secretId;

  @NotNull
  @Valid
  @JsonProperty(value = "url", required = true)
  @JsonPropertyDescription("Confluent Schema Registry URL(s)")
  String url;

  @NotNull
  @Valid
  @JsonProperty(value = "certDir", required = true)
  @JsonPropertyDescription("Certifications directory for the SSL connectivity of the Schema Registry")
  String certDir;

  @Builder.Default
  @Valid
  @JsonProperty(value = "secure", defaultValue = "true")
  @JsonPropertyDescription("Perform a secure connection to the schema registry")
  boolean secure = true;

  @Builder.Default
  @Valid
  @JsonProperty(value = "disable", defaultValue = "false")
  @JsonPropertyDescription("If true the schema registry integration will not be used and topics will be" +
      " considered as not SOE encoded, avro schemas should be configured per each topic")
  boolean disable = false;

  @Override
  public String toString() {
    return YamlUtils.toYaml(this);
  }
}
