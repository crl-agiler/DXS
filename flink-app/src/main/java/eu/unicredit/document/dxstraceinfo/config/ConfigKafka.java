package eu.unicredit.document.dxstraceinfo.config;

import com.fasterxml.jackson.annotation.JsonClassDescription;
import com.fasterxml.jackson.annotation.JsonIgnore;
import com.fasterxml.jackson.annotation.JsonIgnoreProperties;
import com.fasterxml.jackson.annotation.JsonProperty;
import com.fasterxml.jackson.annotation.JsonPropertyDescription;
import com.fasterxml.jackson.annotation.JsonTypeName;
import eu.unicredit.document.dxstraceinfo.tools.YamlUtils;
import lombok.Builder;
import lombok.Singular;
import lombok.Value;
import lombok.extern.jackson.Jacksonized;

import javax.validation.Valid;
import javax.validation.constraints.AssertFalse;
import javax.validation.constraints.NotNull;
import java.io.Serializable;
import java.util.Map;

@Value
@Jacksonized
@Builder
@JsonIgnoreProperties(ignoreUnknown = true)
@JsonClassDescription("Configurations for all Kafka Sources")
@JsonTypeName(ConfigKafka.TYPE)
public class ConfigKafka implements Serializable {

  /**
   * This fields embeds the type name of the serialized entity. Add a version semantic if needed.
   * Note: use @Valid for fields with custom types.
   */
  @JsonIgnore
  public static final String TYPE = "kafka_config";

  @NotNull
  @Valid
  @JsonProperty(value = "bootstrapServers", required = true)
  @JsonPropertyDescription("Kafka Bootstrap Server URL(s)")
  String bootstrapServers;

  @NotNull
  @Valid
  @JsonProperty(value = "topic", required = true)
  @JsonPropertyDescription("Kafka Topic Name")
  String topic;

  @NotNull
  @Valid
  @JsonProperty(value = "consumerGroupId", required = true)
  @JsonPropertyDescription("Kafka Consumer Group Id")
  String consumerGroupId;

  @NotNull
  @Valid
  @JsonProperty(value = "certDir", required = true)
  @JsonPropertyDescription("Certifications directory for the SSL connectivity of the Kafka Consumer")
  String certDir;

  @Singular
  @Valid
  @JsonProperty("otherKafkaProperties")
  @JsonPropertyDescription("Other Kafka Properties")
  Map<String, String> otherKafkaProperties;


  @Builder.Default
  @JsonProperty(value = "startingOffset", defaultValue = "committed_or_earliest")
  String startingOffset = "committed_or_earliest";


  @Builder.Default
  @Valid
  @JsonProperty(value = "secure", defaultValue = "true")
  @JsonPropertyDescription("Perform a secure connection to the kafka cluster")
  boolean secure = true;


  @AssertFalse(message = "auto.offset.reset property is overridden by flink, DO NOT DEFINE!")
  public boolean isAutoOffsetResetDefined() {
    return otherKafkaProperties.get("auto.offset.reset") != null;
  }

  @Override
  public String toString() {
    return YamlUtils.toYaml(this);
  }
}
