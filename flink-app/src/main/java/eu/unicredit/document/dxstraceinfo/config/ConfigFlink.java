package eu.unicredit.document.dxstraceinfo.config;

import static org.apache.flink.runtime.jobgraph.tasks.CheckpointCoordinatorConfiguration.MINIMAL_CHECKPOINT_TIME;

import com.fasterxml.jackson.annotation.JsonClassDescription;
import com.fasterxml.jackson.annotation.JsonIgnore;
import com.fasterxml.jackson.annotation.JsonIgnoreProperties;
import com.fasterxml.jackson.annotation.JsonProperty;
import com.fasterxml.jackson.annotation.JsonPropertyDescription;
import com.fasterxml.jackson.annotation.JsonTypeName;
import eu.unicredit.document.dxstraceinfo.App;
import eu.unicredit.document.dxstraceinfo.tools.YamlUtils;
import java.io.Serializable;
import java.time.Duration;
import javax.validation.Valid;
import javax.validation.constraints.NotNull;
import lombok.Builder;
import lombok.Value;
import lombok.extern.jackson.Jacksonized;

@Value
@Jacksonized
@Builder
@JsonIgnoreProperties(ignoreUnknown = true)
@JsonClassDescription("Apache Flink configuration")
@JsonTypeName(ConfigFlink.TYPE)
public class ConfigFlink implements Serializable {

  @JsonIgnore
  public static final String TYPE = "flink_config";

  @Builder.Default
  @Valid
  @JsonProperty(value = "jobName", defaultValue = App.DP_APP_NAME)
  @JsonPropertyDescription("Flink Job Name")
  String jobName = App.DP_APP_NAME;

  @NotNull
  @Valid
  @JsonProperty(value = "parallelism", required = true)
  @JsonPropertyDescription(
      "Job parallelism. Should normally match the number of Kafka partitions.")
  Integer parallelism;

  @NotNull
  @Valid
  @JsonProperty(value = "checkpointInterval", required = true)
  @JsonPropertyDescription("Checkpoint Interval. If < " + MINIMAL_CHECKPOINT_TIME + "ms, is disabled.")
  Duration checkpointInterval;

  @Builder.Default
  @Valid
  @JsonProperty(value = "checkpointTimeout", defaultValue = "PT1M")
  @JsonPropertyDescription("Maximum time a checkpoint may take before being discarded")
  Duration checkpointTimeout = Duration.ofMinutes(1);

  @Builder.Default
  @Valid
  @JsonProperty(value = "minPauseBetweenCheckpoints", defaultValue = "PT10S")
  @JsonPropertyDescription("Minimum pause between two consecutive checkpoints")
  Duration minPauseBetweenCheckpoints = Duration.ofSeconds(10);

  @Builder.Default
  @Valid
  @JsonProperty(value = "stateBackend", defaultValue = "rocksdb")
  @JsonPropertyDescription("State backend implementation: rocksdb or hashmap")
  String stateBackend = "rocksdb";


  @Override
  public String toString() {
    return YamlUtils.toYaml(this);
  }
}
