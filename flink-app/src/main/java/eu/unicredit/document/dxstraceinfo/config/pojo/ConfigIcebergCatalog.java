package eu.unicredit.document.dxstraceinfo.config.pojo;

import com.fasterxml.jackson.annotation.JsonClassDescription;
import com.fasterxml.jackson.annotation.JsonIgnore;
import com.fasterxml.jackson.annotation.JsonIgnoreProperties;
import com.fasterxml.jackson.annotation.JsonProperty;
import com.fasterxml.jackson.annotation.JsonPropertyDescription;
import com.fasterxml.jackson.annotation.JsonTypeName;
import lombok.Builder;
import lombok.Value;
import lombok.extern.jackson.Jacksonized;

import javax.validation.Valid;
import javax.validation.constraints.NotNull;
import java.io.Serializable;

@Value
@Jacksonized
@Builder
// jackson annotations
@JsonIgnoreProperties(ignoreUnknown = true)
@JsonClassDescription("Configurations for Iceberg catalogs")
@JsonTypeName(ConfigIcebergCatalog.TYPE)
public class ConfigIcebergCatalog implements Serializable {

  /**
   * This fields embeds the type name of the serialized entity. Add a version semantic if needed.
   * Note: use @Valid for fields with custom types.
   */
  @JsonIgnore
  public static final String TYPE = "iceberg_config";

  @NotNull
  @Valid
  @JsonProperty(value = "catalogLocation", required = true)
  @JsonPropertyDescription("Catalog location (GCS folder URL where the catalog is saved)")
  String catalogLocation;

  @NotNull
  @Valid
  @JsonProperty(value = "catalogName", required = true)
  @JsonPropertyDescription("Catalog name for Iceberg tables")
  String catalogName;

}
