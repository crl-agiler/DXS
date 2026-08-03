package eu.unicredit.document.dxstraceinfo.credentials;

import com.fasterxml.jackson.annotation.JsonIgnoreProperties;
import com.fasterxml.jackson.annotation.JsonProperty;
import java.io.Serializable;
import javax.validation.constraints.NotNull;
import lombok.Builder;
import lombok.Value;
import lombok.extern.jackson.Jacksonized;

// lombok annotations
@Value
@Jacksonized
@Builder()
// jackson annotations
@JsonIgnoreProperties(ignoreUnknown = true)
public class Credentials implements Serializable {

  @NotNull
  @JsonProperty(value = "username", required = true)
  String username;

  @NotNull
  @JsonProperty(value = "password", required = true)
  String password;
}
