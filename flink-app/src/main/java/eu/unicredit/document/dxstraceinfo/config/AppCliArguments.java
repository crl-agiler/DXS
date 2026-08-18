package eu.unicredit.document.dxstraceinfo.config;

import com.beust.jcommander.Parameter;
import lombok.Data;

@Data
public final class AppCliArguments {

  @Parameter(names = {"-b", "--bucketName"},
      description = "GCS bucket name", required = true)
  private String bucketName;

  @Parameter(names = {"-base-cfg", "--baseConfigPath"},
      description = "Path of the base configuration file inside the bucket", required = true)
  private String baseConfigPath;

  @Parameter(names = {"-env-cfg", "--envConfigPath"},
      description = "Path of the environment configuration file inside the bucket", required = true)
  private String envConfigPath;

}
