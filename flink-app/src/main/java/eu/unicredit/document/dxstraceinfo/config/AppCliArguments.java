package eu.unicredit.document.dxstraceinfo.config;

import com.beust.jcommander.JCommander;
import com.beust.jcommander.Parameter;
import lombok.EqualsAndHashCode;
import lombok.Getter;
import lombok.ToString;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

@Getter
@ToString
@EqualsAndHashCode
public class AppCliArguments {

  private static final Logger LOG = LoggerFactory.getLogger(AppCliArguments.class);

  @Parameter(names = {"-b", "--bucketName"},
      description = "GCS bucket name", required = true)
  private String bucketName;

  @Parameter(names = {"-base-cfg", "--baseConfigPath"},
      description = "Path of the base configuration file inside the bucket", required = true)
  private String baseConfigPath;

  @Parameter(names = {"-env-cfg", "--envConfigPath"},
      description = "Path of the environment configuration file inside the bucket", required = true)
  private String envConfigPath;

  /**
   * Parse Cli arguments.
   */
  public static AppCliArguments parse(String[] args) {
    final AppCliArguments argv = new AppCliArguments();
    JCommander.newBuilder().addObject(argv).build().parse(args);
    LOG.info("Arguments read: [{}]", argv);
    return argv;
  }
}
