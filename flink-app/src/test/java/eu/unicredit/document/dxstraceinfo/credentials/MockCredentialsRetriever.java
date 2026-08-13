package eu.unicredit.document.dxstraceinfo.credentials;

import javax.annotation.Priority;
import javax.enterprise.context.ApplicationScoped;
import java.io.IOException;

@ApplicationScoped
@Priority(999)
public class MockCredentialsRetriever implements CredentialsRetriever {

  /**
   * Emulate a Secret Manager.
   *
   * @param projectId ignored
   * @param secretId  put the desired username,password here
   */
  @Override
  public Credentials getCredentials(String projectId, String secretId)
      throws IOException, MalformedCredentialsException {
    final String[] split = secretId.split(",");
    String username = split[0];
    String password = split.length > 1 ? split[1] : "";
    return Credentials.builder().username(username).password(password).build();
  }
}
