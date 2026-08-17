package eu.unicredit.document.dxstraceinfo.credentials;

import java.io.IOException;

public class MockCredentialsRetriever implements CredentialsRetriever {

  /**
   * Emulate a Secret Manager.
   *
   * @param params first param should be the projectId, the second the secretId
   */
  @Override
  public Credentials getCredentials(String... params)
      throws IOException, MalformedCredentialsException {
    String projectId = params[0], secretId = params[1];
    final String[] split = secretId.split(",");
    String username = split[0];
    String password = split.length > 1 ? split[1] : "";
    return Credentials.builder().username(username).password(password).build();
  }
}
