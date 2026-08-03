package eu.unicredit.document.dxstraceinfo.credentials;

import eu.unicredit.document.dxstraceinfo.tools.GcpUtils;
import eu.unicredit.document.dxstraceinfo.tools.JsonUtils;
import eu.unicredit.document.dxstraceinfo.tools.ParsingException;
import java.io.IOException;

public class GcpCredentialsRetriever implements CredentialsRetriever {

  @Override
  public Credentials getCredentials(String projectId, String secretId)
      throws IOException, MalformedCredentialsException {
    final String secretJson = GcpUtils.getSecret(projectId, secretId, "latest");
    try {
      return JsonUtils.fromJson(secretJson, Credentials.class);
    } catch (ParsingException e) {
      throw new MalformedCredentialsException("Could not deserialize secret value properly." +
          "Secret payload must be a valid json with keys `username` and `password`.", e);
    }
  }
}
