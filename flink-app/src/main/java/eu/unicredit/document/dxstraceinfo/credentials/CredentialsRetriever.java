package eu.unicredit.document.dxstraceinfo.credentials;

import java.io.IOException;

public interface CredentialsRetriever {

  /**
   * Retrieve Credentials from the Secret Manager (i.e. GCP Secret Manager)
   */
  Credentials getCredentials(String projectId, String secretId) throws IOException, MalformedCredentialsException;

}
