package eu.unicredit.document.dxstraceinfo.tools;

import com.google.cloud.secretmanager.v1.AccessSecretVersionResponse;
import com.google.cloud.secretmanager.v1.SecretManagerServiceClient;
import com.google.cloud.secretmanager.v1.SecretManagerServiceSettings;
import com.google.cloud.secretmanager.v1.SecretVersionName;
import java.io.IOException;

public class GcpUtils {

  private GcpUtils() {
  }

  /**
   * Get a secret from the GCP Secret Manager. <br>
   * <a href="https://cloud.google.com/secret-manager/docs/reference/libraries">Ref libraries</a> <br>
   */
  public static String getSecret(String projectId, String secretId, String secretVersion) throws IOException {
    // Configuring REST transport instead of gRPC because proxy does not support it when making requests
    // from the VDI. In Dataproc we should prefer gRPC.
    SecretManagerServiceSettings settings = SecretManagerServiceSettings.newHttpJsonBuilder().build();
    try (SecretManagerServiceClient client = SecretManagerServiceClient.create(settings)) {
      SecretVersionName secretName = SecretVersionName.of(projectId, secretId, secretVersion);
      AccessSecretVersionResponse secret = client.accessSecretVersion(secretName);
      return secret.getPayload().getData().toStringUtf8(); // extract secret as string
    }
  }
}
