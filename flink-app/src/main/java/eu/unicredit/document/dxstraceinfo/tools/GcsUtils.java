package eu.unicredit.document.dxstraceinfo.tools;

import com.google.cloud.storage.Blob;
import com.google.cloud.storage.BlobId;
import com.google.cloud.storage.Storage;
import com.google.cloud.storage.StorageOptions;

import java.nio.charset.StandardCharsets;
import java.nio.file.Path;

public class GcsUtils {

  private GcsUtils() {
  }

  /**
   * Download a file from GCS to a local file system path.
   */
  public static void downloadFileFromGcsToLocalFs(String bucketName, String gcsFileName, Path destinationPath) {
    getBlob(bucketName, gcsFileName).downloadTo(destinationPath);
  }

  /**
   * Download a file from GCS as a UTF-8 String.
   */
  public static String downloadFileFromGcsAsUtf8String(String bucketName, String gcsFileName) {
    return new String(getBlob(bucketName, gcsFileName).getContent(), StandardCharsets.UTF_8);
  }

  private static Blob getBlob(String bucketName, String gcsFileName) {
    // Initialize a GCS client
    Storage storage = StorageOptions.getDefaultInstance().getService();

    // Get the blob object for the file
    Blob blob = storage.get(BlobId.of(bucketName, gcsFileName));
    if (blob == null) {
      throw new RuntimeException("File not found in GCS: " + gcsFileName);
    }
    return blob;
  }
}
