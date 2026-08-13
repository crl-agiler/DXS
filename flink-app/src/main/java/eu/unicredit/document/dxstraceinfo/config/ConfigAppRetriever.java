package eu.unicredit.document.dxstraceinfo.config;

import eu.unicredit.document.dxstraceinfo.tools.ParsingException;

import javax.validation.ValidationException;

public interface ConfigAppRetriever {

  /**
   * Retrieve App Config from a File System (i.e. GCP Cloud Storage)
   */
  ConfigApp getConfig() throws ParsingException, ValidationException;

}
