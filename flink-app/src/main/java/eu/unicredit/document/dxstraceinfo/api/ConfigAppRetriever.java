package eu.unicredit.document.dxstraceinfo.api;

import eu.unicredit.document.dxstraceinfo.config.pojo.ConfigApp;
import eu.unicredit.document.dxstraceinfo.tools.ParsingException;

import javax.validation.ValidationException;

public interface ConfigAppRetriever {

  ConfigApp getConfig(String... params) throws ParsingException, ValidationException;
}
