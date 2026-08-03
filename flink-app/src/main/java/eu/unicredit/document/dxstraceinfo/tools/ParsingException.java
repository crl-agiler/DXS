package eu.unicredit.document.dxstraceinfo.tools;

public class ParsingException extends RuntimeException {
  public ParsingException(String message, Throwable cause) {
    super(message, cause);
  }

  public ParsingException(String message) {
    super(message);
  }
}
