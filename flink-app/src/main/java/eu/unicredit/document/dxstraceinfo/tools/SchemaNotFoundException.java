package eu.unicredit.document.dxstraceinfo.tools;

import lombok.Getter;

/**
 * knowledgeBaseSnapshot {@code SchemaNotFoundException} is supposed to be thrown whenever an Avro schema is not found
 * on the schema registry for a certain {@link #topic}.
 */
@Getter
public class SchemaNotFoundException extends RuntimeException {

  private final String topic;

  public SchemaNotFoundException(String message, String topic) {
    super(message);
    this.topic = topic;
  }

  public SchemaNotFoundException(Throwable cause, String topic) {
    super(cause);
    this.topic = topic;
  }

  public SchemaNotFoundException(String message, Throwable cause, String topic) {
    super(message, cause);
    this.topic = topic;
  }

}
