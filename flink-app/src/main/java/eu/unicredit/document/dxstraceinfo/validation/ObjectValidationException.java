package eu.unicredit.document.dxstraceinfo.validation;

public class ObjectValidationException extends RuntimeException {

    public ObjectValidationException(String message) {
        super(message);
    }

    public ObjectValidationException(
            String message,
            Throwable cause) {

        super(message, cause);
    }
}