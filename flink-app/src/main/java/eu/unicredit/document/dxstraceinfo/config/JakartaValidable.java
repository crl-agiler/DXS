package eu.unicredit.document.dxstraceinfo.config;

import com.fasterxml.jackson.annotation.JsonIgnore;
import java.util.Locale;
import java.util.Set;
import javax.validation.ConstraintViolation;
import javax.validation.Validation;
import javax.validation.ValidationException;
import javax.validation.Validator;
import org.jooq.lambda.Seq;

public interface JakartaValidable {

  /**
   * Validate the Jakarta Validation Constraints of this object.
   */
  @JsonIgnore
  void validate() throws ValidationException;

  class ValidatorProxy {
    private static final Validator validator;

    static {
      final Locale defaultLocale = Locale.getDefault();
      Locale.setDefault(Locale.ENGLISH);
      validator = Validation.buildDefaultValidatorFactory().getValidator();
      Locale.setDefault(defaultLocale);
    }

    private ValidatorProxy() {
    }

    /**
     * Validate the Jakarta Validation Constraints of this object.
     */
    public static <T> void validate(T obj) throws ValidationException {
      final Set<ConstraintViolation<T>> violations = ValidatorProxy.validator.validate(obj);

      if (!violations.isEmpty()) {
        final String validationErrorStr = "[" + Seq.seq(violations).map(violation ->
                String.format("%s: %s", violation.getPropertyPath(), violation.getMessage()))
            .reduce((a, b) -> a + ", " + b).orElse("no errors") + "]";
        throw new ValidationException(String.format(
            "Validation errors for dto: \n```\n%s\n```\nErrors: %s" +
                "\n(java field name may be different from the json field name, look at the printed dto)",
            obj, validationErrorStr
        ));
      }
    }
  }
}
