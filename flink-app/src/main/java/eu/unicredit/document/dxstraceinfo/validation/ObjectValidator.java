package eu.unicredit.document.dxstraceinfo.validation;

import javax.validation.ConstraintViolation;
import javax.validation.Validation;
import javax.validation.Validator;
import java.util.Comparator;
import java.util.Objects;
import java.util.Set;
import java.util.stream.Collectors;

public final class ObjectValidator {

    private static final ObjectValidator INSTANCE = new ObjectValidator(Validation.buildDefaultValidatorFactory().getValidator());

    private final Validator validator;

    private ObjectValidator(Validator validator) {
        this.validator = Objects.requireNonNull(
                validator,
                "validator cannot be null"
        );
    }

    public static ObjectValidator getInstance() {
        return INSTANCE;
    }

    /**
     * Validates the supplied object using Bean Validation constraints.
     *
     * @param object object to validate
     * @param <T> object type
     * @throws ObjectValidationException if validation fails
     */
    public <T> void validate(T object) {
        Objects.requireNonNull(
                object,
                "object to validate cannot be null"
        );

        Set<ConstraintViolation<T>> violations =
                validator.validate(object);

        if (violations.isEmpty()) {
            return;
        }

        String errors = violations.stream()
                .sorted(Comparator.comparing(
                        violation -> violation.getPropertyPath().toString()
                ))
                .map(ObjectValidator::formatViolation)
                .collect(Collectors.joining(
                        System.lineSeparator() + " - ",
                        " - ",
                        ""
                ));

        throw new ObjectValidationException(
                "Validation failed for "
                        + object.getClass().getName()
                        + ":"
                        + System.lineSeparator()
                        + errors
        );
    }

    private static String formatViolation(
            ConstraintViolation<?> violation) {

        String propertyPath =
                violation.getPropertyPath().toString();

        if (propertyPath.isEmpty()) {
            propertyPath = "<root>";
        }

        return propertyPath
                + ": "
                + violation.getMessage();
    }
}