package eu.unicredit.document.dxstraceinfo.validation;

import org.junit.jupiter.api.Test;

import javax.validation.ConstraintViolation;
import javax.validation.Path;
import javax.validation.Validator;
import java.lang.reflect.Constructor;
import java.util.LinkedHashSet;
import java.util.Set;

import static org.junit.jupiter.api.Assertions.assertDoesNotThrow;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertSame;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.mockito.Mockito.any;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

@SuppressWarnings("ALL")
class ObjectValidatorTest {

    @Test
    void shouldReturnSingletonInstance() {

        ObjectValidator first =
                ObjectValidator.getInstance();

        ObjectValidator second =
                ObjectValidator.getInstance();

        assertSame(first, second);
    }

    @Test
    void shouldThrowWhenObjectIsNull() {

        NullPointerException exception =
                assertThrows(
                        NullPointerException.class,
                        () -> ObjectValidator.getInstance()
                                .validate(null)
                );

        assertEquals(
                "object to validate cannot be null",
                exception.getMessage()
        );
    }

    @Test
    void shouldNotThrowWhenValidationSucceeds()
            throws Exception {

        Validator validator = mock(Validator.class);

        when(validator.validate(any()))
                .thenReturn(Set.of());

        ObjectValidator objectValidator =
                newValidator(validator);

        assertDoesNotThrow(
                () -> objectValidator.validate(
                        new TestBean()
                )
        );
    }

    @Test
    void shouldThrowValidationExceptionForPropertyViolation()
            throws Exception {

        Validator validator = mock(Validator.class);

        @SuppressWarnings("unchecked")
        ConstraintViolation<TestBean> violation =
                mock(ConstraintViolation.class);

        Path path = mock(Path.class);

        when(path.toString())
                .thenReturn("name");

        when(violation.getPropertyPath())
                .thenReturn(path);

        when(violation.getMessage())
                .thenReturn("must not be null");

        Set<ConstraintViolation<TestBean>> violations =
                new LinkedHashSet<>();

        violations.add(violation);

        when(validator.validate(any()))
                .thenReturn((Set) violations);

        ObjectValidator objectValidator =
                newValidator(validator);

        ObjectValidationException exception =
                assertThrows(
                        ObjectValidationException.class,
                        () -> objectValidator.validate(
                                new TestBean()
                        )
                );

        assertTrue(
                exception.getMessage()
                        .contains(
                                "name: must not be null"
                        )
        );
    }

    @Test
    void shouldUseRootWhenPropertyPathIsEmpty()
            throws Exception {

        Validator validator = mock(Validator.class);

        @SuppressWarnings("unchecked")
        ConstraintViolation<TestBean> violation =
                mock(ConstraintViolation.class);

        Path path = mock(Path.class);

        when(path.toString())
                .thenReturn("");

        when(violation.getPropertyPath())
                .thenReturn(path);

        when(violation.getMessage())
                .thenReturn("invalid object");

        Set<ConstraintViolation<TestBean>> violations =
                new LinkedHashSet<>();

        violations.add(violation);

        when(validator.validate(any()))
                .thenReturn((Set) violations);

        ObjectValidator objectValidator =
                newValidator(validator);

        ObjectValidationException exception =
                assertThrows(
                        ObjectValidationException.class,
                        () -> objectValidator.validate(
                                new TestBean()
                        )
                );

        assertTrue(
                exception.getMessage()
                        .contains(
                                "<root>: invalid object"
                        )
        );
    }

    @Test
    void shouldSortViolationsByPropertyName()
            throws Exception {

        Validator validator = mock(Validator.class);

        @SuppressWarnings("unchecked")
        ConstraintViolation<TestBean> a =
                mock(ConstraintViolation.class);

        @SuppressWarnings("unchecked")
        ConstraintViolation<TestBean> b =
                mock(ConstraintViolation.class);

        Path pathA = mock(Path.class);
        Path pathB = mock(Path.class);

        when(pathA.toString()).thenReturn("zField");
        when(pathB.toString()).thenReturn("aField");

        when(a.getPropertyPath()).thenReturn(pathA);
        when(b.getPropertyPath()).thenReturn(pathB);

        when(a.getMessage()).thenReturn("error-z");
        when(b.getMessage()).thenReturn("error-a");

        Set<ConstraintViolation<TestBean>> violations =
                new LinkedHashSet<>();

        violations.add(a);
        violations.add(b);

        when(validator.validate(any()))
                .thenReturn((Set) violations);

        ObjectValidator objectValidator =
                newValidator(validator);

        ObjectValidationException exception =
                assertThrows(
                        ObjectValidationException.class,
                        () -> objectValidator.validate(
                                new TestBean()
                        )
                );

        String message = exception.getMessage();

        assertTrue(
                message.indexOf("aField")
                        < message.indexOf("zField")
        );
    }

    private ObjectValidator newValidator(
            Validator validator)
            throws Exception {

        Constructor<ObjectValidator> constructor =
                ObjectValidator.class
                        .getDeclaredConstructor(
                                Validator.class
                        );

        constructor.setAccessible(true);

        return constructor.newInstance(
                validator
        );
    }

    private static class TestBean {
    }
}