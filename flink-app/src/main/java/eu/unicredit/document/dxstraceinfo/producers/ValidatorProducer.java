package eu.unicredit.document.dxstraceinfo.producers;

import javax.annotation.PreDestroy;
import javax.enterprise.context.ApplicationScoped;
import javax.enterprise.inject.Produces;
import javax.validation.Validation;
import javax.validation.Validator;
import javax.validation.ValidatorFactory;

@ApplicationScoped
public class ValidatorProducer {

    private final ValidatorFactory validatorFactory;
    private final Validator validator;

    public ValidatorProducer() {
        this.validatorFactory =
                Validation.buildDefaultValidatorFactory();

        this.validator =
                validatorFactory.getValidator();
    }

    @Produces
    @ApplicationScoped
    public Validator validator() {
        return validator;
    }

    @PreDestroy
    void close() {
        validatorFactory.close();
    }

}