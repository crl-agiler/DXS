package eu.unicredit.document.dxstraceinfo.it;

import eu.unicredit.document.dxstraceinfo.App;
import eu.unicredit.document.dxstraceinfo.api.AdditionalContextProperty;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;

public class TestAdditionalContextProperties {

    public static class Builder {

        private final Map<String, AdditionalContextProperty>
                additionalContextProperties =
                new LinkedHashMap<>();

        public Builder fromApplicationAdditionalContextProperties() {

            App.defaults().forEach(
                    property -> additionalContextProperties.put(
                            property.name(),
                            property
                    )
            );

            return this;
        }

        public Builder overrideAdditionalContextProperties(
                AdditionalContextProperty... overrides) {

            Arrays.stream(overrides)
                    .forEach(property ->
                            additionalContextProperties.put(
                                    property.name(),
                                    property
                            ));

            return this;
        }

        public List<AdditionalContextProperty> build() {
            return new ArrayList<>(
                    additionalContextProperties.values()
            );
        }
    }

    public static Builder builder() {
        return new Builder();
    }
}