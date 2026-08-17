package eu.unicredit.document.dxstraceinfo.context;

import eu.unicredit.document.dxstraceinfo.api.DXSContext;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;
import org.mockito.Mock;
import org.mockito.Mockito;

import static org.junit.jupiter.api.Assertions.*;

class DiscardOutputTagPropertyTest {

    @Test
    void name() {
        DiscardOutputTagProperty discardOutputTagProperty = new DiscardOutputTagProperty();
        Assertions.assertNotNull(discardOutputTagProperty.name());
    }

    @Test
    void instance() {
        DiscardOutputTagProperty discardOutputTagProperty = new DiscardOutputTagProperty();
        Assertions.assertNotNull(discardOutputTagProperty.instance(Mockito.mock(DXSContext.class)));
    }
}