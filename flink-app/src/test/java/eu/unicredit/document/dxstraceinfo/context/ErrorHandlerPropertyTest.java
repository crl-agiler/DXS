package eu.unicredit.document.dxstraceinfo.context;

import eu.unicredit.document.dxstraceinfo.api.DXSContext;
import eu.unicredit.document.dxstraceinfo.api.ErrorHandler;
import eu.unicredit.document.dxstraceinfo.avro.DossierTraceinfoEvent;
import eu.unicredit.document.dxstraceinfo.handler.FlinkSimpleErrorHandler;
import org.apache.flink.table.data.RowData;
import org.apache.flink.util.OutputTag;
import org.junit.jupiter.api.Test;

import java.util.NoSuchElementException;
import java.util.Optional;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

class ErrorHandlerPropertyTest {

    @Test
    void shouldReturnExpectedName() {

        ErrorHandlerProperty property =
                new ErrorHandlerProperty();

        assertEquals(
                ErrorHandlerProperty.ERROR_HANDLER,
                property.name()
        );
    }

    @Test
    void shouldCreateFlinkSimpleErrorHandler() {

        DXSContext context = mock(DXSContext.class);

        OutputTag<RowData> discardOutputTag =
                new OutputTag<RowData>("discard") {
                };

        when(context.<OutputTag<RowData>>get("DiscardOutputTag"))
                .thenReturn(Optional.of(discardOutputTag));

        ErrorHandlerProperty property =
                new ErrorHandlerProperty();

        ErrorHandler<DossierTraceinfoEvent> handler =
                property.instance(context);

        assertNotNull(handler);
        assertTrue(
                handler instanceof FlinkSimpleErrorHandler
        );

        verify(context)
                .get("DiscardOutputTag");
    }

    @Test
    void shouldThrowWhenDiscardOutputTagIsMissing() {

        DXSContext context = mock(DXSContext.class);

        when(context.<OutputTag<RowData>>get("DiscardOutputTag"))
                .thenReturn(Optional.empty());

        ErrorHandlerProperty property =
                new ErrorHandlerProperty();

        assertThrows(
                NoSuchElementException.class,
                () -> property.instance(context)
        );

        verify(context)
                .get("DiscardOutputTag");
    }
}