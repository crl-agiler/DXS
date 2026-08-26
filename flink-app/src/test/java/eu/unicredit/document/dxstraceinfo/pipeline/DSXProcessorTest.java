package eu.unicredit.document.dxstraceinfo.pipeline;

import eu.unicredit.document.dxstraceinfo.api.DXSContext;
import eu.unicredit.document.dxstraceinfo.api.ErrorHandler;
import eu.unicredit.document.dxstraceinfo.avro.DossierTraceinfoEvent;
import eu.unicredit.document.dxstraceinfo.transform.SplitContext;
import eu.unicredit.document.dxstraceinfo.transform.SplitTransformLogic;
import org.apache.flink.api.java.functions.KeySelector;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.datastream.KeyedStream;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.table.data.RowData;
import org.junit.jupiter.api.Test;
import org.mockito.ArgumentMatchers;

import java.util.List;
import java.util.Optional;

import static org.junit.jupiter.api.Assertions.assertDoesNotThrow;
import static org.junit.jupiter.api.Assertions.assertSame;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.mockito.Mockito.any;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

class DSXProcessorTest {

    @Test
    void shouldInitializeProcessor() {

        DSXProcessor processor = new DSXProcessor();

        assertDoesNotThrow(processor::onInit);
    }

    @Test
    @SuppressWarnings({"rawtypes", "unchecked"})
    void shouldProcessStream() {

        DXSContext context = mock(DXSContext.class);

        ErrorHandler<DossierTraceinfoEvent> errorHandler =
                mock(ErrorHandler.class);

        List<SplitContext<?>> splitContexts =
                List.of(mock(SplitContext.class));

        DataStream<DossierTraceinfoEvent> input =
                mock(DataStream.class);

        KeyedStream<DossierTraceinfoEvent, Long>
                keyed =
                mock(KeyedStream.class);

        SingleOutputStreamOperator<RowData>
                transformed =
                mock(SingleOutputStreamOperator.class);

        SingleOutputStreamOperator<RowData>
                finalStream =
                mock(SingleOutputStreamOperator.class);

        when(context.get("ErrorHandler"))
                .thenReturn(Optional.of(errorHandler));

        when(context.get("SplitContextList"))
                .thenReturn(Optional.of(splitContexts));

        when(input.keyBy(any(org.apache.flink.api.java.functions.KeySelector.class)))
        .thenReturn(keyed);

        when(keyed.process(
                ArgumentMatchers.any(SplitTransformLogic.class)))
                .thenReturn(transformed);

        when(transformed.name("SplitTransformLogic"))
                .thenReturn(transformed);

        when(transformed.uid("transform-transform-logic"))
                .thenReturn(finalStream);

        DSXProcessor processor =
                new DSXProcessor();

        DataStream<RowData> result =
                processor.process(
                        input,
                        context
                );

        assertSame(finalStream, result);

        verify(context)
                .get("ErrorHandler");

        verify(context)
                .get("SplitContextList");

        verify(input)
                .keyBy(any(KeySelector.class));

        verify(keyed)
                .process(
                        any(SplitTransformLogic.class)
                );

        verify(transformed)
                .name("SplitTransformLogic");

        verify(transformed)
                .uid("transform-transform-logic");
    }

    @Test
    void shouldThrowWhenErrorHandlerIsMissing() {

        DXSContext context = mock(DXSContext.class);

        when(context.get("ErrorHandler"))
                .thenReturn(Optional.empty());

        DSXProcessor processor =
                new DSXProcessor();

        assertThrows(
                java.util.NoSuchElementException.class,
                () -> processor.process(
                        mock(DataStream.class),
                        context
                )
        );
    }

    @Test
    void shouldThrowWhenSplitContextListIsMissing() {

        DXSContext context = mock(DXSContext.class);

        when(context.get("ErrorHandler"))
                .thenReturn(Optional.of(mock(ErrorHandler.class)));

        when(context.get("SplitContextList"))
                .thenReturn(Optional.empty());

        DSXProcessor processor =
                new DSXProcessor();

        assertThrows(
                java.util.NoSuchElementException.class,
                () -> processor.process(
                        mock(DataStream.class),
                        context
                )
        );
    }
}