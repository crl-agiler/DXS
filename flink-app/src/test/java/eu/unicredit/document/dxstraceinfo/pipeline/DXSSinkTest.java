package eu.unicredit.document.dxstraceinfo.pipeline;

import eu.unicredit.document.dxstraceinfo.api.DXSContext;
import eu.unicredit.document.dxstraceinfo.context.CatalogLoaderProperty;
import eu.unicredit.document.dxstraceinfo.context.DiscardOutputTagProperty;
import eu.unicredit.document.dxstraceinfo.context.SplitContextListProperty;
import eu.unicredit.document.dxstraceinfo.transform.SplitContext;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.table.data.RowData;
import org.apache.flink.util.OutputTag;
import org.apache.iceberg.catalog.TableIdentifier;
import org.apache.iceberg.flink.CatalogLoader;
import org.apache.iceberg.flink.TableLoader;
import org.apache.iceberg.flink.sink.FlinkSink;
import org.junit.jupiter.api.Test;
import org.mockito.MockedStatic;

import java.util.List;
import java.util.NoSuchElementException;
import java.util.Optional;

import static org.junit.Assert.assertThrows;
import static org.junit.jupiter.api.Assertions.assertDoesNotThrow;
import static org.mockito.Mockito.RETURNS_DEEP_STUBS;
import static org.mockito.Mockito.any;
import static org.mockito.Mockito.anyBoolean;
import static org.mockito.Mockito.anyInt;
import static org.mockito.Mockito.anyList;
import static org.mockito.Mockito.anyString;
import static org.mockito.Mockito.atLeastOnce;
import static org.mockito.Mockito.eq;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.mockStatic;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

class DXSSinkTest {

    @Test
    void shouldInitialize() {

        DXSSink sink = new DXSSink();

        assertDoesNotThrow(sink::onInit);
    }

    @Test
    @SuppressWarnings({"rawtypes", "unchecked"})
    void shouldSinkData() {

        DXSContext context =
                mock(DXSContext.class, RETURNS_DEEP_STUBS);

        when(context.config()
                .getFlinkConfig()
                .getBaseParallelism())
                .thenReturn(2);

        SingleOutputStreamOperator<RowData> stream =
                mock(SingleOutputStreamOperator.class);

        OutputTag<RowData> discardTag =
                new OutputTag<RowData>("discard") {
                };

        OutputTag<RowData> splitTag =
                new OutputTag<RowData>("customer") {
                };

        SplitContext splitContext =
                mock(SplitContext.class);

        CatalogLoader catalogLoader =
                mock(CatalogLoader.class);

        TableLoader tableLoader =
                mock(TableLoader.class);

        DataStream<RowData> sideOutput =
                mock(DataStream.class);

        DataStream<RowData> discardOutput =
                mock(DataStream.class);

        FlinkSink.Builder flinkBuilder =
                mock(FlinkSink.Builder.class);

        when(context.get(
                DiscardOutputTagProperty.DISCARD_OUTPUT_TAG))
                .thenReturn(Optional.of(discardTag));

        when(context.get(
                SplitContextListProperty.SPLIT_CONTEXT_LIST))
                .thenReturn(Optional.of(List.of(splitContext)));

        when(context.get(
                CatalogLoaderProperty.CATALOG_LOADER))
                .thenReturn(Optional.of(catalogLoader));

        when(splitContext.getOutputTag())
                .thenReturn(splitTag);

        when(splitContext.getTableLoader())
                .thenReturn(tableLoader);

        when(splitContext.getEqualityField())
                .thenReturn(List.of("id"));

        when(context.config()
                .getFlinkConfig()
                .getBaseParallelism())
                .thenReturn(2);

        when(stream.getSideOutput(splitTag))
                .thenReturn(sideOutput);

        when(stream.getSideOutput(discardTag))
                .thenReturn(discardOutput);

        try (MockedStatic<FlinkSink> flinkSink =
                     mockStatic(FlinkSink.class);

             MockedStatic<TableLoader> tableLoaderStatic =
                     mockStatic(TableLoader.class)) {

            TableLoader discardLoader =
                    mock(TableLoader.class);

            tableLoaderStatic.when(() ->
                            TableLoader.fromCatalog(
                                    eq(catalogLoader),
                                    any(TableIdentifier.class)))
                    .thenReturn(discardLoader);

            flinkSink.when(() ->
                            FlinkSink.forRowData(any()))
                    .thenReturn(flinkBuilder);

            when(flinkBuilder.tableLoader(any()))
                    .thenReturn(flinkBuilder);

            when(flinkBuilder.upsert(anyBoolean()))
                    .thenReturn(flinkBuilder);

            when(flinkBuilder.equalityFieldColumns(anyList()))
                    .thenReturn(flinkBuilder);

            when(flinkBuilder.writeParallelism(anyInt()))
                    .thenReturn(flinkBuilder);

            when(flinkBuilder.uidPrefix(anyString()))
                    .thenReturn(flinkBuilder);

            DXSSink sink = new DXSSink();

            sink.sink(stream, context);

            verify(stream)
                    .getSideOutput(splitTag);

            verify(stream)
                    .getSideOutput(discardTag);

            verify(flinkBuilder, atLeastOnce())
                    .append();
        }
    }

    @Test
    void shouldThrowWhenSplitContextsMissing() {

        DXSContext context = mock(DXSContext.class);

        when(context.get(
                DiscardOutputTagProperty.DISCARD_OUTPUT_TAG))
                .thenReturn(Optional.of(
                        new OutputTag<RowData>("discard") {
                        }));

        when(context.get(
                SplitContextListProperty.SPLIT_CONTEXT_LIST))
                .thenReturn(Optional.empty());

        DXSSink sink = new DXSSink();

        assertThrows(
                NoSuchElementException.class,
                () -> sink.sink(
                        mock(SingleOutputStreamOperator.class),
                        context
                )
        );
    }

    @Test
    void shouldThrowWhenCatalogLoaderMissing() {

        DXSContext context = mock(DXSContext.class);

        when(context.get(
                DiscardOutputTagProperty.DISCARD_OUTPUT_TAG))
                .thenReturn(Optional.of(
                        new OutputTag<RowData>("discard") {
                        }));

        when(context.get(
                SplitContextListProperty.SPLIT_CONTEXT_LIST))
                .thenReturn(Optional.of(List.of()));

        when(context.get(
                CatalogLoaderProperty.CATALOG_LOADER))
                .thenReturn(Optional.empty());

        DXSSink sink = new DXSSink();

        assertThrows(
                NoSuchElementException.class,
                () -> sink.sink(
                        mock(SingleOutputStreamOperator.class),
                        context
                )
        );
    }
}