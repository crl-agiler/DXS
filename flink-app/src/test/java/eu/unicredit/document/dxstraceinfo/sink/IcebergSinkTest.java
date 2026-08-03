package eu.unicredit.document.dxstraceinfo.sink;

import static org.mockito.Mockito.inOrder;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.mockStatic;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.verifyNoInteractions;
import static org.mockito.Mockito.when;

import eu.unicredit.document.dxstraceinfo.config.ConfigFlink;
import eu.unicredit.document.dxstraceinfo.transform.SplitContext;
import java.util.List;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.table.data.RowData;
import org.apache.flink.util.OutputTag;
import org.apache.iceberg.flink.TableLoader;
import org.apache.iceberg.flink.sink.FlinkSink;
import org.junit.jupiter.api.Test;
import org.mockito.InOrder;
import org.mockito.MockedStatic;

class IcebergSinkTest {

  private static final int WRITE_PARALLELISM = 4;

  @SuppressWarnings("unchecked")
  @Test
  void sinkFromShouldConfigureSinkForEachSplitContext() {
    ConfigFlink configFlink = mock(ConfigFlink.class);

    SingleOutputStreamOperator<Object> stream =
        mock(SingleOutputStreamOperator.class);

    SplitContext<Object> dossierContext =
        mock(SplitContext.class);

    SplitContext<Object> documentContext =
        mock(SplitContext.class);

    OutputTag<RowData> dossierTag =
        new OutputTag<RowData>("dxs-traceinfo-dossier") {
        };

    OutputTag<RowData> documentTag =
        new OutputTag<RowData>("dxs-traceinfo-document") {
        };

    DataStream<RowData> dossierStream =
        mock(DataStream.class);

    DataStream<RowData> documentStream =
        mock(DataStream.class);

    TableLoader dossierTableLoader =
        mock(TableLoader.class);

    TableLoader documentTableLoader =
        mock(TableLoader.class);

    List<String> dossierEqualityFields =
        List.of("dossier_id");

    List<String> documentEqualityFields =
        List.of("document_id");

    FlinkSink.Builder dossierBuilder =
        mock(FlinkSink.Builder.class);

    FlinkSink.Builder documentBuilder =
        mock(FlinkSink.Builder.class);

    when(configFlink.getParallelism())
        .thenReturn(WRITE_PARALLELISM);

    when(dossierContext.getOutputTag())
        .thenReturn(dossierTag);

    when(dossierContext.getTableLoader())
        .thenReturn(dossierTableLoader);

    when(dossierContext.getEqualityField())
        .thenReturn(dossierEqualityFields);

    when(documentContext.getOutputTag())
        .thenReturn(documentTag);

    when(documentContext.getTableLoader())
        .thenReturn(documentTableLoader);

    when(documentContext.getEqualityField())
        .thenReturn(documentEqualityFields);

    when(stream.getSideOutput(dossierTag))
        .thenReturn(dossierStream);

    when(stream.getSideOutput(documentTag))
        .thenReturn(documentStream);

    configureBuilder(dossierBuilder, dossierTableLoader);
    configureBuilder(documentBuilder, documentTableLoader);

    IcebergSink icebergSink =
        new IcebergSink(
            List.of(dossierContext, documentContext),
            configFlink);

    try (MockedStatic<FlinkSink> flinkSink =
             mockStatic(FlinkSink.class)) {

      flinkSink
          .when(() -> FlinkSink.forRowData(dossierStream))
          .thenReturn(dossierBuilder);

      flinkSink
          .when(() -> FlinkSink.forRowData(documentStream))
          .thenReturn(documentBuilder);

      icebergSink.sinkFrom(stream);

      flinkSink.verify(
          () -> FlinkSink.forRowData(dossierStream));

      flinkSink.verify(
          () -> FlinkSink.forRowData(documentStream));

      verifyBuilderConfiguration(
          dossierBuilder,
          dossierTableLoader,
          dossierEqualityFields,
          dossierTag.getId());

      verifyBuilderConfiguration(
          documentBuilder,
          documentTableLoader,
          documentEqualityFields,
          documentTag.getId());
    }

    verify(stream)
        .getSideOutput(dossierTag);

    verify(stream)
        .getSideOutput(documentTag);

    verify(configFlink, times(2))
        .getParallelism();
  }

  @SuppressWarnings("unchecked")
  @Test
  void sinkFromShouldConfigureBuilderMethodsInExpectedOrder() {
    ConfigFlink configFlink =
        mock(ConfigFlink.class);

    SingleOutputStreamOperator<Object> stream =
        mock(SingleOutputStreamOperator.class);

    SplitContext<Object> splitContext =
        mock(SplitContext.class);

    OutputTag<RowData> outputTag =
        new OutputTag<RowData>("dxs-traceinfo-dossier") {
        };

    DataStream<RowData> sideOutput =
        mock(DataStream.class);

    TableLoader tableLoader =
        mock(TableLoader.class);

    List<String> equalityFields =
        List.of("dossier_id");

    FlinkSink.Builder builder =
        mock(FlinkSink.Builder.class);

    when(configFlink.getParallelism())
        .thenReturn(WRITE_PARALLELISM);

    when(splitContext.getOutputTag())
        .thenReturn(outputTag);

    when(splitContext.getTableLoader())
        .thenReturn(tableLoader);

    when(splitContext.getEqualityField())
        .thenReturn(equalityFields);

    when(stream.getSideOutput(outputTag))
        .thenReturn(sideOutput);

    configureBuilder(builder, tableLoader);

    IcebergSink icebergSink =
        new IcebergSink(
            List.of(splitContext),
            configFlink);

    try (MockedStatic<FlinkSink> flinkSink =
             mockStatic(FlinkSink.class)) {

      flinkSink
          .when(() -> FlinkSink.forRowData(sideOutput))
          .thenReturn(builder);

      icebergSink.sinkFrom(stream);
    }

    InOrder inOrder =
        inOrder(builder);

    inOrder.verify(builder)
        .tableLoader(tableLoader);

    inOrder.verify(builder)
        .upsert(true);

    inOrder.verify(builder)
        .equalityFieldColumns(equalityFields);

    inOrder.verify(builder)
        .writeParallelism(WRITE_PARALLELISM);

    inOrder.verify(builder)
        .uidPrefix(outputTag.getId());

    inOrder.verify(builder)
        .append();

    inOrder.verifyNoMoreInteractions();
  }

  @SuppressWarnings("unchecked")
  @Test
  void sinkFromShouldDoNothingWhenSplitContextsAreEmpty() {
    ConfigFlink configFlink =
        mock(ConfigFlink.class);

    SingleOutputStreamOperator<Object> stream =
        mock(SingleOutputStreamOperator.class);

    IcebergSink icebergSink =
        new IcebergSink(
            List.of(),
            configFlink);

    try (MockedStatic<FlinkSink> flinkSink =
             mockStatic(FlinkSink.class)) {

      icebergSink.sinkFrom(stream);

      flinkSink.verifyNoInteractions();
    }

    verifyNoInteractions(stream);
    verifyNoInteractions(configFlink);
  }

  private void configureBuilder(
      FlinkSink.Builder builder,
      TableLoader tableLoader) {

    when(builder.tableLoader(tableLoader))
        .thenReturn(builder);

    when(builder.upsert(true))
        .thenReturn(builder);

    when(builder.equalityFieldColumns(
        org.mockito.ArgumentMatchers.anyList()))
        .thenReturn(builder);

    when(builder.writeParallelism(
        org.mockito.ArgumentMatchers.anyInt()))
        .thenReturn(builder);

    when(builder.uidPrefix(
        org.mockito.ArgumentMatchers.anyString()))
        .thenReturn(builder);
  }

  private void verifyBuilderConfiguration(
      FlinkSink.Builder builder,
      TableLoader tableLoader,
      List<String> equalityFields,
      String uidPrefix) {

    verify(builder)
        .tableLoader(tableLoader);

    verify(builder)
        .upsert(true);

    verify(builder)
        .equalityFieldColumns(equalityFields);

    verify(builder)
        .writeParallelism(WRITE_PARALLELISM);

    verify(builder)
        .uidPrefix(uidPrefix);

    verify(builder)
        .append();
  }
}