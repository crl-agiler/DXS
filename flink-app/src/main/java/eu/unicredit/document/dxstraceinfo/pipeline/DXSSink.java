package eu.unicredit.document.dxstraceinfo.pipeline;

import eu.unicredit.document.dxstraceinfo.api.DXSContext;
import eu.unicredit.document.dxstraceinfo.api.Sink;
import eu.unicredit.document.dxstraceinfo.context.CatalogLoaderProperty;
import eu.unicredit.document.dxstraceinfo.context.DiscardOutputTagProperty;
import eu.unicredit.document.dxstraceinfo.context.SplitContextListProperty;
import eu.unicredit.document.dxstraceinfo.metrics.IcebergCommitMetricsReporter;
import eu.unicredit.document.dxstraceinfo.transform.SplitContext;
import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.table.data.RowData;
import org.apache.flink.util.OutputTag;
import org.apache.iceberg.catalog.TableIdentifier;
import org.apache.iceberg.flink.CatalogLoader;
import org.apache.iceberg.flink.TableLoader;
import org.apache.iceberg.flink.sink.FlinkSink;

import java.util.List;

@RequiredArgsConstructor
@Slf4j
public class DXSSink implements Sink<RowData> {

    @Override
    @SuppressWarnings("unchecked")
    public void sink(SingleOutputStreamOperator<RowData> dataStream, DXSContext dxsContext) {
        OutputTag<RowData> discardTag = (OutputTag<RowData>) dxsContext.get(DiscardOutputTagProperty.DISCARD_OUTPUT_TAG).orElseThrow();
        List<SplitContext<?>> splitContexts = (List<SplitContext<?>>) dxsContext.get(SplitContextListProperty.SPLIT_CONTEXT_LIST).orElseThrow();
        CatalogLoader catalogLoader = (CatalogLoader) dxsContext.get(CatalogLoaderProperty.CATALOG_LOADER).orElseThrow();
        for (var context : splitContexts) {
            OutputTag<RowData> outputTag = context.getOutputTag();
            DataStream<RowData> tableRowOutput = dataStream.getSideOutput(outputTag);
            FlinkSink.Builder flinkSinkBuilder = FlinkSink
                    .forRowData(tableRowOutput)
                    .tableLoader(context.getTableLoader())
                    .upsert(context.isUpsert());
            if (context.isUpsert()) {
                flinkSinkBuilder = flinkSinkBuilder
                        .equalityFieldColumns(context.getEqualityField());
            }
            flinkSinkBuilder.writeParallelism(dxsContext.config().getFlinkConfig().getBaseParallelism())
                    .uidPrefix(context.getOutputTag().getId())
                    .append();
            tableRowOutput
                    .addSink(new IcebergCommitMetricsReporter(
                            context.getTableLoader(),
                            context.getOutputTag().getId()))
                    .name("iceberg-metrics-reporter-" + context.getOutputTag().getId())
                    .uid("iceberg-metrics-" + context.getOutputTag().getId())
                    .setParallelism(1);
        }
        configureDiscardSink(
                discardTag,
                dataStream,
                catalogLoader);
    }

    @Override
    public void onInit() {
        log.info("Initializing DSX Sink");
    }

    private void configureDiscardSink(
            OutputTag<RowData> discardTag,
            SingleOutputStreamOperator<RowData> stream,
            CatalogLoader catalogLoader) {
        TableLoader discardTableLoader =
                TableLoader.fromCatalog(
                        catalogLoader,
                        TableIdentifier.of(
                                discardTag.getId()));
        FlinkSink
                .forRowData(
                        stream.getSideOutput(
                                discardTag))
                .tableLoader(
                        discardTableLoader)
                .upsert(false)
                .uidPrefix(discardTag.getId())
                .append();
    }
}