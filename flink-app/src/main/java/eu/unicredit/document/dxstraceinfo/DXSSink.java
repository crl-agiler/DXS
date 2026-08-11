package eu.unicredit.document.dxstraceinfo;

import eu.unicredit.document.dxstraceinfo.config.ConfigApp;
import eu.unicredit.document.dxstraceinfo.transform.SplitContext;
import lombok.AllArgsConstructor;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.table.data.RowData;
import eu.unicredit.document.dxstraceinfo.api.Sink;
import org.apache.flink.util.OutputTag;
import org.apache.iceberg.catalog.TableIdentifier;
import org.apache.iceberg.flink.CatalogLoader;
import org.apache.iceberg.flink.TableLoader;
import org.apache.iceberg.flink.sink.FlinkSink;

import java.util.List;

@AllArgsConstructor
public class DXSSink implements Sink<RowData>  {

    private final ConfigApp configApp;
    private final List<SplitContext<?>> splitContexts;
    private final CatalogLoader catalogLoader;
    private final OutputTag<RowData> discardTag;

    @Override
    public void sink(SingleOutputStreamOperator<RowData> dataStream) {
        for (var context : splitContexts) {
            DataStream<RowData> tableRowOutput = dataStream.getSideOutput(context.getOutputTag());
            FlinkSink
                    .forRowData(tableRowOutput)
                    .tableLoader(context.getTableLoader())
                    .upsert(true)
                    .equalityFieldColumns(context.getEqualityField())
                    .writeParallelism(configApp.getFlinkConfig().getBaseParallelism())
                    .uidPrefix(context.getOutputTag().getId())
                    .append();
        }
        configureDiscardSink(
                dataStream,
                catalogLoader);
    }

    @Override
    public void onInit() {

    }

    private void configureDiscardSink(
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
                .append();
    }
}
