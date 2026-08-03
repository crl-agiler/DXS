package eu.unicredit.document.dxstraceinfo.sink;

import eu.unicredit.document.dxstraceinfo.config.ConfigFlink;
import eu.unicredit.document.dxstraceinfo.transform.SplitContext;
import java.util.List;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.table.data.RowData;
import org.apache.iceberg.flink.sink.FlinkSink;

public final class IcebergSink {

  private final List<SplitContext<?>> splitContexts;
  private final ConfigFlink confFlink;

  public IcebergSink(List<SplitContext<?>> splitContexts,
                     ConfigFlink confFlink) {
    this.splitContexts = splitContexts;
    this.confFlink = confFlink;
  }

  public <T> void sinkFrom(SingleOutputStreamOperator<T> stream) {
    for (var context : splitContexts) {
      DataStream<RowData> tableRowOutput = stream.getSideOutput(context.getOutputTag());

      FlinkSink
          .forRowData(tableRowOutput)
          .tableLoader(context.getTableLoader())
          .upsert(true)
          .equalityFieldColumns(context.getEqualityField())
          .writeParallelism(confFlink.getParallelism())
          .uidPrefix(context.getOutputTag().getId())
          .append();
    }
  }
}
