package eu.unicredit.document.dxstraceinfo.metrics;

import java.util.Map;
import org.apache.flink.api.common.state.CheckpointListener;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.metrics.Counter;
import org.apache.flink.metrics.MetricGroup;
import org.apache.flink.streaming.api.functions.sink.RichSinkFunction;
import org.apache.flink.table.data.RowData;
import org.apache.iceberg.Snapshot;
import org.apache.iceberg.Table;
import org.apache.iceberg.flink.TableLoader;

public class IcebergCommitMetricsReporter extends RichSinkFunction<RowData> implements CheckpointListener {
  private transient Counter rowsWritten;
  private transient Counter filesWritten;
  private transient Counter bytesWritten;
  private transient long lastCommitDurationMs = 0L;
  private final TableLoader tableLoader;
  private final String tableName;

  public IcebergCommitMetricsReporter(TableLoader tableLoader, String tableName) {
    this.tableLoader = tableLoader;
    this.tableName = tableName;
  }

  @Override
  public void open(Configuration parameters) throws Exception {
    super.open(parameters);
    tableLoader.open();
    MetricGroup group = getRuntimeContext().getMetricGroup().addGroup("iceberg").addGroup("table_name",tableName);
    rowsWritten = group.counter("rows_written");
    filesWritten = group.counter("files_written");
    bytesWritten = group.counter("bytes_written");

    group.gauge("last_commit_duration_ms", () -> lastCommitDurationMs);

  }

  @Override
  public void notifyCheckpointComplete(long checkpointId) throws Exception {
    long start = System.currentTimeMillis();

    Table table = tableLoader.loadTable();
    Snapshot snapshot = table.currentSnapshot();

    if (snapshot != null){
      Map<String,String> summary = snapshot.summary();
      rowsWritten.inc(Long.parseLong(summary.getOrDefault("added-recors","0")));
      filesWritten.inc(Long.parseLong(summary.getOrDefault("added-data-files","0")));
      bytesWritten.inc(Long.parseLong(summary.getOrDefault("added-files-size","0")));

    }

    lastCommitDurationMs = System.currentTimeMillis() - start;
  }

  @Override
  public void close() throws Exception {
    tableLoader.close();
    super.close();
  }
}
