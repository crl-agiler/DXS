package eu.unicredit.document.dxstraceinfo.mapping;

import eu.unicredit.document.dxstraceinfo.model.HistoryLogRecord;
import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.table.data.GenericRowData;
import org.apache.flink.table.data.RowData;

public class HistoryLogMapper
    implements MapFunction<HistoryLogRecord, RowData> {

  @Override
  public RowData map(HistoryLogRecord historyLog) {

    GenericRowData row = new GenericRowData(6);

    row.setField(
        0,
        historyLog.getHistoryLogId()
    );

    row.setField(
        1,
        AvroRowDataConverters.string(
            historyLog.getStatus())
    );

    row.setField(
        2,
        AvroRowDataConverters.string(
            historyLog.getSubStatus())
    );

    row.setField(
        3,
        historyLog.getLevelIdIdentifier()
    );

    row.setField(
        4,
        AvroRowDataConverters.string(
            historyLog.getLevel())
    );

    row.setField(
        5,
        AvroRowDataConverters.timestamp(
            historyLog.getStatusTimestamp())
    );

    return row;
  }
}
