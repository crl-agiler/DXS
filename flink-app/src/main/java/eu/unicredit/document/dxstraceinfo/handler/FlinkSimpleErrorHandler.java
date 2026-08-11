package eu.unicredit.document.dxstraceinfo.handler;

import eu.unicredit.document.dxstraceinfo.api.ErrorHandler;
import eu.unicredit.document.dxstraceinfo.avro.DossierTraceinfoEvent;
import lombok.RequiredArgsConstructor;
import org.apache.commons.lang3.StringUtils;
import org.apache.flink.table.data.GenericRowData;
import org.apache.flink.table.data.RowData;
import org.apache.flink.table.data.StringData;
import org.apache.flink.table.data.TimestampData;
import org.apache.flink.types.RowKind;
import org.apache.flink.util.OutputTag;

import java.time.Instant;
import java.util.Optional;

@RequiredArgsConstructor
public class FlinkSimpleErrorHandler implements ErrorHandler<DossierTraceinfoEvent> {

    private final OutputTag<RowData> malformedRecordOutputTag;

    @Override
    public void handle(DossierTraceinfoEvent record, String error, OutputEmitter emitter) {
        String serializedRecord
                = Optional.ofNullable(record)
                .map(DossierTraceinfoEvent::toString)
                .orElse(StringUtils.EMPTY);
        GenericRowData rowData = GenericRowData.ofKind(
                RowKind.INSERT,
                StringData.fromString(
                        serializedRecord),
                StringData.fromString(
                        error),
                TimestampData.fromInstant(
                        Instant.now()));
        emitter.emit(malformedRecordOutputTag, rowData);
    }
}
