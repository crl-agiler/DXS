package eu.unicredit.document.dxstraceinfo.transform;

import eu.unicredit.document.dxstraceinfo.avro.DossierTraceinfoEvent;
import org.apache.commons.lang3.StringUtils;
import org.apache.flink.streaming.api.functions.ProcessFunction;
import org.apache.flink.table.data.GenericRowData;
import org.apache.flink.table.data.RowData;
import org.apache.flink.table.data.StringData;
import org.apache.flink.table.data.TimestampData;
import org.apache.flink.types.RowKind;
import org.apache.flink.util.Collector;
import org.apache.flink.util.OutputTag;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.time.Instant;
import java.util.Optional;

public class PreKeyFilterProcess extends ProcessFunction<DossierTraceinfoEvent, DossierTraceinfoEvent> {

    private static final StringData ERROR_STRING = StringData.fromString(
            "{\"errorType\": \"PROCESSING_ERROR\", \"message\": \"Event is null or masterDossierId is null\"}");

    private static final Logger LOGGER = LoggerFactory.getLogger(PreKeyFilterProcess.class);

    private final OutputTag<RowData> discardTag;

    public PreKeyFilterProcess(OutputTag<RowData> discardTag) {
        this.discardTag = discardTag;
    }

    @Override
    public void processElement(DossierTraceinfoEvent dossierTraceinfoEvent,
                               ProcessFunction<DossierTraceinfoEvent, DossierTraceinfoEvent>.Context context, Collector<DossierTraceinfoEvent> collector) throws Exception {
        if (dossierTraceinfoEvent == null || dossierTraceinfoEvent.getMasterDossierId() == null) {
            LOGGER.error("Event is null or masterDossierId is null");
            String dossierTraceInfoEventStr = Optional.ofNullable(dossierTraceinfoEvent)
                    .map(DossierTraceinfoEvent::toString)
                    .orElse(StringUtils.EMPTY);
            context.output(discardTag, GenericRowData.ofKind(
                    RowKind.INSERT,
                    StringData.fromString(
                            dossierTraceInfoEventStr),
                    ERROR_STRING,
                    TimestampData.fromInstant(
                            Instant.now())));
        } else {
            collector.collect(dossierTraceinfoEvent);
        }
    }
}
