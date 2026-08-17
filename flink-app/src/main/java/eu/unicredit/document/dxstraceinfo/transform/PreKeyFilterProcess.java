package eu.unicredit.document.dxstraceinfo.transform;

import eu.unicredit.document.dxstraceinfo.api.ErrorHandler;
import eu.unicredit.document.dxstraceinfo.avro.DossierTraceinfoEvent;
import org.apache.flink.streaming.api.functions.ProcessFunction;
import org.apache.flink.util.Collector;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Objects;

public class PreKeyFilterProcess extends ProcessFunction<DossierTraceinfoEvent, DossierTraceinfoEvent> {

    private static final String ERROR_STRING = "{\"errorType\": \"PROCESSING_ERROR\", \"message\": \"Event is null or masterDossierId is null\"}";

    private static final Logger LOGGER = LoggerFactory.getLogger(PreKeyFilterProcess.class);

    private final ErrorHandler<DossierTraceinfoEvent> errorHandler;

    public PreKeyFilterProcess(ErrorHandler<DossierTraceinfoEvent> errorHandler) {
        this.errorHandler = Objects.requireNonNull(errorHandler, "errorHandler cannot be null");
    }

    @Override
    public void processElement(DossierTraceinfoEvent dossierTraceinfoEvent,
                               ProcessFunction<DossierTraceinfoEvent, DossierTraceinfoEvent>.Context context, Collector<DossierTraceinfoEvent> collector) throws Exception {
        if (dossierTraceinfoEvent == null || dossierTraceinfoEvent.getMasterDossierId() == null) {
            LOGGER.error("Event is null or masterDossierId is null");
            errorHandler.handle(dossierTraceinfoEvent, ERROR_STRING, context::output);
        } else {
            collector.collect(dossierTraceinfoEvent);
        }
    }
}
