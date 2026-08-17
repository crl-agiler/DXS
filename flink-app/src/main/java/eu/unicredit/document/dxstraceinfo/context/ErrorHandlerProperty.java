package eu.unicredit.document.dxstraceinfo.context;
import eu.unicredit.document.dxstraceinfo.api.AdditionalContextProperty;
import eu.unicredit.document.dxstraceinfo.api.DXSContext;
import eu.unicredit.document.dxstraceinfo.api.ErrorHandler;
import eu.unicredit.document.dxstraceinfo.avro.DossierTraceinfoEvent;
import eu.unicredit.document.dxstraceinfo.handler.FlinkSimpleErrorHandler;
import org.apache.flink.table.data.RowData;
import org.apache.flink.util.OutputTag;

public class ErrorHandlerProperty
        implements AdditionalContextProperty {

    public static final String ERROR_HANDLER = "ErrorHandler";

    @Override
    public String name() {
        return ERROR_HANDLER;
    }

    @Override
    @SuppressWarnings("unchecked")
    public ErrorHandler<DossierTraceinfoEvent> instance(
            DXSContext context) {

        OutputTag<RowData> discardOutputTag =
                context.<OutputTag<RowData>>get("DiscardOutputTag")
                        .orElseThrow();

        return new FlinkSimpleErrorHandler(
                discardOutputTag);
    }
}