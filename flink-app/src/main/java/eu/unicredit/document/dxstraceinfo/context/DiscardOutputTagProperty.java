package eu.unicredit.document.dxstraceinfo.context;

import eu.unicredit.document.dxstraceinfo.api.AdditionalContextProperty;
import eu.unicredit.document.dxstraceinfo.api.DXSContext;
import org.apache.flink.table.data.RowData;
import org.apache.flink.util.OutputTag;

public class DiscardOutputTagProperty
        implements AdditionalContextProperty {

    public static final String DISCARD_OUTPUT_TAG = "DiscardOutputTag";

    @Override
    public String name() {
        return DISCARD_OUTPUT_TAG;
    }

    @Override
    @SuppressWarnings("unchecked")
    public OutputTag<RowData> instance(DXSContext context) {
        return new OutputTag<>("dxs-traceinfo-discard-log") {
        };
    }
}