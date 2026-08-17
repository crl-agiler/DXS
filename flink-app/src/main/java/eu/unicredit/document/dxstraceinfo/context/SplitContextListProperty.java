package eu.unicredit.document.dxstraceinfo.context;

import eu.unicredit.document.dxstraceinfo.api.AdditionalContextProperty;
import eu.unicredit.document.dxstraceinfo.api.DXSContext;
import eu.unicredit.document.dxstraceinfo.factory.SplitContextFactory;
import eu.unicredit.document.dxstraceinfo.transform.SplitContext;
import org.apache.iceberg.flink.CatalogLoader;

import java.io.Serializable;
import java.util.List;

public class SplitContextListProperty implements AdditionalContextProperty {

    public static final String SPLIT_CONTEXT_LIST = "SplitContextList";

    @Override
    public String name() {
        return SPLIT_CONTEXT_LIST;
    }

    @Override
    @SuppressWarnings("unchecked")
    public List<SplitContext<? extends Serializable>> instance(DXSContext context) {
        CatalogLoader o = context.get("CatalogLoader", CatalogLoader.class).orElseThrow();
        return new SplitContextFactory(o).create();
    }
}
