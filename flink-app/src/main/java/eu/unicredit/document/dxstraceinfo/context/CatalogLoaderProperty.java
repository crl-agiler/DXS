package eu.unicredit.document.dxstraceinfo.context;

import eu.unicredit.document.dxstraceinfo.api.AdditionalContextProperty;
import eu.unicredit.document.dxstraceinfo.api.DXSContext;
import eu.unicredit.document.dxstraceinfo.config.pojo.ConfigIcebergCatalog;
import org.apache.hadoop.conf.Configuration;
import org.apache.iceberg.flink.CatalogLoader;

import java.util.Collections;

public class CatalogLoaderProperty
        implements AdditionalContextProperty {

    public static final String CATALOG_LOADER = "CatalogLoader";

    @Override
    public String name() {
        return CATALOG_LOADER;
    }

    @Override
    @SuppressWarnings("unchecked")
    public CatalogLoader instance(DXSContext context) {

        ConfigIcebergCatalog catalog =
                context.config().getConfigIcebergCatalog();

        return CatalogLoader.hadoop(
                catalog.getCatalogName(),
                new Configuration(),
                Collections.singletonMap(
                        "warehouse",
                        catalog.getCatalogLocation()
                )
        );
    }
}