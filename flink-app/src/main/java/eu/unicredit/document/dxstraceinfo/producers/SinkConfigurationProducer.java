package eu.unicredit.document.dxstraceinfo.producers;

import eu.unicredit.document.dxstraceinfo.config.ConfigApp;
import eu.unicredit.document.dxstraceinfo.config.ConfigIcebergCatalog;
import eu.unicredit.document.dxstraceinfo.factory.SplitContextFactory;
import eu.unicredit.document.dxstraceinfo.transform.SplitContext;
import lombok.RequiredArgsConstructor;
import org.apache.flink.table.data.RowData;
import org.apache.flink.util.OutputTag;
import org.apache.hadoop.conf.Configuration;
import org.apache.iceberg.flink.CatalogLoader;

import javax.enterprise.context.ApplicationScoped;
import javax.enterprise.inject.Produces;
import javax.inject.Inject;
import javax.inject.Singleton;
import java.util.Collections;
import java.util.List;

@ApplicationScoped
@RequiredArgsConstructor(onConstructor_ = @Inject)
public final class SinkConfigurationProducer {

    private final ConfigApp configApp;

    @Produces
    @Singleton
    public CatalogLoader catalogLoader() {

        ConfigIcebergCatalog catalog =
                configApp.getConfigIcebergCatalog();

        return CatalogLoader.hadoop(
                catalog.getCatalogName(),
                new Configuration(),
                Collections.singletonMap(
                        "warehouse",
                        catalog.getCatalogLocation()));
    }

    @Produces
    @Singleton
    public List<SplitContext<?>> splitContexts(CatalogLoader catalogLoader) {
        return new SplitContextFactory(catalogLoader).create();
    }

    @Produces
    @Singleton
    public OutputTag<RowData> discardTag() {
        return new OutputTag<>("dxs-traceinfo-discard-log") {};
    }

}

