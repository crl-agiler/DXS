package eu.unicredit.document.dxstraceinfo.context;

import eu.unicredit.document.dxstraceinfo.api.DXSContext;
import eu.unicredit.document.dxstraceinfo.config.pojo.ConfigApp;
import eu.unicredit.document.dxstraceinfo.config.pojo.ConfigIcebergCatalog;
import org.apache.iceberg.flink.CatalogLoader;
import org.junit.jupiter.api.Test;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

class CatalogLoaderPropertyTest {

    @Test
    void shouldReturnExpectedName() {

        CatalogLoaderProperty property =
                new CatalogLoaderProperty();

        assertEquals(
                CatalogLoaderProperty.CATALOG_LOADER,
                property.name()
        );
    }

    @Test
    void shouldCreateCatalogLoader() {

        ConfigIcebergCatalog catalog =
                mock(ConfigIcebergCatalog.class);

        ConfigApp config =
                mock(ConfigApp.class);

        DXSContext context =
                mock(DXSContext.class);

        when(context.config())
                .thenReturn(config);

        when(config.getConfigIcebergCatalog())
                .thenReturn(catalog);

        when(catalog.getCatalogName())
                .thenReturn("test-catalog");

        when(catalog.getCatalogLocation())
                .thenReturn("gs://warehouse");

        CatalogLoaderProperty property =
                new CatalogLoaderProperty();

        CatalogLoader loader =
                property.instance(context);

        assertNotNull(loader);

        verify(context).config();
        verify(config).getConfigIcebergCatalog();
        verify(catalog).getCatalogName();
        verify(catalog).getCatalogLocation();
    }
}