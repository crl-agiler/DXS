package eu.unicredit.document.dxstraceinfo.context;

import eu.unicredit.document.dxstraceinfo.api.DXSContext;
import eu.unicredit.document.dxstraceinfo.factory.SplitContextFactory;
import eu.unicredit.document.dxstraceinfo.transform.SplitContext;
import org.apache.iceberg.flink.CatalogLoader;
import org.junit.jupiter.api.Test;
import org.mockito.MockedConstruction;

import java.io.Serializable;
import java.util.List;
import java.util.NoSuchElementException;
import java.util.Optional;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertSame;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.mockConstruction;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

class SplitContextListPropertyTest {

    @Test
    void shouldReturnExpectedName() {

        SplitContextListProperty property =
                new SplitContextListProperty();

        assertEquals(
                SplitContextListProperty.SPLIT_CONTEXT_LIST,
                property.name()
        );
    }

    @Test
    void shouldCreateSplitContextList() {

        DXSContext context = mock(DXSContext.class);

        CatalogLoader catalogLoader =
                mock(CatalogLoader.class);

        @SuppressWarnings("unchecked")
        List<SplitContext<? extends Serializable>> expected =
                mock(List.class);

        when(context.get(
                "CatalogLoader",
                CatalogLoader.class))
                .thenReturn(Optional.of(catalogLoader));

        try (MockedConstruction<SplitContextFactory> mocked =
                     mockConstruction(
                             SplitContextFactory.class,
                             (mock, ctx) ->
                                     when(mock.create())
                                             .thenReturn(expected))) {

            SplitContextListProperty property =
                    new SplitContextListProperty();

            List<SplitContext<? extends Serializable>> result =
                    property.instance(context);

            assertSame(expected, result);

            verify(context)
                    .get("CatalogLoader",
                            CatalogLoader.class);

            verify(mocked.constructed().get(0))
                    .create();
        }
    }

    @Test
    void shouldThrowWhenCatalogLoaderIsMissing() {

        DXSContext context = mock(DXSContext.class);

        when(context.get(
                "CatalogLoader",
                CatalogLoader.class))
                .thenReturn(Optional.empty());

        SplitContextListProperty property =
                new SplitContextListProperty();

        assertThrows(
                NoSuchElementException.class,
                () -> property.instance(context)
        );

        verify(context)
                .get("CatalogLoader",
                        CatalogLoader.class);
    }
}
