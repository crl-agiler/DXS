package eu.unicredit.document.dxstraceinfo.transform;


import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertSame;
import static org.mockito.Mockito.mock;

import eu.unicredit.document.dxstraceinfo.avro.DossierTraceinfoEvent;
import java.util.List;
import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.table.data.GenericRowData;
import org.apache.flink.table.data.RowData;
import org.apache.flink.util.OutputTag;
import org.apache.iceberg.flink.TableLoader;
import org.junit.jupiter.api.Test;

class SplitContextTest {

  @Test
  void shouldCreateSplitContextAndExposeAllProperties() {

    MapFunction<DossierTraceinfoEvent, List<String>> extractor =
        event -> List.of("value");

    MapFunction<String, RowData> mapper =
        value -> GenericRowData.of(value);

    OutputTag<RowData> outputTag =
        new OutputTag<>("test-output") {
        };

    TableLoader tableLoader = mock(TableLoader.class);

    List<String> equalityFields =
        List.of("id", "code");

    SplitContext<String> splitContext =
        SplitContext.of(
            extractor,
            mapper,
            outputTag,
            tableLoader,
            equalityFields);

    assertNotNull(splitContext);

    assertSame(extractor, splitContext.getExtractor());
    assertSame(mapper, splitContext.getMapper());
    assertSame(outputTag, splitContext.getOutputTag());
    assertSame(tableLoader, splitContext.getTableLoader());
    assertEquals(equalityFields, splitContext.getEqualityField());
  }
}
