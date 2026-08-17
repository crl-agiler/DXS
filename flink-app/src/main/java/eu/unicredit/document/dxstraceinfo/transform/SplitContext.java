package eu.unicredit.document.dxstraceinfo.transform;

import eu.unicredit.document.dxstraceinfo.avro.DossierTraceinfoEvent;
import lombok.Getter;
import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.table.data.RowData;
import org.apache.flink.util.OutputTag;
import org.apache.iceberg.flink.TableLoader;

import java.io.Serializable;
import java.util.List;

/**
 * Encapsulates all the information required to process a specific section of a
 * {@link DossierTraceinfoEvent} during the split phase.
 *
 * <p>A {@code SplitContext} defines:
 * <ul>
 *     <li>how to extract a collection of domain objects from a
 *     {@link DossierTraceinfoEvent};</li>
 *     <li>how to convert each extracted object into a Flink {@link RowData};</li>
 *     <li>the side output identified by the corresponding {@link OutputTag};</li>
 *     <li>the target Iceberg table represented by the {@link TableLoader};</li>
 *     <li>the equality fields used for Iceberg upsert or write operations.</li>
 * </ul>
 *
 * <p>The generic type parameter {@code T} represents the type of the extracted
 * objects that will be transformed into {@link RowData} instances.
 *
 * @param <T> type of objects extracted from the source event and mapped to
 *            {@link RowData}
 */
@Getter
public class SplitContext<T> implements Serializable {


  /**
   * Extracts a collection of objects of type {@code T} from a
   * {@link DossierTraceinfoEvent}.
   */
  private final MapFunction<DossierTraceinfoEvent, List<T>> extractor;

  /**
   * Maps an extracted object into a Flink {@link RowData}.
   */
  private final MapFunction<T, RowData> mapper;

  /**
   * Side output tag used to route the generated {@link RowData}.
   */
  private final OutputTag<RowData> outputTag;

  /**
   * Iceberg table loader associated with this split context.
   */
  private final TableLoader tableLoader;

  /**
   * List of equality fields used by Iceberg for record identification and
   * upsert operations.
   */
  private final List<String> equalityField;

  /**
   * Creates a new {@code SplitContext}.
   *
   * @param extractor     extracts objects from a {@link DossierTraceinfoEvent}
   * @param mapper        maps extracted objects to {@link RowData}
   * @param outputTag     side output destination for mapped records
   * @param tableLoader   Iceberg table loader associated with the output
   * @param equalityField list of equality fields used by Iceberg
   */
  private SplitContext(
      MapFunction<DossierTraceinfoEvent, List<T>> extractor,
      MapFunction<T, RowData> mapper,
      OutputTag<RowData> outputTag,
      TableLoader tableLoader,
      List<String> equalityField) {

    this.extractor = extractor;
    this.mapper = mapper;
    this.outputTag = outputTag;
    this.tableLoader = tableLoader;
    this.equalityField = equalityField;
  }

  /**
   * Creates a new {@code SplitContext} instance.
   *
   * @param extractor    extracts a list of objects from the source event
   * @param mapper       converts extracted objects into {@link RowData}
   * @param outputTag    side output tag receiving the produced records
   * @param tableLoader  Iceberg table loader associated with the output
   * @param equityFields equality fields used for Iceberg write operations
   * @param <X>          type of extracted objects
   * @return a fully configured {@code SplitContext}
   */
  public static <X extends Serializable> SplitContext<X> of(
      MapFunction<DossierTraceinfoEvent, List<X>> extractor,
      MapFunction<X, RowData> mapper,
      OutputTag<RowData> outputTag,
      TableLoader tableLoader,
      List<String> equityFields) {

    return new SplitContext<>(
        extractor,
        mapper,
        outputTag,
        tableLoader,
        equityFields);
  }
}
