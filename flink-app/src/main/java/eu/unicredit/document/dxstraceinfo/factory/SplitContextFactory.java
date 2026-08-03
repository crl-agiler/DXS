package eu.unicredit.document.dxstraceinfo.factory;

import eu.unicredit.document.dxstraceinfo.extractor.DocumentExtractor;
import eu.unicredit.document.dxstraceinfo.extractor.DocumentGroupExtractor;
import eu.unicredit.document.dxstraceinfo.extractor.HistoryLogExtractor;
import eu.unicredit.document.dxstraceinfo.extractor.SignerExtractor;
import eu.unicredit.document.dxstraceinfo.mapping.DocumentGroupRecordMapper;
import eu.unicredit.document.dxstraceinfo.mapping.DocumentMapper;
import eu.unicredit.document.dxstraceinfo.mapping.DossierMapper;
import eu.unicredit.document.dxstraceinfo.mapping.HistoryLogMapper;
import eu.unicredit.document.dxstraceinfo.mapping.SignerMapper;
import eu.unicredit.document.dxstraceinfo.transform.SplitContext;
import java.io.Serializable;
import java.util.Collections;
import java.util.List;
import java.util.function.Function;
import org.apache.flink.table.data.RowData;
import org.apache.flink.util.OutputTag;
import org.apache.iceberg.catalog.TableIdentifier;
import org.apache.iceberg.flink.CatalogLoader;
import org.apache.iceberg.flink.TableLoader;

public class SplitContextFactory implements Serializable {

  private final CatalogLoader catalogLoader;

  public SplitContextFactory(
      CatalogLoader catalogLoader) {

    this.catalogLoader = catalogLoader;
  }

  public List<SplitContext<?>> create() {

    Function<String, TableLoader> tableLoader =
        tableName -> TableLoader.fromCatalog(
            catalogLoader,
            TableIdentifier.of(tableName));

    OutputTag<RowData> dossier =
        outputTag("dxs-traceinfo-dossier");

    OutputTag<RowData> document =
        outputTag("dxs-traceinfo-document");

    OutputTag<RowData> documentGroup =
        outputTag("dxs-traceinfo-documents-group");

    OutputTag<RowData> signer =
        outputTag("dxs-traceinfo-signer");

    OutputTag<RowData> historyLog =
        outputTag("dxs-traceinfo-history-log");

    return List.of(

        SplitContext.of(
            Collections::singletonList,
            new DossierMapper(),
            dossier,
            tableLoader.apply(dossier.getId()),
            List.of("dossier_id")
        ),

        SplitContext.of(
            new DocumentGroupExtractor(),
            new DocumentGroupRecordMapper(),
            documentGroup,
            tableLoader.apply(documentGroup.getId()),
            List.of("documents_group_id")
        ),

        SplitContext.of(
            new DocumentExtractor(),
            new DocumentMapper(),
            document,
            tableLoader.apply(document.getId()),
            List.of("document_id")
        ),
        SplitContext.of(
            new SignerExtractor(),
            new SignerMapper(),
            signer,
            tableLoader.apply(signer.getId()),
            List.of("signer_id")
        ),

        SplitContext.of(
            new HistoryLogExtractor(),
            new HistoryLogMapper(),
            historyLog,
            tableLoader.apply(historyLog.getId()),
            List.of("history_log_id")
        )
    );
  }

  private OutputTag<RowData> outputTag(
      String tableName) {

    return new OutputTag<>(tableName) {
    };
  }
}
