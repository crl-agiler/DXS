package eu.unicredit.document.dxstraceinfo.extractor;

import eu.unicredit.document.dxstraceinfo.avro.Document;
import eu.unicredit.document.dxstraceinfo.avro.DocumentGroup;
import eu.unicredit.document.dxstraceinfo.avro.DossierTraceinfoEvent;
import eu.unicredit.document.dxstraceinfo.model.HistoryLogRecord;
import org.apache.flink.api.common.functions.MapFunction;

import java.util.ArrayList;
import java.util.List;

public class HistoryLogExtractor
    implements MapFunction<DossierTraceinfoEvent, List<HistoryLogRecord>> {

  @Override
  public List<HistoryLogRecord> map(DossierTraceinfoEvent event) {

    List<HistoryLogRecord> records = new ArrayList<>();
    records.add(
        new HistoryLogRecord(
            String.valueOf(event.getDossierId()),
            event.getDossierId(),
            event.getStatus(),
            event.getSubStatus(),
            event.getCreationDate(),
            "DOSSIER"
        )
    );
    for (DocumentGroup group : event.getDocumentGroups()) {

      records.add(
          new HistoryLogRecord(
                  event.getDossierId() + "-" + group.getId(),
              group.getId(),
              group.getStatus(),
              null,
              group.getCreationDate(),
              "DOCUMENTS_GROUP"
          )
      );

      for (Document document : group.getDocuments()) {

        records.add(
            new HistoryLogRecord(
                    event.getDossierId() + "-" + document.getId(),
                document.getId(),
                document.getStatus(),
                null,
                document.getCreationDate(),
                "DOCUMENT"
            )
        );
      }
    }

    return records;
  }
}
