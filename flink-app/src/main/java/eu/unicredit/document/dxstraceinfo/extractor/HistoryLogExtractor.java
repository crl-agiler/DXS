package eu.unicredit.document.dxstraceinfo.extractor;

import eu.unicredit.document.dxstraceinfo.avro.Document;
import eu.unicredit.document.dxstraceinfo.avro.DocumentGroup;
import eu.unicredit.document.dxstraceinfo.avro.DossierTraceinfoEvent;
import eu.unicredit.document.dxstraceinfo.model.HistoryLogRecord;
import java.time.Instant;
import java.util.ArrayList;
import java.util.List;
import org.apache.flink.api.common.functions.MapFunction;

public class HistoryLogExtractor
    implements MapFunction<DossierTraceinfoEvent, List<HistoryLogRecord>> {

  @Override
  public List<HistoryLogRecord> map(DossierTraceinfoEvent event) {

    List<HistoryLogRecord> records = new ArrayList<>();

    records.add(
        new HistoryLogRecord(
            nextHistoryId(event.getDossierId()),
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
              nextHistoryId(group.getId()),
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
                nextHistoryId(document.getId()),
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

  private long nextHistoryId(long entityId) {
    return Instant.now().toEpochMilli() * 100000L + (entityId % 100000L);
  }
}
