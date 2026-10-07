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

  private static final String KEY_SEPARATOR = "-";

  @Override
  public List<HistoryLogRecord> map(DossierTraceinfoEvent event) {

    List<HistoryLogRecord> records = new ArrayList<>();
    long dossierId = event.getDossierId();

    records.add(new HistoryLogRecord(
            dossierHistoryLogId(dossierId), dossierId,
            event.getStatus(), event.getSubStatus(),
            event.getEventTimestamp(), "DOSSIER"));

    for (DocumentGroup group : event.getDocumentGroups()) {
      records.add(new HistoryLogRecord(
              documentsGroupHistoryLogId(dossierId, group.getId()), group.getId(),
              group.getStatus(), null,
              event.getEventTimestamp(), "DOCUMENTS_GROUP"));

      for (Document document : group.getDocuments()) {
        records.add(new HistoryLogRecord(
                documentHistoryLogId(dossierId, document.getId()), document.getId(),
                document.getStatus(), null,
                event.getEventTimestamp(), "DOCUMENT"));
      }
    }
    return records;
  }

  private String dossierHistoryLogId(long dossierId) {
    return String.valueOf(dossierId);
  }

  private String documentsGroupHistoryLogId(long dossierId, long documentsGroupId) {
    return dossierId + KEY_SEPARATOR + documentsGroupId;
  }

  private String documentHistoryLogId(long dossierId, long documentId) {
    return dossierId + KEY_SEPARATOR + documentId;
  }
}