package eu.unicredit.document.dxstraceinfo.mapping;

import eu.unicredit.document.dxstraceinfo.avro.DocumentGroup;
import eu.unicredit.document.dxstraceinfo.avro.DossierTraceinfoEvent;
import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.table.data.GenericRowData;
import org.apache.flink.table.data.RowData;

public class DocumentGroupRecordMapper
    implements MapFunction<Tuple2<DossierTraceinfoEvent, DocumentGroup>, RowData> {

  @Override
  public RowData map(Tuple2<DossierTraceinfoEvent, DocumentGroup> input) {

    DossierTraceinfoEvent dossier = input.f0;
    DocumentGroup documentGroup = input.f1;

    GenericRowData row = new GenericRowData(8);

    row.setField(
        0,
        documentGroup.getId()
    );

    row.setField(
        1,
        dossier.getDossierId()
    );

    row.setField(
        2,
        AvroRowDataConverters.string(
            outcomeFromStatus(documentGroup.getStatus())
        )
    );

    row.setField(
        3,
        AvroRowDataConverters.string(
            documentGroup.getStatus())
    );

    row.setField(
        4,
        AvroRowDataConverters.timestamp(
            documentGroup.getCreationDate())
    );

    row.setField(
        5,
        AvroRowDataConverters.timestamp(
            documentGroup.getEndDate())
    );

    row.setField(
        6,
        AvroRowDataConverters.string(
            documentGroup.getDigilayerId())
    );

    row.setField(
        7,
        AvroRowDataConverters.string(
            documentGroup.getEnvelopeId())
    );

    return row;
  }

  private String outcomeFromStatus(String status) {

    if (status == null) {
      return null;
    }

    switch (status) {

      case "UPLOADED":
      case "PRINTED":
      case "ARCHIVED":
      case "AUTOMATIC_ARCHIVED":
      case "CLOSED":
        return "OK";

      case "DISABLED":
      case "CANCELLED":
      case "CANCELLED_ARCHIVED":
      case "CANCELLED_CREATED":
      case "CANCELLED_DRAFT":
      case "CANCELLED_LOCKED":
      case "CANCELLED_SENT":
      case "EXPIRED":
      case "ERROR_SIGNAL":
      case "ARCHIVED_CANCELED":
        return "KO";

      case "CREATED":
      case "LOCKED":
      case "READY":
      case "PROCESSING_PARTIALLY_SIGNED":
      case "TO_BE_ARCHIVED":
      case "TO_BE_CONFIRMED":
      case "PROCESSING":
      case "TO_BE_PRINTED":
      case "TO_BE_UPLOADED":
      case "TO_BE_ACKNOWLEDGED":
      case "SIGNED":
      case "HIDDEN":
        return "IN_PROGRESS";

      default:
        return null;
    }
  }
}
