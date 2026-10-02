package eu.unicredit.document.dxstraceinfo.mapping;

import eu.unicredit.document.dxstraceinfo.avro.Document;
import eu.unicredit.document.dxstraceinfo.avro.DocumentGroup;
import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.table.data.GenericRowData;
import org.apache.flink.table.data.RowData;

import java.time.Instant;

public class DocumentMapper
    implements MapFunction<Tuple2<DocumentGroup, Document>, RowData> {

  @Override
  public RowData map(Tuple2<DocumentGroup, Document> input) {

    DocumentGroup group = input.f0;
    Document document = input.f1;

    GenericRowData row = new GenericRowData(27);

    // 0 - document_id
    row.setField(
        0,
        document.getId()
    );

    // 1 - document_documents_group_id
    row.setField(
        1,
        group.getId()
    );

    // 2 - document_code
    row.setField(
        2,
        AvroRowDataConverters.string(
            document.getCode())
    );

    // 3 - document_description
    row.setField(
        3,
        AvroRowDataConverters.string(
            document.getDocumentDescription())
    );

    // 4 - document_process_type
    row.setField(
        4,
        AvroRowDataConverters.string(
            document.getProcessType())
    );

    // 5 - document_category
    row.setField(
        5,
        AvroRowDataConverters.string(
            document.getCategoryCode())
    );

    // 6 - document_signature_type
    row.setField(
        6,
        AvroRowDataConverters.string(
            document.getSignatureType())
    );

    // 7 - document_digitalization
    row.setField(
        7,
        AvroRowDataConverters.string(
            document.getDigitalization())
    );

    // 8 - document_datamatrix
    row.setField(
        8,
        AvroRowDataConverters.string(
            document.getDatamatrix())
    );

    // 9 - document_outcome
    row.setField(
        9,
        AvroRowDataConverters.string(
            outcomeFromStatus(
                document.getStatus())
        )
    );

    // 10 - document_process_code
    row.setField(
        10,
        AvroRowDataConverters.string(
            document.getProcessCode())
    );

    // 11 - document_product_name
    row.setField(
        11,
        AvroRowDataConverters.string(
            document.getProductName())
    );

    // 12 - document_status
    row.setField(
        12,
        AvroRowDataConverters.string(
            document.getStatus())
    );

    // 13 - document_page_number
    row.setField(
        13,
        document.getNumberOfPages()
    );

    // 14 - document_account_number
    row.setField(
        14,
        AvroRowDataConverters.string(
            document.getAccountNumber())
    );

    // 15 - document_size
    row.setField(
        15,
        document.getSize()
    );

    // 16 - document_signer_number
    row.setField(
        16,
        group.getSigners() == null ?
            0L
            : (long) group.getSigners().size()
    );

    // 17 - document_start_timestamp
    row.setField(
        17,
        AvroRowDataConverters.timestamp(
            document.getCreationDate())
    );

    // 18 - document_last_signature_timestamp
    row.setField(
            18,
            AvroRowDataConverters.timestamp(
                    isSentinelEpoch(document.getSignatureDate())
                            ? null
                            : document.getSignatureDate())
    );

    // 19 - document_end_timestamp
    row.setField(
            19,
            AvroRowDataConverters.timestamp(
                    isSentinelEpoch(document.getEndDate())
                            ? null
                            : document.getEndDate())
    );


    // 20 - document_controls_outcome_pec
    row.setField(
        20,
        AvroRowDataConverters.string(
            document.getControlsOutcomePec())
    );

    // 21 - document_signature_type_pec
    row.setField(
        21,
        AvroRowDataConverters.string(
            document.getSignatureTypePec())
    );

    // 22 - document_certificate_authority_pec
    row.setField(
        22,
        AvroRowDataConverters.string(
            document.getCertificateAuthorityPec())
    );

    // 23 - document_signature_point_timestamp_pec
    row.setField(
        23,
        AvroRowDataConverters.string(
            document.getSignaturePointTimestampPec())
    );

    // 24 - document_accessibility
    row.setField(
        24,
        AvroRowDataConverters.string(
            document.getAccessibility())
    );

    // 25 - document_archive_link
    row.setField(
        25,
        AvroRowDataConverters.string(
            document.getArchiveLink())
    );

    // 26 - document_signature_box_order
    row.setField(
        26,
        AvroRowDataConverters.string(
            document.getSignatureBoxOrder())
    );

    return row;
  }


  private boolean isSentinelEpoch(Instant instant) {
    return instant != null && instant.toEpochMilli() == 0L;
  }

  private String outcomeFromStatus(String status) {

    if (status == null) {
      return null;
    }

    switch (status) {

      case "PRINTED":
      case "ARCHIVED":
      case "ARCHIVED_SKIPPED":
        return "OK";

      case "CONTROLS_FAILED":
      case "CANCELLED":
      case "REJECTED":
        return "KO";

      case "CREATED":
      case "DOC_GENERATED":
      case "IN_ARCHIVING":
      case "TO_BE_ARCHIVED":
      case "BO_APPROVAL":
      case "RM_APPROVAL":
      case "CONFIRMED":
      case "UPLOADED":
        return "IN_PROGRESS";

      default:
        return null;
    }
  }
}
