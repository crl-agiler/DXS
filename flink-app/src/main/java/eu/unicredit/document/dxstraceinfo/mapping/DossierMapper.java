package eu.unicredit.document.dxstraceinfo.mapping;

import eu.unicredit.document.dxstraceinfo.avro.Customer;
import eu.unicredit.document.dxstraceinfo.avro.DossierTraceinfoEvent;
import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.table.data.GenericRowData;
import org.apache.flink.table.data.RowData;

public class DossierMapper implements MapFunction<DossierTraceinfoEvent, RowData> {

  @Override
  public RowData map(DossierTraceinfoEvent event) {

    GenericRowData row = new GenericRowData(27);

    Customer customer = getMainCustomer(event);

    // 0 - dossier_id
    row.setField(
        0,
        event.getDossierId()
    );

    // 1 - dossier_selected_process_type
    row.setField(
        1,
        AvroRowDataConverters.string(
            event.getSelectedProcessType())
    );

    // 2 - dossier_reprint
    row.setField(
        2,
        AvroRowDataConverters.string(
            event.getReprint())
    );

    // 3 - dossier_paper_reason
    row.setField(
        3,
        AvroRowDataConverters.string(
            event.getPaperReason())
    );

    // 4 - dossier_vertical_code
    row.setField(
        4,
        AvroRowDataConverters.string(
            event.getApplicationCode())
    );

    // 5 - dossier_case_id
    row.setField(
        5,
        AvroRowDataConverters.string(
            event.getPracticeId())
    );

    // 6 - dossier_exe_application
    row.setField(
        6,
        AvroRowDataConverters.string(
            event.getExeApp())
    );

    // 7 - dossier_user_id
    row.setField(
        7,
        AvroRowDataConverters.string(
            event.getAuthor())
    );

    // 8 - dossier_pec_flow_type
    row.setField(
        8,
        AvroRowDataConverters.string(
            event.getPecFlow())
    );

    // 9 - dossier_outcome
    row.setField(
        9,
        AvroRowDataConverters.string(
            outcomeFromStatus(event.getStatus()))
    );

    // 10 - dossier_related_dossier_id
    row.setField(
        10,
        AvroRowDataConverters.string(
            event.getRelatedDossier())
    );

    // 11 - dossier_correlation_id
    row.setField(
        11,
        AvroRowDataConverters.string(
            event.getCorrelationKey())
    );

    if (customer != null) {

      // 12 - customer_type
      row.setField(
          12,
          AvroRowDataConverters.string(
              customer.getType())
      );

      // 17 - customer_ndg
      row.setField(
          17,
          AvroRowDataConverters.string(
              customer.getNdg())
      );

      // 18 - customer_channel
      row.setField(
          18,
          AvroRowDataConverters.string(
              customer.getSelectedChannels())
      );
    }

    // 13 - dossier_status
    row.setField(
        13,
        AvroRowDataConverters.string(
            event.getStatus())
    );

    // 14 - dossier_sub_status
    row.setField(
        14,
        AvroRowDataConverters.string(
            event.getSubStatus())
    );

    // 15 - dossier_branch
    row.setField(
        15,
        AvroRowDataConverters.string(
            event.getBranch())
    );

    // 16 - dossier_abi
    row.setField(
        16,
        AvroRowDataConverters.string(
            event.getBankCode())
    );

    // 19 - dossier_start_timestamp
    row.setField(
        19,
        AvroRowDataConverters.timestamp(
            event.getCreationDate())
    );

    // 20 - dossier_end_timestamp
    row.setField(
        20,
        AvroRowDataConverters.timestamp(
            event.getEndDate())
    );

    // 21 - dossier_master_id
    row.setField(
        21,
        AvroRowDataConverters.longValue(
            event.getMasterDossierId())
    );

    // 22 - dossier_motivation_of_expiration
    row.setField(
        22,
        AvroRowDataConverters.string(
            event.getMotivation())
    );

    // 23 - dossier_flow_type
    row.setField(
        23,
        AvroRowDataConverters.string(
            event.getFlowType())
    );

    // 24 - dossier_type
    row.setField(
        24,
        AvroRowDataConverters.string(
            event.getDossierType())
    );

    // 25 - dossier_reference_branch_pec
    row.setField(
        25,
        AvroRowDataConverters.string(
            event.getReferenceBranchPec())
    );

    // 26 - dossier_legal_entity
    row.setField(
        26,
        AvroRowDataConverters.string(
            event.getLegalEntity())
    );

    return row;
  }

  private Customer getMainCustomer(DossierTraceinfoEvent event) {

    return event.getCustomers() == null ||
        event.getCustomers().isEmpty() ?
        null
        : event.getCustomers().get(0);
  }

  private String outcomeFromStatus(String status) {

    if (status == null) {
      return null;
    }

    switch (status) {

      case "CONCLUDED":
      case "CLOSED":
        return "OK";

      case "EXPIRED":
      case "CANCELLED":
        return "KO";

      case "CREATED":
      case "DRAFT":
      case "DRAFT_PEC":
      case "PROCESSING":
      case "RETRY_ON_PROCESS":
      case "PEC_PROCESSING":
      case "TO_BE_ACCEPTED":
      case "TO_BE_CLOSED":
        return "IN_PROGRESS";

      default:
        return null;
    }
  }
}
