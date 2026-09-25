package eu.unicredit.document.dxstraceinfo.it;

import org.apache.iceberg.Schema;
import org.apache.iceberg.types.Types;

public final class IcebergTestSchemas {

    private IcebergTestSchemas() {
    }

    public static Schema dossier() {

        return new Schema(
                Types.NestedField.optional(1, "dossier_id", Types.LongType.get()),
                Types.NestedField.optional(2, "dossier_selected_process_type", Types.StringType.get()),
                Types.NestedField.optional(3, "dossier_reprint", Types.StringType.get()),
                Types.NestedField.optional(4, "dossier_paper_reason", Types.StringType.get()),
                Types.NestedField.optional(5, "dossier_vertical_code", Types.StringType.get()),
                Types.NestedField.optional(6, "dossier_case_id", Types.StringType.get()),
                Types.NestedField.optional(7, "dossier_exe_application", Types.StringType.get()),
                Types.NestedField.optional(8, "dossier_user_id", Types.StringType.get()),
                Types.NestedField.optional(9, "dossier_pec_flow_type", Types.StringType.get()),
                Types.NestedField.optional(10, "dossier_outcome", Types.StringType.get()),
                Types.NestedField.optional(11, "dossier_related_dossier_id", Types.StringType.get()),
                Types.NestedField.optional(12, "dossier_correlation_id", Types.StringType.get()),
                Types.NestedField.optional(13, "customer_type", Types.StringType.get()),
                Types.NestedField.optional(14, "dossier_status", Types.StringType.get()),
                Types.NestedField.optional(15, "dossier_sub_status", Types.StringType.get()),
                Types.NestedField.optional(16, "dossier_branch", Types.StringType.get()),
                Types.NestedField.optional(17, "dossier_abi", Types.StringType.get()),
                Types.NestedField.optional(18, "customer_ndg", Types.StringType.get()),
                Types.NestedField.optional(19, "customer_channel", Types.StringType.get()),
                Types.NestedField.optional(20, "dossier_start_timestamp", Types.TimestampType.withZone()),
                Types.NestedField.optional(21, "dossier_end_timestamp", Types.TimestampType.withZone()),
                Types.NestedField.optional(22, "dossier_master_id", Types.LongType.get()),
                Types.NestedField.optional(23, "dossier_motivation_of_expiration", Types.StringType.get()),
                Types.NestedField.optional(24, "dossier_flow_type", Types.StringType.get()),
                Types.NestedField.optional(25, "dossier_type", Types.StringType.get()),
                Types.NestedField.optional(26, "dossier_reference_branch_pec", Types.StringType.get()),
                Types.NestedField.optional(27, "dossier_legal_entity", Types.StringType.get()),
                Types.NestedField.optional(28, "update_timestamp", Types.TimestampType.withZone()),
                Types.NestedField.optional(29, "event_timestamp", Types.TimestampType.withZone())
        );
    }

    public static Schema documentsGroup() {

        return new Schema(
                Types.NestedField.optional(1, "documents_group_id", Types.LongType.get()),
                Types.NestedField.optional(2, "documents_group_dossier_id", Types.LongType.get()),
                Types.NestedField.optional(3, "documents_group_outcome", Types.StringType.get()),
                Types.NestedField.optional(4, "documents_group_status", Types.StringType.get()),
                Types.NestedField.optional(5, "documents_group_start_timestamp", Types.TimestampType.withZone()),
                Types.NestedField.optional(6, "documents_group_end_timestamp", Types.TimestampType.withZone()),
                Types.NestedField.optional(7, "documents_group_digilayer_id", Types.StringType.get()),
                Types.NestedField.optional(8, "documents_group_envelope_id", Types.StringType.get()),
                Types.NestedField.optional(9, "update_timestamp", Types.TimestampType.withZone()),
                Types.NestedField.optional(10, "event_timestamp", Types.TimestampType.withZone())
        );
    }

    public static Schema document() {

        return new Schema(
                Types.NestedField.optional(1, "document_id", Types.LongType.get()),
                Types.NestedField.optional(2, "document_documents_group_id", Types.LongType.get()),
                Types.NestedField.optional(3, "document_code", Types.StringType.get()),
                Types.NestedField.optional(4, "document_description", Types.StringType.get()),
                Types.NestedField.optional(5, "document_process_type", Types.StringType.get()),
                Types.NestedField.optional(6, "document_category", Types.StringType.get()),
                Types.NestedField.optional(7, "document_signature_type", Types.StringType.get()),
                Types.NestedField.optional(8, "document_digitalization", Types.StringType.get()),
                Types.NestedField.optional(9, "document_datamatrix", Types.StringType.get()),
                Types.NestedField.optional(10, "document_outcome", Types.StringType.get()),
                Types.NestedField.optional(11, "document_process_code", Types.StringType.get()),
                Types.NestedField.optional(12, "document_product_name", Types.StringType.get()),
                Types.NestedField.optional(13, "document_status", Types.StringType.get()),
                Types.NestedField.optional(14, "document_page_number", Types.LongType.get()),
                Types.NestedField.optional(15, "document_account_number", Types.StringType.get()),
                Types.NestedField.optional(16, "document_size", Types.LongType.get()),
                Types.NestedField.optional(17, "document_signer_number", Types.LongType.get()),
                Types.NestedField.optional(18, "document_start_timestamp", Types.TimestampType.withZone()),
                Types.NestedField.optional(19, "document_last_signature_timestamp", Types.TimestampType.withZone()),
                Types.NestedField.optional(20, "document_end_timestamp", Types.TimestampType.withZone()),
                Types.NestedField.optional(21, "document_controls_outcome_pec", Types.StringType.get()),
                Types.NestedField.optional(22, "document_signature_type_pec", Types.StringType.get()),
                Types.NestedField.optional(23, "document_certificate_authority_pec", Types.StringType.get()),
                Types.NestedField.optional(24, "document_signature_point_timestamp_pec", Types.StringType.get()),
                Types.NestedField.optional(25, "document_accessibility", Types.StringType.get()),
                Types.NestedField.optional(26, "document_archive_link", Types.StringType.get()),
                Types.NestedField.optional(27, "document_signature_box_order", Types.StringType.get()),
                Types.NestedField.optional(28, "update_timestamp", Types.TimestampType.withZone()),
                Types.NestedField.optional(29, "event_timestamp", Types.TimestampType.withZone())
        );
    }

    public static Schema signer() {

        return new Schema(
                Types.NestedField.optional(1, "signer_id", Types.LongType.get()),
                Types.NestedField.optional(2, "signer_documents_group_id", Types.LongType.get()),
                Types.NestedField.optional(3, "signer_ndg", Types.StringType.get()),
                Types.NestedField.optional(4, "signer_signature_channel", Types.StringType.get()),
                Types.NestedField.optional(5, "signer_authentication_method", Types.StringType.get()),
                Types.NestedField.optional(6, "signer_transaction_id", Types.StringType.get()),
                Types.NestedField.optional(7, "signer_reb", Types.LongType.get()),
                Types.NestedField.optional(8, "signer_document_sign_timestamp", Types.TimestampType.withZone()),
                Types.NestedField.optional(9, "update_timestamp", Types.TimestampType.withZone()),
                Types.NestedField.optional(10, "event_timestamp", Types.TimestampType.withZone())
        );
    }

    public static Schema historyLog() {

        return new Schema(
                Types.NestedField.optional(1, "history_log_id", Types.StringType.get()),
                Types.NestedField.optional(2, "status", Types.StringType.get()),
                Types.NestedField.optional(3, "substatus", Types.StringType.get()),
                Types.NestedField.optional(4, "level_id_identifier", Types.LongType.get()),
                Types.NestedField.optional(5, "level", Types.StringType.get()),
                Types.NestedField.optional(6, "status_timestamp", Types.TimestampType.withZone()),
                Types.NestedField.optional(7, "update_timestamp", Types.TimestampType.withZone()),
                Types.NestedField.optional(8, "event_timestamp", Types.TimestampType.withZone())
        );
    }

    public static Schema discardLog() {

        return new Schema(
                Types.NestedField.optional(1, "payload", Types.StringType.get()),
                Types.NestedField.optional(2, "error", Types.StringType.get()),
                Types.NestedField.optional(3, "processing_time", Types.TimestampType.withZone())
        );
    }
}
