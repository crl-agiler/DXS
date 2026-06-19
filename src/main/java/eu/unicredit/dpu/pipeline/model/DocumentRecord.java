package eu.unicredit.dpu.pipeline.model;

import lombok.AllArgsConstructor;
import lombok.Builder;
import lombok.Data;
import lombok.NoArgsConstructor;

/**
 * Flat representation of a Document, exploded out of the
 * {@code documentGroups[].documents} array in {@code DossierTraceinfoEvent}.
 *
 * <p>Equality field for Iceberg upsert: {@code documentId}.
 */
@Data
@Builder
@NoArgsConstructor
@AllArgsConstructor
public class DocumentRecord {

    private String uuid;

    /** Primary key for upsert on traceinfo_document. */
    private Long documentId;

    /** Foreign key — parent document group. */
    private Long documentGroupId;

    /** Denormalised — parent dossier, useful for direct filtering without a join. */
    private Long dossierId;

    private String code;
    private String status;
    private String categoryCode;
    private String productName;
    private String processCode;

    private Long creationDate;
    private Long endDate;
    private Long signatureDate;
    private Long eventTimestamp;

    /** Computed field: OK / KO / IN_PROGRESS. */
    private String outcome;

    /** Replicated from the parent DocumentGroup — see DocumentGroupRecord.signerCount. */
    private Integer documentSignerNumber;
}
