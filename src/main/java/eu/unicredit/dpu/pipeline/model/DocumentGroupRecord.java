package eu.unicredit.dpu.pipeline.model;

import lombok.AllArgsConstructor;
import lombok.Builder;
import lombok.Data;
import lombok.NoArgsConstructor;

/**
 * Flat representation of a DocumentGroup, exploded out of the
 * {@code documentGroups} array in {@code DossierTraceinfoEvent}.
 *
 * <p>Equality field for Iceberg upsert: {@code documentGroupId}.
 */
@Data
@Builder
@NoArgsConstructor
@AllArgsConstructor
public class DocumentGroupRecord {

    private String uuid;

    /** Primary key for upsert on traceinfo_document_group. */
    private Long documentGroupId;

    /** Foreign key — parent dossier. */
    private Long dossierId;

    private String digilayerId;
    private String envelopeId;
    private String status;

    private Long creationDate;
    private Long endDate;
    private Long eventTimestamp;

    /** Computed field: OK / KO / IN_PROGRESS. */
    private String outcome;

    /**
     * Number of signers attached to this DocumentGroup.
     * Replicated on every Document row of the group (DOCUMENT_SIGNER_NUMBER
     * requirement from Anna's data model) but stored here at group level.
     */
    private Integer signerCount;
}
