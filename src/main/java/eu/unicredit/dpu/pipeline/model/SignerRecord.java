package eu.unicredit.dpu.pipeline.model;

import lombok.AllArgsConstructor;
import lombok.Builder;
import lombok.Data;
import lombok.NoArgsConstructor;

/**
 * Flat representation of a Signer, exploded out of the
 * {@code documentGroups[].signers} array in {@code DossierTraceinfoEvent}.
 *
 * <p>The source Avro has no native signer identifier — {@code signerId} is
 * generated here as a composite key, per the open question raised with DXS
 * (pending confirmation on whether DXS will add a native field in a future
 * schema version).
 *
 * <p>Equality field for Iceberg upsert: {@code signerId} (composite).
 */
@Data
@Builder
@NoArgsConstructor
@AllArgsConstructor
public class SignerRecord {

    private String uuid;

    /**
     * Composite primary key: {@code dossierId + "-" + documentGroupId + "-" + ndg}.
     * See {@link eu.unicredit.dpu.pipeline.transform.SignerIdGenerator}.
     */
    private String signerId;

    /** Foreign key — parent document group (SIGNER_DOCUMENTS_GROUP_ID in Anna's model). */
    private Long documentGroupId;

    /** Denormalised — parent dossier. */
    private Long dossierId;

    private String ndg;
    private String mfaMethod;
    private String authenticationId;
    private String signatureChannel;

    private Long signatureDate;
    private Long eventTimestamp;
}
