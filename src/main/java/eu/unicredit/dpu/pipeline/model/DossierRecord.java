package eu.unicredit.dpu.pipeline.model;

import lombok.AllArgsConstructor;
import lombok.Builder;
import lombok.Data;
import lombok.NoArgsConstructor;

/**
 * Flat representation of the Dossier entity, ready for upsert on the
 * {@code traceinfo_dossier} Iceberg table.
 *
 * <p>Produced by {@link eu.unicredit.dpu.pipeline.transform.DossierTransformFunction}
 * by flattening the top-level fields of {@code DossierTraceinfoEvent} and
 * computing the {@code outcome} field via status mapping.
 *
 * <p>Equality field for Iceberg upsert: {@code dossierId}.
 */
@Data
@Builder
@NoArgsConstructor
@AllArgsConstructor
public class DossierRecord {

    /** Technical event identifier — kept for traceability, not used as Iceberg key. */
    private String uuid;

    /** Primary key for upsert on traceinfo_dossier. */
    private Long dossierId;

    private String applicationCode;
    private String selectedProcessType;
    private String status;
    private String subStatus;
    private String branch;
    private String bankCode;
    private String correlationKey;
    private String dossierType; // mapped from "dosierType" typo in source Avro

    private Long creationDate;
    private Long endDate;

    /** Technical event timestamp — used for ordering / freshness checks downstream. */
    private Long eventTimestamp;

    /**
     * Computed field: OK / KO / IN_PROGRESS, derived from {@code status}
     * via {@link eu.unicredit.dpu.pipeline.mapping.OutcomeMapper}.
     */
    private String outcome;
}
