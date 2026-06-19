package eu.unicredit.dpu.pipeline.transform;

/**
 * Generates the composite {@code signerId} used as Iceberg equality field
 * for {@code traceinfo_signer}.
 *
 * <p>The source Avro schema has no native signer identifier (DXS confirmed
 * the Signer entity only carries {@code ndg} plus signature metadata).
 * Anna's data model requires SIGNER_ID as primary key and
 * SIGNER_DOCUMENTS_GROUP_ID as foreign key — pending confirmation from DXS
 * on whether these will be added natively to the Avro schema (open question,
 * see DXS_DPU_Open_Questions doc, point 2.3).
 *
 * <p>Until that is confirmed, the key is derived deterministically from
 * fields already present, so the same signer always maps to the same row
 * on every event that includes it — required for Iceberg upsert correctness.
 */
public final class SignerIdGenerator {

    private static final String SEPARATOR = "-";

    private SignerIdGenerator() {
        // utility class
    }

    /**
     * Builds the composite signerId: {@code dossierId-documentGroupId-ndg}.
     *
     * <p>This combination is unique because a given {@code ndg} can sign
     * multiple DocumentGroups (even within the same Dossier, e.g. an
     * addendum signed later by the same person) — the composite key
     * correctly creates one row per (dossier, group, signer) combination
     * rather than collapsing them.
     */
    public static String generate(long dossierId, long documentGroupId, String ndg) {
        if (ndg == null || ndg.isBlank()) {
            throw new IllegalArgumentException(
                    "Cannot generate signerId: ndg is null or blank for dossierId="
                            + dossierId + ", documentGroupId=" + documentGroupId);
        }
        return dossierId + SEPARATOR + documentGroupId + SEPARATOR + ndg.trim();
    }
}
