package eu.unicredit.dpu.pipeline.mapping;

import java.io.Serializable;
import java.util.Map;
import java.util.Set;

/**
 * Maps a raw DXS status value to the business-facing {@code outcome} field
 * (OK / KO / IN_PROGRESS), as agreed with Anna's data model.
 *
 * <p>The mapping is intentionally externalised into three sets rather than
 * hardcoded as a switch statement, so it can be loaded from configuration
 * (YAML/properties) without a code change if Anna revises the status list.
 *
 * <p>Implements {@link Serializable} so it can be safely captured inside a
 * Flink {@code ProcessFunction} (closure serialization).
 */
public class OutcomeMapper implements Serializable {

    public static final String OUTCOME_OK = "OK";
    public static final String OUTCOME_KO = "KO";
    public static final String OUTCOME_IN_PROGRESS = "IN_PROGRESS";

    private final Set<String> okStatuses;
    private final Set<String> koStatuses;

    public OutcomeMapper(Set<String> okStatuses, Set<String> koStatuses) {
        this.okStatuses = okStatuses;
        this.koStatuses = koStatuses;
    }

    /**
     * Builds the default mapper with the status sets agreed for Dossier,
     * DocumentGroup and Document (they share the same terminal-state vocabulary
     * in the current DXS schema — CONCLUDED/CLOSED/ARCHIVED for OK,
     * EXPIRED/CANCELLED/REJECTED for KO).
     *
     * <p>Pending confirmation from Anna: whether Document-level statuses
     * need a distinct mapping (e.g. DOC_GENERATED is technical, not terminal).
     */
    public static OutcomeMapper defaultMapper() {
        return new OutcomeMapper(
                Set.of("CONCLUDED", "CLOSED", "ARCHIVED", "SIGNED"),
                Set.of("EXPIRED", "CANCELLED", "REJECTED")
        );
    }

    /**
     * Builds a mapper from explicit status→outcome entries, e.g. loaded
     * from a YAML config section. Useful if Anna provides a flat lookup
     * table instead of the OK/KO/default split.
     */
    public static OutcomeMapper fromLookupTable(Map<String, String> statusToOutcome) {
        Set<String> ok = statusToOutcome.entrySet().stream()
                .filter(e -> OUTCOME_OK.equals(e.getValue()))
                .map(Map.Entry::getKey)
                .collect(java.util.stream.Collectors.toSet());
        Set<String> ko = statusToOutcome.entrySet().stream()
                .filter(e -> OUTCOME_KO.equals(e.getValue()))
                .map(Map.Entry::getKey)
                .collect(java.util.stream.Collectors.toSet());
        return new OutcomeMapper(ok, ko);
    }

    /**
     * Returns OK, KO, or IN_PROGRESS for the given raw status.
     * Null or unrecognised statuses default to IN_PROGRESS rather than
     * failing the record — an unknown status should not block the pipeline.
     */
    public String map(String rawStatus) {
        if (rawStatus == null) {
            return OUTCOME_IN_PROGRESS;
        }
        String normalised = rawStatus.trim().toUpperCase();
        if (okStatuses.contains(normalised)) {
            return OUTCOME_OK;
        }
        if (koStatuses.contains(normalised)) {
            return OUTCOME_KO;
        }
        return OUTCOME_IN_PROGRESS;
    }
}
