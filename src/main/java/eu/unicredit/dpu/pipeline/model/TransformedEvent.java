package eu.unicredit.dpu.pipeline.model;

import lombok.AllArgsConstructor;
import lombok.Builder;
import lombok.Data;
import lombok.NoArgsConstructor;

import java.util.List;

/**
 * Container holding every flat record produced by transforming a single
 * {@code DossierTraceinfoEvent}.
 *
 * <p>One source event (one Dossier snapshot) always yields exactly one
 * {@link DossierRecord}, plus zero or more {@link DocumentGroupRecord},
 * {@link DocumentRecord} and {@link SignerRecord} — one per nested element
 * present in the event's arrays.
 *
 * <p>This is the output type of {@link eu.unicredit.dpu.pipeline.transform.DossierTransformFunction}.
 * Downstream, a side-output split routes each list to its own Iceberg sink.
 */
@Data
@Builder
@NoArgsConstructor
@AllArgsConstructor
public class TransformedEvent {

    private DossierRecord dossier;
    private List<DocumentGroupRecord> documentGroups;
    private List<DocumentRecord> documents;
    private List<SignerRecord> signers;
}
