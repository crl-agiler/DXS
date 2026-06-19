package eu.unicredit.dpu.pipeline.transform;

import eu.unicredit.dpu.pipeline.mapping.OutcomeMapper;
import eu.unicredit.dpu.pipeline.model.*;
import org.apache.avro.generic.GenericRecord;
import org.apache.flink.api.common.functions.RuntimeContext;
import org.apache.flink.streaming.api.functions.ProcessFunction;
import org.apache.flink.util.Collector;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.ArrayList;
import java.util.List;

/**
 * Stateless transform stage of the DXS DPU pipeline.
 *
 * <p>Takes one {@code DossierTraceinfoEvent} (a full snapshot of a Dossier,
 * with nested DocumentGroups/Documents/Signers) and flattens it into a
 * {@link TransformedEvent} containing one {@link DossierRecord} plus a list
 * of flat records per nested entity — ready for direct upsert on Iceberg.
 *
 * <h3>Why stateless</h3>
 * This function does NOT compare the incoming event against any previous
 * snapshot. Per the architectural confirmations obtained from DXS:
 * <ul>
 *   <li>No physical deletes at any entity level (logical CDC only)</li>
 *   <li>No duplicates expected (transactional producer)</li>
 * </ul>
 * every field present in the event is emitted as-is; Iceberg's upsert
 * (equality field = entity ID) takes care of "last write wins" semantics.
 *
 * <p>Per-row business filtering (e.g. "only emit a Document row if its
 * status actually changed since last time") is intentionally NOT done here
 * — that requirement is still being clarified with DXS/Business (see Open
 * Questions doc, point 3) and, if confirmed, will be implemented as a
 * separate batch stage comparing Iceberg snapshots, not inside this
 * streaming transform. Keeping this function side-effect-free and
 * comparison-free is what keeps the whole job stateless.
 */
public class DossierTransformFunction extends ProcessFunction<GenericRecord, TransformedEvent> {

    private static final Logger LOG = LoggerFactory.getLogger(DossierTransformFunction.class);

    private transient OutcomeMapper outcomeMapper;

    @Override
    public void open(org.apache.flink.configuration.Configuration parameters) {
        // Default mapper for now — see OutcomeMapper for how to load
        // a YAML-driven lookup table instead, once Anna's mapping is final.
        this.outcomeMapper = OutcomeMapper.defaultMapper();
    }

    @Override
    public void processElement(GenericRecord event, Context ctx, Collector<TransformedEvent> out) {
        try {
            TransformedEvent transformed = transform(event);
            out.collect(transformed);
        } catch (Exception e) {
            // A single malformed event must not kill the job. Log with enough
            // context to find it again (uuid, dossierId) and move on —
            // this is a stateless pipeline, there is no state to roll back.
            LOG.error("Failed to transform event — uuid={}, dossierId={}. Skipping. Cause: {}",
                    safeGet(event, "uuid"), safeGet(event, "dossierId"), e.getMessage(), e);
        }
    }

    // ── Core transform logic ────────────────────────────────────────────────

    private TransformedEvent transform(GenericRecord event) {
        String uuid = asString(event.get("uuid"));
        Long eventTimestamp = asLong(event.get("eventTimestamp"));
        long dossierId = requireLong(event.get("dossierId"), "dossierId");

        DossierRecord dossierRecord = buildDossierRecord(event, uuid, dossierId, eventTimestamp);

        List<DocumentGroupRecord> documentGroupRecords = new ArrayList<>();
        List<DocumentRecord> documentRecords = new ArrayList<>();
        List<SignerRecord> signerRecords = new ArrayList<>();

        @SuppressWarnings("unchecked")
        List<GenericRecord> documentGroups = (List<GenericRecord>) event.get("documentGroups");

        if (documentGroups != null) {
            for (GenericRecord dg : documentGroups) {
                long documentGroupId = requireLong(dg.get("id"), "documentGroup.id");

                @SuppressWarnings("unchecked")
                List<GenericRecord> signers = (List<GenericRecord>) dg.get("signers");
                int signerCount = signers == null ? 0 : signers.size();

                documentGroupRecords.add(
                        buildDocumentGroupRecord(dg, uuid, documentGroupId, dossierId, eventTimestamp, signerCount));

                if (signers != null) {
                    for (GenericRecord signer : signers) {
                        signerRecords.add(
                                buildSignerRecord(signer, uuid, dossierId, documentGroupId, eventTimestamp));
                    }
                }

                @SuppressWarnings("unchecked")
                List<GenericRecord> documents = (List<GenericRecord>) dg.get("documents");
                if (documents != null) {
                    for (GenericRecord doc : documents) {
                        documentRecords.add(
                                buildDocumentRecord(doc, uuid, dossierId, documentGroupId, eventTimestamp, signerCount));
                    }
                }
            }
        }

        return TransformedEvent.builder()
                .dossier(dossierRecord)
                .documentGroups(documentGroupRecords)
                .documents(documentRecords)
                .signers(signerRecords)
                .build();
    }

    private DossierRecord buildDossierRecord(GenericRecord event, String uuid, long dossierId, Long eventTimestamp) {
        String status = asString(event.get("status"));
        return DossierRecord.builder()
                .uuid(uuid)
                .dossierId(dossierId)
                .applicationCode(asString(event.get("applicationCode")))
                .selectedProcessType(asString(event.get("selectedProcessType")))
                .status(status)
                .subStatus(asString(event.get("subStatus")))
                .branch(asString(event.get("branch")))
                .bankCode(asString(event.get("bankCode")))
                .correlationKey(asString(event.get("correlationKey")))
                .dossierType(asString(event.get("dosierType"))) // note: source field has the typo
                .creationDate(asLong(event.get("creationDate")))
                .endDate(asLong(event.get("endDate")))
                .eventTimestamp(eventTimestamp)
                .outcome(outcomeMapper.map(status))
                .build();
    }

    private DocumentGroupRecord buildDocumentGroupRecord(
            GenericRecord dg, String uuid, long documentGroupId, long dossierId,
            Long eventTimestamp, int signerCount) {
        String status = asString(dg.get("status"));
        return DocumentGroupRecord.builder()
                .uuid(uuid)
                .documentGroupId(documentGroupId)
                .dossierId(dossierId)
                .digilayerId(asString(dg.get("digilayerId")))
                .envelopeId(asString(dg.get("envelopeId")))
                .status(status)
                .creationDate(asLong(dg.get("creationDate")))
                .endDate(asLong(dg.get("endDate")))
                .eventTimestamp(eventTimestamp)
                .outcome(outcomeMapper.map(status))
                .signerCount(signerCount)
                .build();
    }

    private DocumentRecord buildDocumentRecord(
            GenericRecord doc, String uuid, long dossierId, long documentGroupId,
            Long eventTimestamp, int signerCount) {
        long documentId = requireLong(doc.get("id"), "document.id");
        String status = asString(doc.get("status"));
        return DocumentRecord.builder()
                .uuid(uuid)
                .documentId(documentId)
                .documentGroupId(documentGroupId)
                .dossierId(dossierId)
                .code(asString(doc.get("code")))
                .status(status)
                .categoryCode(asString(doc.get("categoryCode")))
                .productName(asString(doc.get("productName")))
                .processCode(asString(doc.get("processCode")))
                .creationDate(asLong(doc.get("creationDate")))
                .endDate(asLong(doc.get("endDate")))
                .signatureDate(asLong(doc.get("signatureDate")))
                .eventTimestamp(eventTimestamp)
                .outcome(outcomeMapper.map(status))
                .documentSignerNumber(signerCount)
                .build();
    }

    private SignerRecord buildSignerRecord(
            GenericRecord signer, String uuid, long dossierId, long documentGroupId, Long eventTimestamp) {
        String ndg = asString(signer.get("ndg"));
        String signerId = SignerIdGenerator.generate(dossierId, documentGroupId, ndg);
        return SignerRecord.builder()
                .uuid(uuid)
                .signerId(signerId)
                .documentGroupId(documentGroupId)
                .dossierId(dossierId)
                .ndg(ndg)
                .mfaMethod(asString(signer.get("mfaMethod")))
                .authenticationId(asString(signer.get("authenticationId")))
                .signatureChannel(asString(signer.get("signatureChannel")))
                .signatureDate(asLong(signer.get("signatureDate")))
                .eventTimestamp(eventTimestamp)
                .build();
    }

    // ── Avro field extraction helpers ───────────────────────────────────────
    // GenericRecord.get() returns Object (or Utf8 for strings) — these
    // helpers normalise to plain Java types and tolerate nulls/missing fields,
    // which matters because nested optional fields are common in this schema
    // and initial-load replay events may carry an older schema version.

    private static String asString(Object value) {
        return value == null ? null : value.toString();
    }

    private static Long asLong(Object value) {
        if (value == null) return null;
        if (value instanceof Long) return (Long) value;
        if (value instanceof Number) return ((Number) value).longValue();
        return Long.parseLong(value.toString());
    }

    private static long requireLong(Object value, String fieldName) {
        Long result = asLong(value);
        if (result == null) {
            throw new IllegalStateException("Required field '" + fieldName + "' is null or missing.");
        }
        return result;
    }

    private static Object safeGet(GenericRecord record, String field) {
        try {
            return record.get(field);
        } catch (Exception e) {
            return "<unavailable>";
        }
    }
}
