package eu.unicredit.dpu.pipeline.transform;

import eu.unicredit.dpu.pipeline.model.TransformedEvent;
import org.apache.avro.Schema;
import org.apache.avro.generic.GenericData;
import org.apache.avro.generic.GenericRecord;
import org.apache.flink.streaming.api.functions.ProcessFunction;
import org.apache.flink.util.Collector;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.mockito.ArgumentCaptor;

import java.util.ArrayList;
import java.util.List;

import static org.assertj.core.api.Assertions.assertThat;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.verify;

/**
 * Tests the core stateless transform: flattening one DossierTraceinfoEvent
 * (with nested DocumentGroups/Documents/Signers) into flat per-entity records.
 *
 * <p>Builds Avro {@link GenericRecord} instances by hand against a minimal
 * schema mirroring DossierTraceinfoEvent.avsc — this keeps the test fast
 * and independent from the full generated schema, while exercising exactly
 * the field names the transform function reads.
 */
class DossierTransformFunctionTest {

    private DossierTransformFunction function;
    private Schema dossierSchema;
    private Schema documentGroupSchema;
    private Schema documentSchema;
    private Schema signerSchema;

    @BeforeEach
    void setUp() throws Exception {
        function = new DossierTransformFunction();
        function.open(new org.apache.flink.configuration.Configuration());
        buildSchemas();
    }

    @Test
    void shouldFlattenFullEventWithNestedEntities() throws Exception {
        // ── Build one signer ──
        GenericRecord signer = new GenericData.Record(signerSchema);
        signer.put("ndg", "0000000022516018");
        signer.put("mfaMethod", "OTP");
        signer.put("authenticationId", "auth-1");
        signer.put("signerReb", null);
        signer.put("signatureChannel", "APP");
        signer.put("signatureDate", 1_718_000_000_000L);

        GenericRecord signer2 = new GenericData.Record(signerSchema);
        signer2.put("ndg", "0000000022516019");
        signer2.put("mfaMethod", "OTP");
        signer2.put("authenticationId", "auth-2");
        signer2.put("signerReb", null);
        signer2.put("signatureChannel", "APP");
        signer2.put("signatureDate", 1_718_000_001_000L);

        // ── Build two documents ──
        GenericRecord doc1 = new GenericData.Record(documentSchema);
        doc1.put("id", 1001L);
        doc1.put("code", "DOC_MUTUO");
        doc1.put("status", "ARCHIVED");
        doc1.put("categoryCode", "CONTRACT");
        doc1.put("productName", "Mutuo");
        doc1.put("processCode", "PROC1");
        doc1.put("creationDate", 1_717_000_000_000L);
        doc1.put("endDate", 1_717_500_000_000L);
        doc1.put("signatureDate", 1_717_400_000_000L);

        GenericRecord doc2 = new GenericData.Record(documentSchema);
        doc2.put("id", 1002L);
        doc2.put("code", "DOC_PRIVACY");
        doc2.put("status", "TO_BE_ARCHIVED");
        doc2.put("categoryCode", "DISCLOSURE");
        doc2.put("productName", "Informativa");
        doc2.put("processCode", "PROC1");
        doc2.put("creationDate", 1_717_000_000_000L);
        doc2.put("endDate", null);
        doc2.put("signatureDate", 1_717_400_000_000L);

        // ── Build one document group ──
        GenericRecord dg = new GenericData.Record(documentGroupSchema);
        dg.put("id", 482119L);
        dg.put("digilayerId", "dl-1");
        dg.put("envelopeId", "env-1");
        dg.put("status", "SIGNED");
        dg.put("creationDate", 1_717_000_000_000L);
        dg.put("endDate", 1_717_500_000_000L);
        dg.put("signers", List.of(signer, signer2));
        dg.put("documents", List.of(doc1, doc2));

        // ── Build the top-level dossier event ──
        GenericRecord event = new GenericData.Record(dossierSchema);
        event.put("uuid", "evt-bbb-222");
        event.put("eventType", "UPDATE");
        event.put("eventTimestamp", 1_717_600_000_000L);
        event.put("dossierId", 1400340L);
        event.put("applicationCode", "ELD");
        event.put("selectedProcessType", "MUTUO");
        event.put("status", "PROCESSING");
        event.put("subStatus", null);
        event.put("branch", "00200");
        event.put("bankCode", "02008");
        event.put("correlationKey", "corr-1");
        event.put("dosierType", "MORTGAGE");
        event.put("creationDate", 1_717_000_000_000L);
        event.put("endDate", null);
        event.put("documentGroups", List.of(dg));

        // ── Run the transform ──
        TransformedEvent result = invokeTransform(event);

        // ── Dossier-level assertions ──
        assertThat(result.getDossier()).isNotNull();
        assertThat(result.getDossier().getDossierId()).isEqualTo(1400340L);
        assertThat(result.getDossier().getUuid()).isEqualTo("evt-bbb-222");
        assertThat(result.getDossier().getStatus()).isEqualTo("PROCESSING");
        assertThat(result.getDossier().getOutcome()).isEqualTo("IN_PROGRESS");
        assertThat(result.getDossier().getDossierType()).isEqualTo("MORTGAGE");
        assertThat(result.getDossier().getEventTimestamp()).isEqualTo(1_717_600_000_000L);

        // ── DocumentGroup-level assertions ──
        assertThat(result.getDocumentGroups()).hasSize(1);
        var dgRecord = result.getDocumentGroups().get(0);
        assertThat(dgRecord.getDocumentGroupId()).isEqualTo(482119L);
        assertThat(dgRecord.getDossierId()).isEqualTo(1400340L);
        assertThat(dgRecord.getStatus()).isEqualTo("SIGNED");
        assertThat(dgRecord.getOutcome()).isEqualTo("OK"); // SIGNED is a terminal-OK status
        assertThat(dgRecord.getSignerCount()).isEqualTo(2);

        // ── Document-level assertions — both documents flattened, signerCount replicated ──
        assertThat(result.getDocuments()).hasSize(2);
        var docRecord1 = result.getDocuments().stream()
                .filter(d -> d.getDocumentId().equals(1001L)).findFirst().orElseThrow();
        assertThat(docRecord1.getStatus()).isEqualTo("ARCHIVED");
        assertThat(docRecord1.getOutcome()).isEqualTo("OK");
        assertThat(docRecord1.getDocumentGroupId()).isEqualTo(482119L);
        assertThat(docRecord1.getDossierId()).isEqualTo(1400340L);
        assertThat(docRecord1.getDocumentSignerNumber()).isEqualTo(2);

        var docRecord2 = result.getDocuments().stream()
                .filter(d -> d.getDocumentId().equals(1002L)).findFirst().orElseThrow();
        assertThat(docRecord2.getStatus()).isEqualTo("TO_BE_ARCHIVED");
        assertThat(docRecord2.getOutcome()).isEqualTo("IN_PROGRESS");

        // ── Signer-level assertions — composite key generated correctly ──
        assertThat(result.getSigners()).hasSize(2);
        var signerRecord1 = result.getSigners().stream()
                .filter(s -> s.getNdg().equals("0000000022516018")).findFirst().orElseThrow();
        assertThat(signerRecord1.getSignerId()).isEqualTo("1400340-482119-0000000022516018");
        assertThat(signerRecord1.getDocumentGroupId()).isEqualTo(482119L);
        assertThat(signerRecord1.getDossierId()).isEqualTo(1400340L);
    }

    @Test
    void shouldHandleDossierWithNoDocumentGroups() throws Exception {
        GenericRecord event = new GenericData.Record(dossierSchema);
        event.put("uuid", "evt-aaa-111");
        event.put("eventType", "INSERT");
        event.put("eventTimestamp", 1_717_000_000_000L);
        event.put("dossierId", 1400341L);
        event.put("applicationCode", "ELD");
        event.put("selectedProcessType", "MUTUO");
        event.put("status", "CREATED");
        event.put("subStatus", null);
        event.put("branch", "00200");
        event.put("bankCode", "02008");
        event.put("correlationKey", "corr-2");
        event.put("dosierType", "MORTGAGE");
        event.put("creationDate", 1_717_000_000_000L);
        event.put("endDate", null);
        event.put("documentGroups", List.of()); // empty — first INSERT event

        TransformedEvent result = invokeTransform(event);

        assertThat(result.getDossier().getDossierId()).isEqualTo(1400341L);
        assertThat(result.getDossier().getOutcome()).isEqualTo("IN_PROGRESS");
        assertThat(result.getDocumentGroups()).isEmpty();
        assertThat(result.getDocuments()).isEmpty();
        assertThat(result.getSigners()).isEmpty();
    }

    @Test
    void shouldNotThrowWhenMalformedEventIsProcessed() throws Exception {
        // Missing required dossierId — processElement must swallow the
        // exception and simply not emit, never crash the job.
        GenericRecord malformed = new GenericData.Record(dossierSchema);
        malformed.put("uuid", "evt-broken");
        malformed.put("eventType", "INSERT");
        malformed.put("eventTimestamp", 1_717_000_000_000L);
        malformed.put("dossierId", null); // invalid — required field
        malformed.put("documentGroups", List.of());

        @SuppressWarnings("unchecked")
        Collector<TransformedEvent> collector = mock(Collector.class);

        function.processElement(malformed, mockContext(), collector);

        // No event should be emitted — verified by zero interactions with collect()
        org.mockito.Mockito.verifyNoInteractions(collector);
    }

    // ── Test helpers ─────────────────────────────────────────────────────────

    @SuppressWarnings("unchecked")
    private TransformedEvent invokeTransform(GenericRecord event) throws Exception {
        Collector<TransformedEvent> collector = mock(Collector.class);
        function.processElement(event, mockContext(), collector);

        ArgumentCaptor<TransformedEvent> captor = ArgumentCaptor.forClass(TransformedEvent.class);
        verify(collector).collect(captor.capture());
        return captor.getValue();
    }

    private ProcessFunction<GenericRecord, TransformedEvent>.Context mockContext() {
        return mock(ProcessFunction.Context.class);
    }

    private void buildSchemas() {
        signerSchema = Schema.createRecord("Signer", null, "eu.unicredit.dxs", false);
        signerSchema.setFields(List.of(
                field("ndg", Schema.create(Schema.Type.STRING)),
                nullableField("mfaMethod"),
                nullableField("authenticationId"),
                nullableField("signerReb"),
                nullableField("signatureChannel"),
                field("signatureDate", Schema.create(Schema.Type.LONG))
        ));

        documentSchema = Schema.createRecord("Document", null, "eu.unicredit.dxs", false);
        documentSchema.setFields(List.of(
                field("id", Schema.create(Schema.Type.LONG)),
                field("code", Schema.create(Schema.Type.STRING)),
                field("status", Schema.create(Schema.Type.STRING)),
                field("categoryCode", Schema.create(Schema.Type.STRING)),
                field("productName", Schema.create(Schema.Type.STRING)),
                field("processCode", Schema.create(Schema.Type.STRING)),
                field("creationDate", Schema.create(Schema.Type.LONG)),
                nullableField("endDate"),
                field("signatureDate", Schema.create(Schema.Type.LONG))
        ));

        documentGroupSchema = Schema.createRecord("DocumentGroup", null, "eu.unicredit.dxs", false);
        documentGroupSchema.setFields(List.of(
                field("id", Schema.create(Schema.Type.LONG)),
                nullableField("digilayerId"),
                nullableField("envelopeId"),
                field("status", Schema.create(Schema.Type.STRING)),
                field("creationDate", Schema.create(Schema.Type.LONG)),
                field("endDate", Schema.create(Schema.Type.LONG)),
                field("signers", Schema.createArray(signerSchema)),
                field("documents", Schema.createArray(documentSchema))
        ));

        dossierSchema = Schema.createRecord("DossierTraceinfoEvent", null, "eu.unicredit.dxs", false);
        dossierSchema.setFields(List.of(
                field("uuid", Schema.create(Schema.Type.STRING)),
                field("eventType", Schema.create(Schema.Type.STRING)),
                field("eventTimestamp", Schema.create(Schema.Type.LONG)),
                nullableField("dossierId", Schema.Type.LONG),
                field("applicationCode", Schema.create(Schema.Type.STRING)),
                field("selectedProcessType", Schema.create(Schema.Type.STRING)),
                field("status", Schema.create(Schema.Type.STRING)),
                nullableField("subStatus"),
                field("branch", Schema.create(Schema.Type.STRING)),
                field("bankCode", Schema.create(Schema.Type.STRING)),
                field("correlationKey", Schema.create(Schema.Type.STRING)),
                field("dosierType", Schema.create(Schema.Type.STRING)),
                field("creationDate", Schema.create(Schema.Type.LONG)),
                nullableField("endDate"),
                field("documentGroups", Schema.createArray(documentGroupSchema))
        ));
    }

    private Schema.Field field(String name, Schema schema) {
        return new Schema.Field(name, schema, null, (Object) null);
    }

    private Schema.Field nullableField(String name) {
        return nullableField(name, Schema.Type.STRING);
    }

    private Schema.Field nullableField(String name, Schema.Type type) {
        Schema nullable = Schema.createUnion(
                Schema.create(Schema.Type.NULL), Schema.create(type));
        return new Schema.Field(name, nullable, null, org.apache.avro.JsonProperties.NULL_VALUE);
    }
}
