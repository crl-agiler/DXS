package eu.unicredit.document.dxstraceinfo.extractor;

import eu.unicredit.document.dxstraceinfo.avro.DocumentGroup;
import eu.unicredit.document.dxstraceinfo.avro.DossierTraceinfoEvent;
import eu.unicredit.document.dxstraceinfo.mapping.AvroFixtureLoader;
import eu.unicredit.document.dxstraceinfo.model.HistoryLogRecord;
import org.junit.jupiter.api.Test;

import java.time.Instant;
import java.util.List;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNotEquals;

class HistoryLogExtractorTest {

    private final HistoryLogExtractor extractor = new HistoryLogExtractor();

    private DossierTraceinfoEvent loadEvent() throws Exception {
        return AvroFixtureLoader.load(
                "/fixtures/dossier-ok.json",
                DossierTraceinfoEvent.getClassSchema());
    }

    @Test
    void shouldEmitOneRecordPerEntityWithHierarchicalKeys() throws Exception {
        DossierTraceinfoEvent event = loadEvent();

        List<HistoryLogRecord> records = extractor.map(event);

        long groups = event.getDocumentGroups().size();
        long documents = event.getDocumentGroups().stream()
                .mapToLong(g -> g.getDocuments().size())
                .sum();
        assertEquals(1 + groups + documents, records.size());

        long dossierId = event.getDossierId();

        HistoryLogRecord dossier = records.get(0);
        assertEquals("DOSSIER", dossier.getLevel());
        assertEquals(String.valueOf(dossierId), dossier.getHistoryLogId());
        assertEquals(dossierId, dossier.getLevelIdIdentifier());

        DocumentGroup group = event.getDocumentGroups().get(0);
        HistoryLogRecord groupRecord = records.get(1);
        assertEquals("DOCUMENTS_GROUP", groupRecord.getLevel());
        assertEquals(dossierId + "-" + group.getId(), groupRecord.getHistoryLogId());

        HistoryLogRecord documentRecord = records.get(2);
        assertEquals("DOCUMENT", documentRecord.getLevel());
        assertEquals(
                dossierId + "-" + group.getDocuments().get(0).getId(),
                documentRecord.getHistoryLogId());
    }

    @Test
    void shouldUseEventTimestampAsStatusTimestampForEveryLevel() throws Exception {
        DossierTraceinfoEvent event = loadEvent();
        Instant eventTimestamp = Instant.parse("2026-10-02T10:00:00Z");
        event.setEventTimestamp(eventTimestamp);

        List<HistoryLogRecord> records = extractor.map(event);

        for (HistoryLogRecord record : records) {
            assertEquals(eventTimestamp, record.getStatusTimestamp());
            assertNotEquals(event.getCreationDate(), record.getStatusTimestamp());
        }
    }
}