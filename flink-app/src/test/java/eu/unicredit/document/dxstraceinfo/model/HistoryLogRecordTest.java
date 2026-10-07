package eu.unicredit.document.dxstraceinfo.model;

import org.junit.jupiter.api.Test;

import java.time.Instant;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNotEquals;

class HistoryLogRecordTest {

    @Test
    void shouldExposeLevelAndIdAsChangeKeyAndStatusAsChangeValue() {
        HistoryLogRecord record =
                new HistoryLogRecord("100", 100L, "DRAFT", null, Instant.EPOCH, "DOSSIER");

        assertEquals("DOSSIER|100", record.changeKey());
        assertEquals("DRAFT", record.changeValue());
    }

    @Test
    void shouldExposeTrackingData() {
        Instant ts = Instant.parse("2026-10-02T10:00:00Z");
        HistoryLogRecord record =
                new HistoryLogRecord("100", 100L, "DRAFT", null, ts, "DOSSIER");

        assertEquals("DOSSIER|100", record.trackKey());
        assertEquals("DRAFT", record.trackStatus());
        assertEquals(ts, record.trackTimestamp());
    }

    @Test
    void shouldNotCollideBetweenLevelsWithSameNumericId() {
        HistoryLogRecord group =
                new HistoryLogRecord("1-7", 7L, "CREATED", null, Instant.EPOCH, "DOCUMENTS_GROUP");
        HistoryLogRecord document =
                new HistoryLogRecord("1-7", 7L, "CREATED", null, Instant.EPOCH, "DOCUMENT");

        assertNotEquals(group.trackKey(), document.trackKey());
    }
}