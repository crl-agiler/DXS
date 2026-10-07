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
    void shouldNotCollideBetweenLevelsWithSameNumericId() {
        HistoryLogRecord group =
                new HistoryLogRecord("1-7", 7L, "CREATED", null, Instant.EPOCH, "DOCUMENTS_GROUP");
        HistoryLogRecord document =
                new HistoryLogRecord("1-7", 7L, "CREATED", null, Instant.EPOCH, "DOCUMENT");

        assertNotEquals(group.changeKey(), document.changeKey());
    }
}