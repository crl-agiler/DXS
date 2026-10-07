package eu.unicredit.document.dxstraceinfo.transform;

import eu.unicredit.document.dxstraceinfo.api.ErrorHandler;
import eu.unicredit.document.dxstraceinfo.avro.DossierTraceinfoEvent;
import eu.unicredit.document.dxstraceinfo.extractor.HistoryLogExtractor;
import eu.unicredit.document.dxstraceinfo.mapping.AvroFixtureLoader;
import eu.unicredit.document.dxstraceinfo.mapping.HistoryLogMapper;
import org.apache.flink.api.common.typeinfo.Types;
import org.apache.flink.streaming.api.operators.KeyedProcessOperator;
import org.apache.flink.streaming.runtime.streamrecord.StreamRecord;
import org.apache.flink.streaming.util.KeyedOneInputStreamOperatorTestHarness;
import org.apache.flink.table.data.RowData;
import org.apache.flink.table.data.TimestampData;
import org.apache.flink.util.OutputTag;
import org.apache.iceberg.flink.TableLoader;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import java.time.Instant;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.Queue;
import java.util.stream.Collectors;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.mockito.Mockito.mock;

class SplitTransformLogicStatusChangeTest {

    private static final OutputTag<RowData> HISTORY_TAG =
            new OutputTag<RowData>("dxs-traceinfo-history-log") {};

    private static final List<String> ERRORS = new ArrayList<>();

    private KeyedOneInputStreamOperatorTestHarness<Long, DossierTraceinfoEvent, RowData> harness;

    @BeforeEach
    void setUp() throws Exception {
        ERRORS.clear();

        SplitContext<?> historyLogContext =
                SplitContext.of(
                        new HistoryLogExtractor(),
                        new HistoryLogMapper(),
                        HISTORY_TAG,
                        mock(TableLoader.class),
                        Collections.emptyList(),
                        false);

        SplitTransformLogic logic =
                new SplitTransformLogic(
                        List.of(historyLogContext),
                        (record, error, emitter) -> ERRORS.add(error));

        harness =
                new KeyedOneInputStreamOperatorTestHarness<>(
                        new KeyedProcessOperator<>(logic),
                        DossierTraceinfoEvent::getDossierId,
                        Types.LONG);
        harness.open();
    }

    @AfterEach
    void tearDown() throws Exception {
        harness.close();
    }

    @Test
    void shouldWriteDossierRowsOnlyWhenStatusChanges() throws Exception {
        send(100L, "DRAFT", 1_000L);
        send(100L, "DRAFT", 2_000L);       // same status -> skipped
        send(100L, "PROCESSING", 3_000L);  // change
        send(100L, "DRAFT", 4_000L);       // back to a previous status -> still a change

        assertTrue(ERRORS.isEmpty(), "unexpected discards: " + ERRORS);
        assertEquals(
                List.of("DRAFT", "PROCESSING", "DRAFT"),
                statuses("DOSSIER"));
    }

    @Test
    void shouldUseEventTimestampAsStatusTimestamp() throws Exception {
        send(100L, "DRAFT", 1_000L);
        send(100L, "PROCESSING", 3_000L);

        List<Long> timestamps =
                rows("DOSSIER").stream()
                        .map(row -> ((TimestampData) row.getField(5)).getMillisecond())
                        .collect(Collectors.toList());

        assertEquals(List.of(1_000L, 3_000L), timestamps);
    }

    @Test
    void shouldTrackGroupsAndDocumentsToo() throws Exception {
        // the fixture keeps the same group/document statuses in every event,
        // so each group and document must be written exactly once
        send(100L, "DRAFT", 1_000L);
        send(100L, "PROCESSING", 2_000L);
        send(100L, "DRAFT", 3_000L);

        DossierTraceinfoEvent reference = event(100L, "DRAFT", 1_000L);
        long groups = reference.getDocumentGroups().size();
        long documents = reference.getDocumentGroups().stream()
                .mapToLong(g -> g.getDocuments().size())
                .sum();

        assertEquals(groups, rows("DOCUMENTS_GROUP").size());
        assertEquals(documents, rows("DOCUMENT").size());
    }

    @Test
    void shouldKeepStateIndependentPerDossier() throws Exception {
        send(100L, "DRAFT", 1_000L);
        send(200L, "DRAFT", 2_000L);       // same status, different dossier -> written
        send(100L, "DRAFT", 3_000L);       // skipped
        send(200L, "DRAFT", 4_000L);       // skipped

        assertEquals(List.of("DRAFT", "DRAFT"), statuses("DOSSIER"));
    }

    // ---------------------------------------------------------------- helpers

    private void send(long dossierId, String status, long eventTimestampMillis) throws Exception {
        harness.processElement(
                event(dossierId, status, eventTimestampMillis),
                eventTimestampMillis);
    }

    private DossierTraceinfoEvent event(long dossierId, String status, long eventTimestampMillis)
            throws Exception {
        DossierTraceinfoEvent event =
                AvroFixtureLoader.load(
                        "/fixtures/dossier-ok.json",
                        DossierTraceinfoEvent.getClassSchema());
        event.setDossierId(dossierId);
        event.setStatus(status);
        event.setEventTimestamp(Instant.ofEpochMilli(eventTimestampMillis));
        return event;
    }

    private List<RowData> rows(String level) {
        Queue<StreamRecord<RowData>> output = harness.getSideOutput(HISTORY_TAG);
        if (output == null) {
            return Collections.emptyList();
        }
        return output.stream()
                .map(StreamRecord::getValue)
                .filter(row -> level.equals(row.getString(4).toString()))
                .collect(Collectors.toList());
    }

    private List<String> statuses(String level) {
        return rows(level).stream()
                .map(row -> row.getString(1).toString())
                .collect(Collectors.toList());
    }
}