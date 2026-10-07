package eu.unicredit.document.dxstraceinfo.transform;

import eu.unicredit.document.dxstraceinfo.avro.DossierTraceinfoEvent;
import eu.unicredit.document.dxstraceinfo.extractor.HistoryLogExtractor;
import eu.unicredit.document.dxstraceinfo.mapping.AvroFixtureLoader;
import eu.unicredit.document.dxstraceinfo.model.HistoryLogRecord;
import org.apache.flink.api.common.state.MapState;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import java.time.Instant;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.stream.Collectors;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.mockito.ArgumentMatchers.anyString;
import static org.mockito.Mockito.doAnswer;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

class StatusChangeFilterTest {

    private StatusChangeFilter filter;

    @BeforeEach
    @SuppressWarnings("unchecked")
    void setUp() throws Exception {
        Map<String, String> backing = new HashMap<>();
        MapState<String, String> state = mock(MapState.class);

        when(state.get(anyString()))
                .thenAnswer(invocation -> backing.get(invocation.<String>getArgument(0)));
        doAnswer(invocation -> backing.put(
                invocation.<String>getArgument(0),
                invocation.<String>getArgument(1)))
                .when(state).put(anyString(), anyString());

        filter = new StatusChangeFilter(state);
    }

    private HistoryLogRecord record(String level, long id, String status) {
        return new HistoryLogRecord(String.valueOf(id), id, status, null, Instant.EPOCH, level);
    }

    @Test
    void firstEventOfAnEntityIsAlwaysAChange() throws Exception {
        assertEquals(true, filter.hasChanged(record("DOSSIER", 100, "DRAFT")));
    }

    @Test
    void sameStatusInARowIsNotAChange() throws Exception {
        assertEquals(true, filter.hasChanged(record("DOSSIER", 100, "DRAFT")));
        assertEquals(false, filter.hasChanged(record("DOSSIER", 100, "DRAFT")));
        assertEquals(false, filter.hasChanged(record("DOSSIER", 100, "DRAFT")));
    }

    @Test
    void returningToAPreviousStatusIsAChange() throws Exception {
        assertEquals(true, filter.hasChanged(record("DOSSIER", 100, "DRAFT")));
        assertEquals(true, filter.hasChanged(record("DOSSIER", 100, "PROCESSING")));
        assertEquals(true, filter.hasChanged(record("DOSSIER", 100, "DRAFT")));
    }

    @Test
    void entitiesAreTrackedIndependently() throws Exception {
        assertEquals(true, filter.hasChanged(record("DOSSIER", 100, "DRAFT")));
        // same numeric id but different level -> different entity
        assertEquals(true, filter.hasChanged(record("DOCUMENT", 100, "DRAFT")));
        // different id, same level
        assertEquals(true, filter.hasChanged(record("DOSSIER", 200, "DRAFT")));
        // each of them is now unchanged
        assertEquals(false, filter.hasChanged(record("DOSSIER", 100, "DRAFT")));
        assertEquals(false, filter.hasChanged(record("DOCUMENT", 100, "DRAFT")));
        assertEquals(false, filter.hasChanged(record("DOSSIER", 200, "DRAFT")));
    }

    @Test
    void shouldWriteGroupsAndDocumentsOnlyOnceWhenOnlyTheDossierChanges() throws Exception {
        HistoryLogExtractor extractor = new HistoryLogExtractor();
        List<HistoryLogRecord> written = new ArrayList<>();

        for (String status : List.of("DRAFT", "PROCESSING", "DRAFT")) {
            DossierTraceinfoEvent event =
                    AvroFixtureLoader.load(
                            "/fixtures/dossier-ok.json",
                            DossierTraceinfoEvent.getClassSchema());
            event.setStatus(status);

            for (HistoryLogRecord record : extractor.map(event)) {
                if (filter.hasChanged(record)) {
                    written.add(record);
                }
            }
        }

        DossierTraceinfoEvent reference =
                AvroFixtureLoader.load(
                        "/fixtures/dossier-ok.json",
                        DossierTraceinfoEvent.getClassSchema());
        long groups = reference.getDocumentGroups().size();
        long documents = reference.getDocumentGroups().stream()
                .mapToLong(g -> g.getDocuments().size())
                .sum();

        assertEquals(3, countByLevel(written, "DOSSIER"));
        assertEquals(groups, countByLevel(written, "DOCUMENTS_GROUP"));
        assertEquals(documents, countByLevel(written, "DOCUMENT"));
        assertEquals(
                List.of("DRAFT", "PROCESSING", "DRAFT"),
                written.stream()
                        .filter(r -> "DOSSIER".equals(r.getLevel()))
                        .map(HistoryLogRecord::getStatus)
                        .collect(Collectors.toList()));
    }

    private long countByLevel(List<HistoryLogRecord> records, String level) {
        return records.stream().filter(r -> level.equals(r.getLevel())).count();
    }
}