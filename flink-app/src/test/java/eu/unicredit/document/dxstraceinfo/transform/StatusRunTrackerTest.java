package eu.unicredit.document.dxstraceinfo.transform;

import eu.unicredit.document.dxstraceinfo.model.HistoryLogRecord;
import org.apache.flink.api.common.state.MapState;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import java.time.Instant;
import java.util.HashMap;
import java.util.Map;
import java.util.Optional;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.anyString;
import static org.mockito.Mockito.doAnswer;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

class StatusRunTrackerTest {

    private StatusRunTracker tracker;

    @BeforeEach
    @SuppressWarnings("unchecked")
    void setUp() throws Exception {
        Map<String, StatusRun> backing = new HashMap<>();
        MapState<String, StatusRun> state = mock(MapState.class);

        when(state.get(anyString()))
                .thenAnswer(invocation -> backing.get(invocation.<String>getArgument(0)));
        doAnswer(invocation -> backing.put(
                invocation.<String>getArgument(0),
                invocation.<StatusRun>getArgument(1)))
                .when(state).put(anyString(), any(StatusRun.class));

        tracker = new StatusRunTracker(state);
    }

    private Optional<Instant> track(String level, long id, String status, long millis)
            throws Exception {
        return tracker.track(
                new HistoryLogRecord(
                        String.valueOf(id), id, status, null, Instant.ofEpochMilli(millis), level));
    }

    private Optional<Instant> start(long millis) {
        return Optional.of(Instant.ofEpochMilli(millis));
    }

    @Test
    void sameStatusKeepsTheStartOfTheRun() throws Exception {
        assertEquals(start(1_000), track("DOSSIER", 100, "DRAFT", 1_000));
        assertEquals(start(1_000), track("DOSSIER", 100, "DRAFT", 2_000));
        assertEquals(start(1_000), track("DOSSIER", 100, "DRAFT", 3_000));
    }

    @Test
    void statusChangeStartsANewRun() throws Exception {
        assertEquals(start(1_000), track("DOSSIER", 100, "DRAFT", 1_000));
        assertEquals(start(2_000), track("DOSSIER", 100, "PROCESSING", 2_000));
    }

    @Test
    void returningToAPreviousStatusStartsANewRun() throws Exception {
        track("DOSSIER", 100, "DRAFT", 1_000);
        track("DOSSIER", 100, "PROCESSING", 2_000);
        assertEquals(start(3_000), track("DOSSIER", 100, "DRAFT", 3_000));
    }

    @Test
    void olderEventsAreIgnored() throws Exception {
        track("DOSSIER", 100, "DRAFT", 2_000);

        assertEquals(Optional.empty(), track("DOSSIER", 100, "DRAFT", 1_000));
        assertEquals(Optional.empty(), track("DOSSIER", 100, "PROCESSING", 1_500));
        // the run is still the original one
        assertEquals(start(2_000), track("DOSSIER", 100, "DRAFT", 3_000));
    }

    @Test
    void redeliveredEventIsIdempotent() throws Exception {
        assertEquals(start(1_000), track("DOSSIER", 100, "DRAFT", 1_000));
        assertEquals(start(1_000), track("DOSSIER", 100, "DRAFT", 1_000));
    }

    @Test
    void entitiesAreTrackedIndependently() throws Exception {
        assertEquals(start(1_000), track("DOSSIER", 100, "DRAFT", 1_000));
        assertEquals(start(2_000), track("DOCUMENT", 100, "DRAFT", 2_000));
        assertEquals(start(3_000), track("DOSSIER", 200, "DRAFT", 3_000));
        assertEquals(start(1_000), track("DOSSIER", 100, "DRAFT", 4_000));
    }
}