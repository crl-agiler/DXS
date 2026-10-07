package eu.unicredit.document.dxstraceinfo.transform;

import eu.unicredit.document.dxstraceinfo.model.StatusTracked;
import org.apache.flink.api.common.state.MapState;

import java.time.Instant;
import java.util.Optional;

/**
 * Groups consecutive events with the same status into a "run" and tells, for each event,
 * when the run it belongs to started.
 */
public class StatusRunTracker {

    private final MapState<String, StatusRun> runs;

    public StatusRunTracker(MapState<String, StatusRun> runs) {
        this.runs = runs;
    }

    /**
     * @return the start of the run the event belongs to, or empty if the event is older than the
     *     last one already seen for the entity (it must not move the history backwards)
     */
    public Optional<Instant> track(StatusTracked tracked) throws Exception {
        String key = tracked.trackKey();
        String status = tracked.trackStatus();
        long timestamp = tracked.trackTimestamp().toEpochMilli();

        StatusRun run = runs.get(key);

        if (run != null && timestamp < run.lastMillis) {
            return Optional.empty();
        }

        if (run == null || !status.equals(run.status)) {
            run = new StatusRun(status, timestamp, timestamp);   // status changed: new run
        } else {
            run.lastMillis = timestamp;                           // same status: extend the run
        }

        runs.put(key, run);
        return Optional.of(Instant.ofEpochMilli(run.startMillis));
    }
}