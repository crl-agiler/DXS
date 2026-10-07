package eu.unicredit.document.dxstraceinfo.transform;

import eu.unicredit.document.dxstraceinfo.model.ChangeDetectable;
import org.apache.flink.api.common.state.MapState;

/**
 * Decides whether a record represents a status change, using keyed state that holds the last
 * status seen for each entity.
 */
public class StatusChangeFilter {

    private final MapState<String, String> lastStatusState;

    public StatusChangeFilter(MapState<String, String> lastStatusState) {
        this.lastStatusState = lastStatusState;
    }

    public boolean hasChanged(ChangeDetectable detectable) throws Exception {
        String key = detectable.changeKey();
        String current = detectable.changeValue();
        if (current.equals(lastStatusState.get(key))) {
            return false;
        }
        lastStatusState.put(key, current);
        return true;
    }
}