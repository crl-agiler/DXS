package eu.unicredit.document.dxstraceinfo.model;

import java.time.Instant;

public interface StatusTracked {

    /** Identifies the tracked entity. */
    String trackKey();

    /** Current status of the entity. */
    String trackStatus();

    /** Timestamp of the event that carried this status. */
    Instant trackTimestamp();
}