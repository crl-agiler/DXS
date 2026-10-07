package eu.unicredit.document.dxstraceinfo.model;

public interface ChangeDetectable {

    /** Identifies the tracked entity. */
    String changeKey();

    /** Current value; a row is written only when it differs from the last seen one. */
    String changeValue();
}