package eu.unicredit.document.dxstraceinfo.api;

public interface AdditionalContextProperty {
    String name();
    <T> T instance(DXSContext context);
}
