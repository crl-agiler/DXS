package eu.unicredit.document.dxstraceinfo.api;

public interface ModuleContext {

    <T> void register(Class<T> type, T instance);

    <T> T get(Class<T> type);

    <T> T get(Class<T> type, String name);

}
