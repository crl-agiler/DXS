package eu.unicredit.document.dxstraceinfo.api;

public interface Bootstrap<T> {
    T bootstrap(String[] args) throws Exception;
}
