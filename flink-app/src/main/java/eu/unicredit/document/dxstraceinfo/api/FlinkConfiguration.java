package eu.unicredit.document.dxstraceinfo.api;

public interface FlinkConfiguration {

    FlinkConfiguration NONE = env -> {
    };

    void configure(DXSContext env) throws Exception;

}
