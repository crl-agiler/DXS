package eu.unicredit.document.dxstraceinfo.api;

import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;

import java.io.IOException;

public interface FlinkConfiguration {

    FlinkConfiguration NONE = env -> {
    };

    void configure(StreamExecutionEnvironment env) throws Exception;

}
