package eu.unicredit.document.dxstraceinfo.api;

import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;

public interface Pipeline {

    String jobName();

    void run(StreamExecutionEnvironment streamExecutionEnvironment) throws Exception;

}
