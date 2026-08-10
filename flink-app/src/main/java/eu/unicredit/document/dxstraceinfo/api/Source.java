package eu.unicredit.document.dxstraceinfo.api;

import eu.unicredit.document.dxstraceinfo.credentials.MalformedCredentialsException;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;

import java.io.IOException;

public interface Source<I> extends InitHook {
    DataStream<I> source(StreamExecutionEnvironment env) throws MalformedCredentialsException, IOException;
}
