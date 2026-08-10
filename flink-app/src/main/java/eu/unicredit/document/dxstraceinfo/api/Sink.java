package eu.unicredit.document.dxstraceinfo.api;

import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;

public interface Sink<O> extends InitHook {

    void sink(SingleOutputStreamOperator<O> dataStream);

}
