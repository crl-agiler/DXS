package eu.unicredit.document.dxstraceinfo.api;

import org.apache.flink.streaming.api.datastream.DataStream;

public interface Processor<I, O> extends InitHook {

    DataStream<O> process(DataStream<I> input);

}
