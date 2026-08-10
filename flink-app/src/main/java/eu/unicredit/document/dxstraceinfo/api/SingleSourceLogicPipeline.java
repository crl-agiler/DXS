package eu.unicredit.document.dxstraceinfo.api;

import lombok.Builder;

import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;

@Builder
public class SingleSourceLogicPipeline<I, O> implements Pipeline {

    private final String jobName;
    private final Source<I> source;
    private final Processor<I, O> processor;
    private final Sink<O> sink;

    @Override
    public String jobName() {
        return jobName;
    }

    @Override
    public void run(StreamExecutionEnvironment streamExecutionEnvironment) throws Exception {
        source.onInit();
        DataStream<I> source = this.source.source(streamExecutionEnvironment);
        DataStream<O> process = processor.process(source);
        sink.sink((SingleOutputStreamOperator<O>) process);
        streamExecutionEnvironment.execute(jobName());
    }
}
