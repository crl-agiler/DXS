package eu.unicredit.document.dxstraceinfo.api;

import lombok.Builder;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;

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
    public void run(DXSContext context) throws Exception {
        source.onInit();
        processor.onInit();
        sink.onInit();
        DataStream<I> source = this.source.source(context);
        DataStream<O> process = processor.process(source, context);
        sink.sink((SingleOutputStreamOperator<O>) process, context);
        context.streamingExecutionEnv().execute(jobName());
    }
}
