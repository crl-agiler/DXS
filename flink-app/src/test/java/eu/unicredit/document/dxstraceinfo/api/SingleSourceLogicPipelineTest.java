package eu.unicredit.document.dxstraceinfo.api;

import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.junit.jupiter.api.Test;
import org.mockito.InOrder;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.mockito.Mockito.inOrder;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

class SingleSourceLogicPipelineTest {

    @Test
    void shouldReturnJobName() {

        SingleSourceLogicPipeline<String, Integer> pipeline =
                SingleSourceLogicPipeline.<String, Integer>builder()
                        .jobName("dxs-traceinfo-job")
                        .source(mock(Source.class))
                        .processor(mock(Processor.class))
                        .sink(mock(Sink.class))
                        .build();

        assertEquals(
                "dxs-traceinfo-job",
                pipeline.jobName()
        );
    }

    @Test
    @SuppressWarnings({"rawtypes", "unchecked"})
    void shouldExecutePipeline() throws Exception {

        Source<String> source =
                mock(Source.class);

        Processor<String, Integer> processor =
                mock(Processor.class);

        Sink<Integer> sink =
                mock(Sink.class);

        DXSContext context =
                mock(DXSContext.class);

        StreamExecutionEnvironment env =
                mock(StreamExecutionEnvironment.class);

        DataStream<String> sourceStream =
                mock(DataStream.class);

        SingleOutputStreamOperator<Integer> processedStream =
                mock(SingleOutputStreamOperator.class);

        when(context.streamingExecutionEnv())
                .thenReturn(env);

        when(source.source(context))
                .thenReturn(sourceStream);

        when(processor.process(
                sourceStream,
                context))
                .thenReturn(processedStream);

        SingleSourceLogicPipeline<String, Integer> pipeline =
                SingleSourceLogicPipeline.<String, Integer>builder()
                        .jobName("dxs-traceinfo-job")
                        .source(source)
                        .processor(processor)
                        .sink(sink)
                        .build();

        pipeline.run(context);

        InOrder inOrder =
                inOrder(source, processor, sink);

        inOrder.verify(source).onInit();
        inOrder.verify(processor).onInit();
        inOrder.verify(sink).onInit();

        verify(source)
                .source(context);

        verify(processor)
                .process(
                        sourceStream,
                        context
                );

        verify(sink)
                .sink(
                        processedStream,
                        context
                );

        verify(env)
                .execute(
                        "dxs-traceinfo-job"
                );
    }
}