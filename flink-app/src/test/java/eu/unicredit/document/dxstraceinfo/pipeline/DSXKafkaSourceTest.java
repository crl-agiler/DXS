package eu.unicredit.document.dxstraceinfo.pipeline;

import eu.unicredit.document.dxstraceinfo.api.DXSContext;
import eu.unicredit.document.dxstraceinfo.avro.DossierTraceinfoEvent;
import eu.unicredit.document.dxstraceinfo.config.pojo.ConfigApp;
import eu.unicredit.document.dxstraceinfo.credentials.Credentials;
import eu.unicredit.document.dxstraceinfo.kafka.DxsKafkaSourceFactory;
import org.apache.flink.api.common.eventtime.WatermarkStrategy;
import org.apache.flink.connector.kafka.source.KafkaSource;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.junit.jupiter.api.Test;
import org.mockito.MockedConstruction;

import java.lang.reflect.Method;

import static org.junit.jupiter.api.Assertions.assertDoesNotThrow;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertSame;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.eq;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.mockConstruction;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

class DSXKafkaSourceTest {

    @Test
    void shouldInitializeSource() {

        DSXKafkaSource source = new DSXKafkaSource();

        assertDoesNotThrow(source::onInit);
    }

    @Test
    @SuppressWarnings("unchecked")
    void shouldCreateDataStreamFromKafkaSource() throws Exception {

        DXSContext context = mock(DXSContext.class);

        ConfigApp config = mock(ConfigApp.class);
        Credentials credentials = mock(Credentials.class);

        StreamExecutionEnvironment env =
                mock(StreamExecutionEnvironment.class);

        KafkaSource<DossierTraceinfoEvent> kafkaSource =
                mock(KafkaSource.class);

        DataStreamSource<DossierTraceinfoEvent> dataStream =
                mock(DataStreamSource.class);

        when(context.config()).thenReturn(config);
        when(context.credentials()).thenReturn(credentials);
        when(context.streamingExecutionEnv()).thenReturn(env);

        when(env.fromSource(
                any(KafkaSource.class),
                any(WatermarkStrategy.class),
                eq("DXS Kafka Source")))
                .thenReturn(dataStream);

        when(dataStream.uid("kafka-source-dxs"))
                .thenReturn(dataStream);

        try (MockedConstruction<DxsKafkaSourceFactory> mocked =
                     mockConstruction(
                             DxsKafkaSourceFactory.class,
                             (mock, ctx) ->
                                     when(mock.build())
                                             .thenReturn(kafkaSource))) {

            DSXKafkaSource source =
                    new DSXKafkaSource();

            DataStream<DossierTraceinfoEvent> result =
                    source.source(context);

            assertSame(dataStream, result);

            verify(env)
                    .fromSource(
                            eq(kafkaSource),
                            any(WatermarkStrategy.class),
                            eq("DXS Kafka Source"));

            verify(dataStream)
                    .uid("kafka-source-dxs");

            verify(mocked.constructed().get(0))
                    .build();
        }
    }

    @Test
    @SuppressWarnings("unchecked")
    void shouldCreateWatermarkStrategy() throws Exception {

        DSXKafkaSource source =
                new DSXKafkaSource();

        Method method =
                DSXKafkaSource.class.getDeclaredMethod(
                        "createWatermarkStrategy");

        method.setAccessible(true);

        WatermarkStrategy<DossierTraceinfoEvent> strategy =
                (WatermarkStrategy<DossierTraceinfoEvent>)
                        method.invoke(source);

        assertNotNull(strategy);
    }
}