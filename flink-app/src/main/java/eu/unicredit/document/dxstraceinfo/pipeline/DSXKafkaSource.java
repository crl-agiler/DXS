package eu.unicredit.document.dxstraceinfo.pipeline;

import eu.unicredit.document.dxstraceinfo.api.DXSContext;
import eu.unicredit.document.dxstraceinfo.api.Source;
import eu.unicredit.document.dxstraceinfo.avro.DossierTraceinfoEvent;
import eu.unicredit.document.dxstraceinfo.credentials.MalformedCredentialsException;
import eu.unicredit.document.dxstraceinfo.kafka.DxsKafkaSourceFactory;
import lombok.extern.slf4j.Slf4j;
import org.apache.flink.api.common.eventtime.WatermarkStrategy;
import org.apache.flink.connector.kafka.source.KafkaSource;
import org.apache.flink.streaming.api.datastream.DataStream;

import java.io.IOException;
import java.time.Duration;

@Slf4j
public class DSXKafkaSource implements Source<DossierTraceinfoEvent> {

    @Override
    public DataStream<DossierTraceinfoEvent> source(DXSContext context) throws MalformedCredentialsException, IOException {

        KafkaSource<DossierTraceinfoEvent> kafkaSource = new DxsKafkaSourceFactory(context.config(), context.credentials()).build();

        var env = context.streamingExecutionEnv();
        return env.fromSource(kafkaSource, createWatermarkStrategy(), "DXS Kafka Source").uid("kafka-source-dxs");
        //.process(dossierIdNotNullProcess)
        //.keyBy(DossierTraceinfoEvent::getMasterDossierId);
    }

    private WatermarkStrategy<DossierTraceinfoEvent> createWatermarkStrategy() {

        return WatermarkStrategy.<DossierTraceinfoEvent>forBoundedOutOfOrderness(Duration.ofSeconds(30)).withTimestampAssigner((event, ts) -> event.getEventTimestamp().toEpochMilli());
    }

    @Override
    public void onInit() {
        log.info("Initializing DSX Source");
    }
}
