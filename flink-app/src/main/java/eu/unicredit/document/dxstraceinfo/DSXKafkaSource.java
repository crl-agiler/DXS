package eu.unicredit.document.dxstraceinfo;

import eu.unicredit.document.dxstraceinfo.api.Source;
import eu.unicredit.document.dxstraceinfo.avro.DossierTraceinfoEvent;
import eu.unicredit.document.dxstraceinfo.config.ConfigApp;
import eu.unicredit.document.dxstraceinfo.credentials.Credentials;
import eu.unicredit.document.dxstraceinfo.credentials.CredentialsRetriever;
import eu.unicredit.document.dxstraceinfo.credentials.MalformedCredentialsException;
import eu.unicredit.document.dxstraceinfo.kafka.DxsKafkaSourceFactory;
import eu.unicredit.document.dxstraceinfo.transform.PreKeyFilterProcess;
import lombok.AllArgsConstructor;
import org.apache.flink.api.common.eventtime.WatermarkStrategy;
import org.apache.flink.connector.kafka.source.KafkaSource;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.datastream.KeyedStream;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.util.OutputTag;

import java.io.IOException;
import java.time.Duration;

@AllArgsConstructor
public class DSXKafkaSource implements Source<DossierTraceinfoEvent> {

    private final ConfigApp configApp;
    private final CredentialsRetriever credentialsRetriever;

    @Override
    public DataStream<DossierTraceinfoEvent> source(StreamExecutionEnvironment env) throws MalformedCredentialsException, IOException {

        KafkaSource<DossierTraceinfoEvent> kafkaSource =
                buildKafkaSource();

        //PreKeyFilterProcess dossierIdNotNullProcess = new PreKeyFilterProcess(DISCARD_TAG);


        return env.fromSource(
                        kafkaSource,
                        createWatermarkStrategy(),
                        "DXS Kafka Source")
                .uid("kafka-source-dxs");
                        //.process(dossierIdNotNullProcess)
                        //.keyBy(DossierTraceinfoEvent::getMasterDossierId);
    }

    private KafkaSource<DossierTraceinfoEvent> buildKafkaSource()
            throws IOException,
            MalformedCredentialsException {

        Credentials credentials =
                credentialsRetriever.getCredentials(
                        configApp.getProjectId(),
                        configApp
                                .getSchemaRegistryConfig()
                                .getSecretId());

        return new DxsKafkaSourceFactory(
                configApp, credentials)
                .build();
    }

    private WatermarkStrategy<DossierTraceinfoEvent> createWatermarkStrategy() {

        return WatermarkStrategy
                .<DossierTraceinfoEvent>
                        forBoundedOutOfOrderness(
                        Duration.ofSeconds(30))
                .withTimestampAssigner(
                        (event, ts) ->
                                event
                                        .getEventTimestamp()
                                        .toEpochMilli());
    }

    @Override
    public void onInit() {

    }
}
