package eu.unicredit.document.dxstraceinfo.kafka;

import eu.unicredit.document.dxstraceinfo.avro.DossierTraceinfoEvent;
import lombok.RequiredArgsConstructor;
import org.apache.flink.api.common.serialization.DeserializationSchema;
import org.apache.flink.api.common.typeinfo.TypeInformation;
import org.apache.flink.connector.kafka.source.reader.deserializer.KafkaRecordDeserializationSchema;
import org.apache.flink.util.Collector;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;

@RequiredArgsConstructor(staticName = "of")
public class SafeKafkaDeserializer
        implements KafkaRecordDeserializationSchema<DossierTraceinfoEvent> {

    private static final Logger LOG =
            LoggerFactory.getLogger(SafeKafkaDeserializer.class);

    private final DeserializationSchema<DossierTraceinfoEvent> delegate;

    @Override
    public void deserialize(
            ConsumerRecord<byte[], byte[]> record,
            Collector<DossierTraceinfoEvent> out) throws IOException {

        try {
            DossierTraceinfoEvent event = delegate.deserialize(record.value());

            if (event != null) {
                out.collect(event);
            }

        } catch (Exception e) {
            LOG.error(
                    "Discarding invalid record. topic={}, partition={}, offset={}",
                    record.topic(),
                    record.partition(),
                    record.offset(),
                    e
            );
        }
    }

    @Override
    public TypeInformation<DossierTraceinfoEvent> getProducedType() {
        return TypeInformation.of(DossierTraceinfoEvent.class);
    }
}