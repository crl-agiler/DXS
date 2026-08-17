package eu.unicredit.document.dxstraceinfo.it;

import eu.unicredit.document.dxstraceinfo.avro.DossierTraceinfoEvent;
import lombok.extern.slf4j.Slf4j;
import org.apache.avro.io.BinaryEncoder;
import org.apache.avro.io.DatumWriter;
import org.apache.avro.io.EncoderFactory;
import org.apache.avro.specific.SpecificData;
import org.apache.avro.specific.SpecificDatumWriter;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.common.serialization.ByteArraySerializer;
import org.apache.kafka.common.serialization.StringSerializer;

import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.util.List;
import java.util.Objects;
import java.util.Properties;
import java.util.concurrent.TimeUnit;

@Slf4j
public final class DossierKafkaPublisher {

  private final String bootstrapServers;
  private final String topic;
  private final int schemaId;

  DossierKafkaPublisher(
      String bootstrapServers,
      String topic,
      int schemaId) {

    this.bootstrapServers =
        Objects.requireNonNull(
            bootstrapServers,
            "bootstrapServers must not be null");

    this.topic =
        Objects.requireNonNull(
            topic,
            "topic must not be null");

    this.schemaId = schemaId;
  }

  void publish(
      List<DossierTraceinfoEvent> events)
      throws Exception {

    try (KafkaProducer<String, byte[]> producer =
             new KafkaProducer<>(
                 producerProperties())) {

      for (DossierTraceinfoEvent event : events) {
        String key =
            String.valueOf(
                event.getDossierId());

        ProducerRecord<String, byte[]> record =
            new ProducerRecord<>(
                topic,
                key,
                serialize(event));

        producer.send(record)
            .get(
                5,
                TimeUnit.SECONDS);

        log.info(
            "Published event key={} uuid={}",
            key,
            event.getUuid());
      }

      producer.flush();
    }

    log.info(
        "Published {} event(s) to {}",
        events.size(),
        topic);
  }

  private Properties producerProperties() {
    Properties properties =
        new Properties();

    properties.put(
        ProducerConfig.BOOTSTRAP_SERVERS_CONFIG,
        bootstrapServers);

    properties.put(
        ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG,
        StringSerializer.class.getName());

    properties.put(
        ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG,
        ByteArraySerializer.class.getName());

    properties.put(
        ProducerConfig.ACKS_CONFIG,
        "all");

    properties.put(
        ProducerConfig.ENABLE_IDEMPOTENCE_CONFIG,
        "true");

    properties.put(
        ProducerConfig.MAX_IN_FLIGHT_REQUESTS_PER_CONNECTION,
        "1");

    properties.put(
        ProducerConfig.LINGER_MS_CONFIG,
        "100");

    properties.put(
        ProducerConfig.BATCH_SIZE_CONFIG,
        "16384");

    return properties;
  }

  private byte[] serialize(
      DossierTraceinfoEvent event)
      throws IOException {

    try (ByteArrayOutputStream output =
             new ByteArrayOutputStream()) {

      output.write(0x00);
      output.write((schemaId >> 24) & 0xFF);
      output.write((schemaId >> 16) & 0xFF);
      output.write((schemaId >> 8) & 0xFF);
      output.write(schemaId & 0xFF);

      SpecificData specificData =
          SpecificData.getForClass(
              DossierTraceinfoEvent.class);

      DatumWriter<DossierTraceinfoEvent> writer =
          new SpecificDatumWriter<>(
              event.getSchema(),
              specificData);

      BinaryEncoder encoder =
          EncoderFactory.get()
              .binaryEncoder(
                  output,
                  null);

      writer.write(event, encoder);
      encoder.flush();

      return output.toByteArray();
    }
  }
}
