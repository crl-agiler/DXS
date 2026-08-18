package eu.unicredit.document.dxstraceinfo.kafka;

import eu.unicredit.document.dxstraceinfo.avro.DossierTraceinfoEvent;
import org.apache.flink.api.common.serialization.DeserializationSchema;
import org.apache.flink.util.Collector;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.junit.jupiter.api.Test;

import java.io.IOException;

import static org.junit.jupiter.api.Assertions.assertDoesNotThrow;
import static org.mockito.Mockito.any;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.verifyNoInteractions;
import static org.mockito.Mockito.when;

class SafeKafkaDeserializerTest {

    @Test
    void shouldCollectEventWhenDeserializationSucceeds() throws IOException {

        DeserializationSchema<DossierTraceinfoEvent> delegate =
                mock(DeserializationSchema.class);

        Collector<DossierTraceinfoEvent> collector =
                mock(Collector.class);

        DossierTraceinfoEvent event =
                mock(DossierTraceinfoEvent.class);

        when(delegate.deserialize(any(byte[].class)))
                .thenReturn(event);

        SafeKafkaDeserializer deserializer =
                SafeKafkaDeserializer.of(delegate);

        ConsumerRecord<byte[], byte[]> record =
                new ConsumerRecord<>(
                        "topic",
                        0,
                        1L,
                        null,
                        new byte[]{1, 2, 3});

        deserializer.deserialize(record, collector);

        verify(delegate).deserialize(record.value());
        verify(collector).collect(event);
    }

    @Test
    void shouldNotCollectWhenDelegateReturnsNull() throws IOException {

        DeserializationSchema<DossierTraceinfoEvent> delegate =
                mock(DeserializationSchema.class);

        Collector<DossierTraceinfoEvent> collector =
                mock(Collector.class);

        when(delegate.deserialize(any(byte[].class)))
                .thenReturn(null);

        SafeKafkaDeserializer deserializer =
                SafeKafkaDeserializer.of(delegate);

        ConsumerRecord<byte[], byte[]> record =
                new ConsumerRecord<>(
                        "topic",
                        0,
                        1L,
                        null,
                        new byte[]{1, 2, 3});

        deserializer.deserialize(record, collector);

        verify(delegate).deserialize(record.value());
        verifyNoInteractions(collector);
    }

    @Test
    void shouldSwallowExceptionWhenDeserializationFails() throws IOException {

        DeserializationSchema<DossierTraceinfoEvent> delegate =
                mock(DeserializationSchema.class);

        Collector<DossierTraceinfoEvent> collector =
                mock(Collector.class);

        when(delegate.deserialize(any(byte[].class)))
                .thenThrow(new RuntimeException("boom"));

        SafeKafkaDeserializer deserializer =
                SafeKafkaDeserializer.of(delegate);

        ConsumerRecord<byte[], byte[]> record =
                new ConsumerRecord<>(
                        "topic",
                        1,
                        99L,
                        null,
                        new byte[]{1, 2, 3});

        assertDoesNotThrow(() ->
                deserializer.deserialize(record, collector));

        verify(delegate).deserialize(record.value());
        verifyNoInteractions(collector);
    }
}