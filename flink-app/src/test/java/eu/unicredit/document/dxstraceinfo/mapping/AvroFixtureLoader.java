package eu.unicredit.document.dxstraceinfo.mapping;

import java.nio.charset.StandardCharsets;
import org.apache.avro.Schema;
import org.apache.avro.io.Decoder;
import org.apache.avro.io.DecoderFactory;
import org.apache.avro.specific.SpecificDatumReader;
import org.apache.avro.specific.SpecificRecord;

public class AvroFixtureLoader {

  private AvroFixtureLoader() {
  }

  public static <T extends SpecificRecord> T load(
      String resource,
      Schema schema) throws Exception {
    try (var is =
             AvroFixtureLoader.class
                 .getResourceAsStream(resource)) {

      assert is != null;

      String json = new String(
          is.readAllBytes(),
          StandardCharsets.UTF_8);
      Decoder decoder =
          DecoderFactory.get()
              .jsonDecoder(schema, json);

      SpecificDatumReader<T> reader =
          new SpecificDatumReader<>(schema);

      return reader.read(null, decoder);
    }
  }
}
