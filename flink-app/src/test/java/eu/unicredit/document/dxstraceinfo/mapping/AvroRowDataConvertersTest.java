package eu.unicredit.document.dxstraceinfo.mapping;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.junit.jupiter.api.Assertions.assertThrows;

import java.time.Instant;
import org.apache.flink.table.data.StringData;
import org.apache.flink.table.data.TimestampData;
import org.junit.jupiter.api.Test;

class AvroRowDataConvertersTest {

  @Test
  void stringShouldConvertCharSequenceToStringData() {
    StringData result = AvroRowDataConverters.string("test");

    assertNotNull(result);
    assertEquals("test", result.toString());
  }

  @Test
  void stringShouldReturnNullWhenInputIsNull() {
    assertNull(AvroRowDataConverters.string(null));
  }

  @Test
  void integerShouldConvertCharSequenceToInteger() {
    Integer result = AvroRowDataConverters.integer("123");

    assertNotNull(result);
    assertEquals(123, result);
  }

  @Test
  void integerShouldReturnNullWhenInputIsNull() {
    assertNull(AvroRowDataConverters.integer(null));
  }

  @Test
  void integerShouldThrowExceptionForInvalidNumber() {
    assertThrows(NumberFormatException.class,
        () -> AvroRowDataConverters.integer("abc"));
  }

  @Test
  void longValueShouldConvertCharSequenceToLong() {
    Long result = AvroRowDataConverters.longValue("123456789");

    assertNotNull(result);
    assertEquals(123456789L, result);
  }

  @Test
  void longValueShouldReturnNullWhenInputIsNull() {
    assertNull(AvroRowDataConverters.longValue(null));
  }

  @Test
  void longValueShouldThrowExceptionForInvalidNumber() {
    assertThrows(NumberFormatException.class,
        () -> AvroRowDataConverters.longValue("abc"));
  }

  @Test
  void timestampShouldConvertInstantToTimestampData() {
    Instant instant = Instant.parse("2024-01-01T10:00:00Z");

    TimestampData result = AvroRowDataConverters.timestamp(instant);

    assertNotNull(result);
    assertEquals(instant, result.toInstant());
  }

  @Test
  void timestampShouldReturnNullWhenInputIsNull() {
    assertNull(AvroRowDataConverters.timestamp(null));
  }
}
