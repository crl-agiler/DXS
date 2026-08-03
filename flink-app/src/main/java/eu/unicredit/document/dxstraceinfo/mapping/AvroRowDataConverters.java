package eu.unicredit.document.dxstraceinfo.mapping;

import java.time.Instant;
import org.apache.flink.table.data.StringData;
import org.apache.flink.table.data.TimestampData;

public final class AvroRowDataConverters {

  private AvroRowDataConverters() {
  }

  public static StringData string(CharSequence value) {
    return value == null ?
        null
        : StringData.fromString(value.toString());
  }

  public static Integer integer(CharSequence value) {
    return value == null ?
        null
        : Integer.parseInt(value.toString());
  }

  public static Long longValue(CharSequence value) {
    return value == null ?
        null
        : Long.parseLong(value.toString());
  }

  public static TimestampData timestamp(Instant instant) {
    return instant == null ?
        null
        : TimestampData.fromInstant(instant);
  }
}
