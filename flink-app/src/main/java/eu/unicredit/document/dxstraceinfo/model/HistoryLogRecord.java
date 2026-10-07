package eu.unicredit.document.dxstraceinfo.model;

import lombok.AllArgsConstructor;
import lombok.Data;
import lombok.NoArgsConstructor;

import java.io.Serializable;
import java.time.Instant;

@Data
@AllArgsConstructor
@NoArgsConstructor
public class HistoryLogRecord implements Serializable, StatusTracked {
  private String historyLogId;
  private Long levelIdIdentifier;
  private String status;
  private String subStatus;
  private Instant statusTimestamp;
  private String level;

  @Override
  public String trackKey() {
    return level + "|" + levelIdIdentifier;   // level avoids collisions between group and document ids
  }

  @Override
  public String trackStatus() {
    return status;
  }

  @Override
  public Instant trackTimestamp() {
    return statusTimestamp;
  }
}