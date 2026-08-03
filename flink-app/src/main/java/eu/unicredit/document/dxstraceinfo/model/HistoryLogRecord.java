package eu.unicredit.document.dxstraceinfo.model;

import java.time.Instant;
import lombok.AllArgsConstructor;
import lombok.Data;
import lombok.NoArgsConstructor;

@Data
@AllArgsConstructor
@NoArgsConstructor
public class HistoryLogRecord {
  private Long historyLogId;
  private Long levelIdIdentifier;
  private String status;
  private String subStatus;
  private Instant statusTimestamp;
  private String level;
}
