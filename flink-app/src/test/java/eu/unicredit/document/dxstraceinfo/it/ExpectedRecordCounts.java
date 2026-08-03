package eu.unicredit.document.dxstraceinfo.it;

import lombok.AccessLevel;
import lombok.AllArgsConstructor;
import lombok.Getter;

@Getter
@AllArgsConstructor(access = AccessLevel.PACKAGE)
final class ExpectedRecordCounts {

  private final long dossierCount;
  private final long documentGroupCount;
  private final long documentCount;
  private final long signerCount;
  private final long historyLogCount;
  private final long discardCount;

}
