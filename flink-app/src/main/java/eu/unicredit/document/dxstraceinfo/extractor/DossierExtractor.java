package eu.unicredit.document.dxstraceinfo.extractor;

import eu.unicredit.document.dxstraceinfo.avro.DossierTraceinfoEvent;
import java.util.List;
import org.apache.flink.api.common.functions.MapFunction;

public class DossierExtractor
    implements MapFunction<DossierTraceinfoEvent, List<DossierTraceinfoEvent>> {

  @Override
  public List<DossierTraceinfoEvent> map(DossierTraceinfoEvent event) {
    return List.of(event);
  }
}
