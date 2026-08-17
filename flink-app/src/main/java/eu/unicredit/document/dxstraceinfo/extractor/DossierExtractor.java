package eu.unicredit.document.dxstraceinfo.extractor;

import eu.unicredit.document.dxstraceinfo.avro.DossierTraceinfoEvent;
import org.apache.flink.api.common.functions.MapFunction;

import java.util.List;

public class DossierExtractor
    implements MapFunction<DossierTraceinfoEvent, List<DossierTraceinfoEvent>> {

  @Override
  public List<DossierTraceinfoEvent> map(DossierTraceinfoEvent event) {
    return List.of(event);
  }
}
