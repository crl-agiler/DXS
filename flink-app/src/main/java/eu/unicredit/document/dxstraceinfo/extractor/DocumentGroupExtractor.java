package eu.unicredit.document.dxstraceinfo.extractor;

import eu.unicredit.document.dxstraceinfo.avro.DocumentGroup;
import eu.unicredit.document.dxstraceinfo.avro.DossierTraceinfoEvent;
import java.util.List;
import java.util.stream.Collectors;
import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.api.java.tuple.Tuple2;

public class DocumentGroupExtractor
    implements MapFunction<DossierTraceinfoEvent, List<Tuple2<DossierTraceinfoEvent, DocumentGroup>>> {

  @Override
  public List<Tuple2<DossierTraceinfoEvent, DocumentGroup>> map(DossierTraceinfoEvent event) {

    return event.getDocumentGroups().stream()
        .map(documentGroup -> Tuple2.of(event, documentGroup))
        .collect(Collectors.toList());
  }
}
