package eu.unicredit.document.dxstraceinfo.extractor;

import eu.unicredit.document.dxstraceinfo.avro.Document;
import eu.unicredit.document.dxstraceinfo.avro.DocumentGroup;
import eu.unicredit.document.dxstraceinfo.avro.DossierTraceinfoEvent;
import java.util.List;
import java.util.stream.Collectors;
import java.util.stream.Stream;
import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.api.java.tuple.Tuple2;

public class DocumentExtractor implements MapFunction<DossierTraceinfoEvent, List<Tuple2<DocumentGroup, Document>>> {

  @Override
  public List<Tuple2<DocumentGroup, Document>> map(DossierTraceinfoEvent event) {

    return event.getDocumentGroups().stream()
        .flatMap(this::extractDocuments)
        .collect(Collectors.toList());
  }

  private Stream<Tuple2<DocumentGroup, Document>> extractDocuments(DocumentGroup group) {
    return group.getDocuments().stream()
        .map(document -> Tuple2.of(group, document));
  }

}
