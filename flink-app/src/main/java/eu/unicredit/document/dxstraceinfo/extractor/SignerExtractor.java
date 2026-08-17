package eu.unicredit.document.dxstraceinfo.extractor;

import eu.unicredit.document.dxstraceinfo.avro.DocumentGroup;
import eu.unicredit.document.dxstraceinfo.avro.DossierTraceinfoEvent;
import eu.unicredit.document.dxstraceinfo.avro.Signer;
import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.api.java.tuple.Tuple2;

import java.util.List;
import java.util.stream.Collectors;
import java.util.stream.Stream;

public class SignerExtractor
    implements MapFunction<DossierTraceinfoEvent, List<Tuple2<DocumentGroup, Signer>>> {

  @Override
  public List<Tuple2<DocumentGroup, Signer>> map(DossierTraceinfoEvent event) {

    return event.getDocumentGroups().stream()
        .flatMap(this::extractSigners)
        .collect(Collectors.toList());
  }

  private Stream<Tuple2<DocumentGroup, Signer>> extractSigners(DocumentGroup group) {

    return group.getSigners().stream()
        .map(signer -> Tuple2.of(group, signer));
  }
}
