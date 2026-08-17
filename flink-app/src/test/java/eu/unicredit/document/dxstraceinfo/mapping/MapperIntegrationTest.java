package eu.unicredit.document.dxstraceinfo.mapping;


import eu.unicredit.document.dxstraceinfo.avro.Document;
import eu.unicredit.document.dxstraceinfo.avro.DocumentGroup;
import eu.unicredit.document.dxstraceinfo.avro.DossierTraceinfoEvent;
import eu.unicredit.document.dxstraceinfo.avro.Signer;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.table.data.GenericRowData;
import org.apache.flink.table.data.StringData;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.Arguments;
import org.junit.jupiter.params.provider.MethodSource;

import java.util.stream.Stream;

import static org.junit.jupiter.api.Assertions.assertEquals;

class MapperIntegrationTest {

  private final DossierMapper dossierMapper =
      new DossierMapper();

  private final DocumentGroupRecordMapper documentGroupMapper =
      new DocumentGroupRecordMapper();

  private final DocumentMapper documentMapper =
      new DocumentMapper();

  private final SignerMapper signerMapper =
      new SignerMapper();

  static Stream<Arguments> scenarios() {

    return Stream.of(
        Arguments.of(
            "/fixtures/dossier-ok.json",
            "OK",
            "OK",
            "OK"),
        Arguments.of(
            "/fixtures/dossier-ko.json",
            "KO",
            "KO",
            "KO"),
        Arguments.of(
            "/fixtures/dossier-in-progress.json",
            "IN_PROGRESS",
            "IN_PROGRESS",
            "IN_PROGRESS")
    );
  }

  @ParameterizedTest
  @MethodSource("scenarios")
  void shouldMapWholeHierarchy(
      String fixture,
      String dossierOutcome,
      String groupOutcome,
      String documentOutcome) throws Exception {

    DossierTraceinfoEvent event =
        AvroFixtureLoader.load(fixture, DossierTraceinfoEvent.getClassSchema());

    // DOSSIER

    GenericRowData dossierRow =
        (GenericRowData)
            dossierMapper.map(event);

    assertEquals(
        StringData.fromString(dossierOutcome),
        dossierRow.getField(9));

    // GROUPS

    for (DocumentGroup group : event.getDocumentGroups()) {

      GenericRowData groupRow =
          (GenericRowData)
              documentGroupMapper.map(
                  Tuple2.of(event, group));

      assertEquals(
          StringData.fromString(groupOutcome),
          groupRow.getField(2));

      // DOCUMENTS

      for (Document document : group.getDocuments()) {

        GenericRowData documentRow =
            (GenericRowData)
                documentMapper.map(
                    Tuple2.of(group, document));

        assertEquals(
            StringData.fromString(documentOutcome),
            documentRow.getField(9));
      }

      // SIGNERS

      for (Signer signer : group.getSigners()) {

        GenericRowData signerRow =
            (GenericRowData)
                signerMapper.map(
                    Tuple2.of(group, signer));

        assertEquals(
            signer.getId(),
            signerRow.getField(0));
      }
    }
  }
}
