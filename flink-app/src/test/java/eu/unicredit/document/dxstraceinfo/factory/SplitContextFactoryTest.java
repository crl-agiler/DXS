package eu.unicredit.document.dxstraceinfo.factory;

import eu.unicredit.document.dxstraceinfo.mapping.DocumentGroupRecordMapper;
import eu.unicredit.document.dxstraceinfo.mapping.DocumentMapper;
import eu.unicredit.document.dxstraceinfo.mapping.DossierMapper;
import eu.unicredit.document.dxstraceinfo.mapping.HistoryLogMapper;
import eu.unicredit.document.dxstraceinfo.mapping.SignerMapper;
import eu.unicredit.document.dxstraceinfo.transform.SplitContext;
import org.apache.iceberg.flink.CatalogLoader;
import org.junit.jupiter.api.Test;

import java.io.Serializable;
import java.util.List;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertInstanceOf;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.mockito.Mockito.mock;

class SplitContextFactoryTest {

  @Test
  void shouldCreateAllSplitContexts() {

    CatalogLoader catalogLoader =
        mock(CatalogLoader.class);

    SplitContextFactory factory =
        new SplitContextFactory(catalogLoader);

    List<SplitContext<? extends Serializable>> contexts =
        factory.create();

    assertNotNull(contexts);
    assertEquals(5, contexts.size());
  }

  @Test
  void shouldCreateDossierContext() {

    SplitContext<?> context =
        createContexts().get(0);

    assertEquals(
        "dxs-traceinfo-dossier",
        context.getOutputTag().getId());

    assertInstanceOf(
        DossierMapper.class,
        context.getMapper());

    assertEquals(
        List.of("dossier_id"),
        context.getEqualityField());

    assertNotNull(
        context.getTableLoader());
  }

  @Test
  void shouldCreateDocumentGroupContext() {

    SplitContext<?> context =
        createContexts().get(1);

    assertEquals(
        "dxs-traceinfo-documents-group",
        context.getOutputTag().getId());

    assertInstanceOf(
        DocumentGroupRecordMapper.class,
        context.getMapper());

    assertEquals(
        List.of("documents_group_id"),
        context.getEqualityField());

    assertNotNull(
        context.getTableLoader());
  }

  @Test
  void shouldCreateDocumentContext() {

    SplitContext<?> context =
        createContexts().get(2);

    assertEquals(
        "dxs-traceinfo-document",
        context.getOutputTag().getId());

    assertInstanceOf(
        DocumentMapper.class,
        context.getMapper());

    assertEquals(
        List.of("document_id"),
        context.getEqualityField());

    assertNotNull(
        context.getTableLoader());
  }

  @Test
  void shouldCreateSignerContext() {

    SplitContext<?> context =
        createContexts().get(3);

    assertEquals(
        "dxs-traceinfo-signer",
        context.getOutputTag().getId());

    assertInstanceOf(
        SignerMapper.class,
        context.getMapper());

    assertEquals(
        List.of("signer_id"),
        context.getEqualityField());

    assertNotNull(
        context.getTableLoader());
  }

  @Test
  void shouldCreateHistoryLogContext() {

    SplitContext<?> context =
        createContexts().get(4);

    assertEquals(
        "dxs-traceinfo-history-log",
        context.getOutputTag().getId());

    assertInstanceOf(
        HistoryLogMapper.class,
        context.getMapper());

    assertEquals(
            List.of("history_log_id"),
        context.getEqualityField());

    assertNotNull(
        context.getTableLoader());
  }

  private List<SplitContext<? extends Serializable>> createContexts() {

    CatalogLoader catalogLoader =
        mock(CatalogLoader.class);

    return new SplitContextFactory(catalogLoader)
        .create();
  }
}
