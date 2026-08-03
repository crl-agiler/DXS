package eu.unicredit.document.dxstraceinfo.transform;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.eq;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.never;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.verifyNoInteractions;
import static org.mockito.Mockito.when;

import eu.unicredit.document.dxstraceinfo.avro.DossierTraceinfoEvent;
import java.time.Instant;
import java.util.List;
import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.api.functions.KeyedProcessFunction;
import org.apache.flink.table.data.GenericRowData;
import org.apache.flink.table.data.RowData;
import org.apache.flink.table.data.StringData;
import org.apache.flink.types.RowKind;
import org.apache.flink.util.OutputTag;
import org.junit.jupiter.api.Test;
import org.mockito.ArgumentCaptor;

class SplitTransformLogicTest {

  private static final OutputTag<RowData> DOSSIER_TAG =
      new OutputTag<>("dxs-traceinfo-dossier") {
      };

  private static final OutputTag<RowData> DOCUMENT_TAG =
      new OutputTag<>("dxs-traceinfo-document") {
      };

  private static final OutputTag<RowData> DISCARD_TAG =
      new OutputTag<>("dxs-traceinfo-discard-log") {
      };

  @SuppressWarnings("unchecked")
  @Test
  void processElementShouldProcessAllSplitContexts() throws Exception {
    DossierTraceinfoEvent event = createValidEvent();

    KeyedProcessFunction<String, DossierTraceinfoEvent, RowData>.Context context =
        mock(KeyedProcessFunction.Context.class);

    SplitContext<Long> dossierContext = mock(SplitContext.class);
    SplitContext<String> documentContext = mock(SplitContext.class);

    RowData dossierRow = dossierRow();
    RowData documentRow = documentRow();

    when(dossierContext.getExtractor())
        .thenReturn(input -> List.of(input.getDossierId()));
    when(dossierContext.getMapper()).thenReturn(id -> dossierRow);
    when(dossierContext.getOutputTag()).thenReturn(DOSSIER_TAG);

    when(documentContext.getExtractor()).thenReturn(input -> List.of("DOC01"));
    when(documentContext.getMapper()).thenReturn(document -> documentRow);
    when(documentContext.getOutputTag()).thenReturn(DOCUMENT_TAG);

    SplitTransformLogic logic =
        createLogic(List.of(dossierContext, documentContext));

    try {
      logic.processElement(event, context, null);

      verify(context).output(DOSSIER_TAG, dossierRow);
      verify(context).output(DOCUMENT_TAG, documentRow);
      verify(context, never()).output(eq(DISCARD_TAG), any(RowData.class));
    } finally {
      logic.close();
    }
  }

  @SuppressWarnings("unchecked")
  @Test
  void processElementShouldHandleEmptyExtractedList() throws Exception {
    DossierTraceinfoEvent event = createValidEvent();

    KeyedProcessFunction<String, DossierTraceinfoEvent, RowData>.Context context =
        mock(KeyedProcessFunction.Context.class);

    SplitContext<String> splitContext = mock(SplitContext.class);

    when(splitContext.getExtractor()).thenReturn(input -> List.of());
    when(splitContext.getMapper())
        .thenReturn(value -> GenericRowData.of(StringData.fromString(value)));
    when(splitContext.getOutputTag()).thenReturn(DOCUMENT_TAG);

    SplitTransformLogic logic = createLogic(List.of(splitContext));

    try {
      logic.processElement(event, context, null);

      verifyNoInteractions(context);
    } finally {
      logic.close();
    }
  }

  @SuppressWarnings("unchecked")
  @Test
  void processElementShouldWriteDiscardWhenExtractorFails() throws Exception {
    DossierTraceinfoEvent event = createValidEvent();

    KeyedProcessFunction<String, DossierTraceinfoEvent, RowData>.Context context =
        mock(KeyedProcessFunction.Context.class);

    SplitContext<String> splitContext = mock(SplitContext.class);

    when(splitContext.getExtractor())
        .thenThrow(new RuntimeException("extractor error"));

    SplitTransformLogic logic = createLogic(List.of(splitContext));

    try {
      logic.processElement(event, context, null);

      RowData discard = captureDiscard(context);

      assertDiscardRow(discard);

      String errorJson = discard.getString(1).toString();

      assertTrue(errorJson.contains("\"errorType\":\"PROCESSING_ERROR\""));
      assertTrue(errorJson.contains("extractor error"));

      verify(context, never()).output(eq(DOCUMENT_TAG), any(RowData.class));
    } finally {
      logic.close();
    }
  }

  @SuppressWarnings("unchecked")
  @Test
  void processElementShouldWriteDiscardWhenMapperFails() throws Exception {
    DossierTraceinfoEvent event = createValidEvent();


    KeyedProcessFunction<String, DossierTraceinfoEvent, RowData>.Context context =
        mock(KeyedProcessFunction.Context.class);

    SplitContext<String> splitContext = mock(SplitContext.class);
    MapFunction<String, RowData> mapper = mock(MapFunction.class);

    when(splitContext.getExtractor()).thenReturn(input -> List.of("DOC01"));
    when(splitContext.getMapper()).thenReturn(mapper);
    when(splitContext.getOutputTag()).thenReturn(DOCUMENT_TAG);
    when(mapper.map("DOC01")).thenThrow(new RuntimeException("mapper error"));

    SplitTransformLogic logic = createLogic(List.of(splitContext));

    try {
      logic.processElement(event, context, null);

      RowData discard = captureDiscard(context);

      assertDiscardRow(discard);

      String errorJson = discard.getString(1).toString();

      assertTrue(errorJson.contains("\"errorType\":\"PROCESSING_ERROR\""));
      assertTrue(errorJson.contains("mapper error"));

      verify(context, never()).output(eq(DOCUMENT_TAG), any(RowData.class));
    } finally {
      logic.close();
    }
  }

  @SuppressWarnings("unchecked")
  @Test
  void processElementShouldWriteOneDiscardWhenValidationFails()
      throws Exception {

    DossierTraceinfoEvent event = createInvalidEvent();

    KeyedProcessFunction<String, DossierTraceinfoEvent, RowData>.Context context =
        mock(KeyedProcessFunction.Context.class);

    SplitContext<Long> dossierContext = mock(SplitContext.class);
    SplitContext<String> documentContext = mock(SplitContext.class);

    SplitTransformLogic logic =
        createLogic(List.of(dossierContext, documentContext));

    try {
      logic.processElement(event, context, null);

      RowData discard = captureDiscard(context);

      assertDiscardRow(discard);

      String payload = discard.getString(0).toString();
      String errorJson = discard.getString(1).toString();

      assertTrue(payload.contains("INVALID"));
      assertTrue(errorJson.contains("\"errorType\":\"VALIDATION_ERROR\""));
      assertTrue(errorJson.contains("\"field\":\"selectedProcessType\""));
      assertTrue(errorJson.contains("\"invalid_value\":\"INVALID\""));
      assertTrue(errorJson.contains("\"constraint\":\"Pattern\""));

      verify(dossierContext, never()).getExtractor();
      verify(documentContext, never()).getExtractor();

      verify(context, never()).output(eq(DOSSIER_TAG), any(RowData.class));
      verify(context, never()).output(eq(DOCUMENT_TAG), any(RowData.class));
    } finally {
      logic.close();
    }
  }

  @Test
  void constructorShouldRejectNullSplitContexts() {
    assertThrows(
        NullPointerException.class,
        () -> new SplitTransformLogic(null, DISCARD_TAG));
  }

  @Test
  void constructorShouldRejectNullDiscardTag() {
    assertThrows(
        NullPointerException.class,
        () -> new SplitTransformLogic(List.of(), null));
  }

  private SplitTransformLogic createLogic(
      List<SplitContext<?>> splitContexts) throws Exception {

    SplitTransformLogic logic =
        new SplitTransformLogic(splitContexts, DISCARD_TAG);

    logic.open(new Configuration());

    return logic;
  }

  private RowData captureDiscard(
      KeyedProcessFunction<String, DossierTraceinfoEvent, RowData>.Context context) {

    ArgumentCaptor<RowData> captor =
        ArgumentCaptor.forClass(RowData.class);

    verify(context).output(eq(DISCARD_TAG), captor.capture());

    return captor.getValue();
  }

  private void assertDiscardRow(RowData discard) {
    assertNotNull(discard);
    assertEquals(RowKind.INSERT, discard.getRowKind());
    assertEquals(3, discard.getArity());
    assertNotNull(discard.getString(0));
    assertNotNull(discard.getString(1));
    assertNotNull(discard.getTimestamp(2, 9));
  }

  private DossierTraceinfoEvent createValidEvent() {
    Instant start = Instant.parse("2021-08-21T10:26:00Z");
    Instant end = Instant.parse("2021-08-26T11:26:00Z");

    return DossierTraceinfoEvent.newBuilder()
        .setUuid("uuid")
        .setEventType("CREATED")
        .setEventTimestamp(Instant.parse("2021-08-21T10:25:00Z"))
        .setDossierId(123L)
        .setApplicationCode("DXS")
        .setSelectedProcessType("DIGITAL")
        .setReprint("N")
        .setExeApp("APP")
        .setCorrelationKey("CORR-123")
        .setAuthor("AUTHOR")
        .setStatus("CREATED")
        .setSubStatus(null)
        .setBranch("00100")
        .setPaperReason(null)
        .setPracticeId(null)
        .setPecFlow(null)
        .setBankCode("02008")
        .setRelatedDossier(null)
        .setMasterDossierId(null)
        .setMotivation(null)
        .setDossierType("ENTERPRISE")
        .setReferenceBranchPec(null)
        .setLegalEntity(null)
        .setFlowType(null)
        .setCreationDate(start)
        .setEndDate(end)
        .setCustomers(List.of())
        .setDocumentGroups(List.of())
        .build();
  }

  private DossierTraceinfoEvent createInvalidEvent() {
    DossierTraceinfoEvent event = createValidEvent();
    event.setSelectedProcessType("INVALID");
    return event;
  }

  private RowData dossierRow() {
    return GenericRowData.of(
        123L,
        StringData.fromString("DIGITAL"),
        StringData.fromString("CREATED"));
  }

  private RowData documentRow() {
    return GenericRowData.of(
        999L,
        StringData.fromString("DOC01"),
        StringData.fromString("ARCHIVED"));
  }
}
