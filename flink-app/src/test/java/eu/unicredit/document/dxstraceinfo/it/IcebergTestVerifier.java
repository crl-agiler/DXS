package eu.unicredit.document.dxstraceinfo.it;


import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import eu.unicredit.document.dxstraceinfo.avro.DossierTraceinfoEvent;
import lombok.extern.slf4j.Slf4j;
import org.apache.flink.api.common.JobStatus;
import org.apache.flink.core.execution.JobClient;
import org.apache.iceberg.Table;
import org.apache.iceberg.catalog.Catalog;
import org.apache.iceberg.catalog.TableIdentifier;
import org.apache.iceberg.data.IcebergGenerics;
import org.apache.iceberg.data.Record;
import org.apache.iceberg.io.CloseableIterable;
import org.awaitility.Awaitility;

import java.io.IOException;
import java.time.Duration;
import java.util.ArrayList;
import java.util.List;
import java.util.Set;
import java.util.concurrent.TimeUnit;
import java.util.stream.Collectors;

import static eu.unicredit.document.dxstraceinfo.it.FlinkDSXTraceInfoIT.DISCARD_LOG_TABLE;
import static eu.unicredit.document.dxstraceinfo.it.FlinkDSXTraceInfoIT.DOCUMENTS_GROUP_TABLE;
import static eu.unicredit.document.dxstraceinfo.it.FlinkDSXTraceInfoIT.DOCUMENT_TABLE;
import static eu.unicredit.document.dxstraceinfo.it.FlinkDSXTraceInfoIT.DOSSIER_TABLE;
import static eu.unicredit.document.dxstraceinfo.it.FlinkDSXTraceInfoIT.HISTORY_LOG_TABLE;
import static eu.unicredit.document.dxstraceinfo.it.FlinkDSXTraceInfoIT.SIGNER_TABLE;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertTrue;

@Slf4j
public final class IcebergTestVerifier {

  private final Catalog catalog;
  private final Duration waitTimeout;
  private final Duration pollInterval;
  private final Duration jobOperationTimeout;
  private final ObjectMapper objectMapper;

  IcebergTestVerifier(
      Catalog catalog,
      Duration waitTimeout,
      Duration pollInterval,
      Duration jobOperationTimeout) {

    this.catalog = catalog;
    this.waitTimeout = waitTimeout;
    this.pollInterval = pollInterval;
    this.jobOperationTimeout = jobOperationTimeout;
    this.objectMapper = new ObjectMapper();
  }

  void awaitExpectedRecords(
      JobClient jobClient,
      ExpectedRecordCounts expected) {

    awaitTable(
        jobClient,
        DOSSIER_TABLE,
        expected.getDossierCount());

    awaitTable(
        jobClient,
        DOCUMENTS_GROUP_TABLE,
        expected.getDocumentGroupCount());

    awaitTable(
        jobClient,
        DOCUMENT_TABLE,
        expected.getDocumentCount());

    awaitTable(
        jobClient,
        SIGNER_TABLE,
        expected.getSignerCount());

    awaitTable(
        jobClient,
        HISTORY_LOG_TABLE,
        expected.getHistoryLogCount());

    awaitTable(
        jobClient,
        DISCARD_LOG_TABLE,
        expected.getDiscardCount());
  }

  void assertBusinessTables(
      ExpectedRecordCounts expected)
      throws IOException {

    assertRecordCount(
        DOSSIER_TABLE,
        expected.getDossierCount());

    assertRecordCount(
        DOCUMENTS_GROUP_TABLE,
        expected.getDocumentGroupCount());

    assertRecordCount(
        DOCUMENT_TABLE,
        expected.getDocumentCount());

    assertRecordCount(
        SIGNER_TABLE,
        expected.getSignerCount());

    assertRecordCount(
        HISTORY_LOG_TABLE,
        expected.getHistoryLogCount());
  }

  void assertDiscardTable(
      List<DossierTraceinfoEvent> invalidEvents)
      throws IOException {

    List<Record> records =
        readRecords(DISCARD_LOG_TABLE);

    assertEquals(
        invalidEvents.size(),
        records.size(),
        "Expected exactly one discard for each invalid event");

    Set<String> expectedUuids =
        invalidEvents.stream()
            .map(DossierTraceinfoEvent::getUuid)
            .map(Object::toString)
            .collect(Collectors.toSet());

    Set<String> actualUuids =
        records.stream()
            .map(this::assertAndExtractDiscardUuid)
            .collect(Collectors.toSet());

    assertEquals(
        expectedUuids,
        actualUuids,
        "Discard records do not match the invalid input events");
  }

  void dumpTables(
      Set<TableIdentifier> identifiers)
      throws IOException {

    for (TableIdentifier identifier : identifiers) {
      List<Record> records =
          readRecords(identifier);

      for (Record record : records) {
        log.info(
            "Table {} - Row {}",
            identifier.name(),
            record);
      }

      log.info(
          "Table {} - Total records {}",
          identifier.name(),
          records.size());
    }
  }

  private void awaitTable(
      JobClient jobClient,
      TableIdentifier identifier,
      long minimumExpectedRecords) {

    Awaitility.await()
        .alias(
            "Waiting for Iceberg table " +
                identifier.name())
        .atMost(waitTimeout)
        .pollDelay(Duration.ofSeconds(1))
        .pollInterval(pollInterval)
        .untilAsserted(
            () -> {
              JobStatus status =
                  jobClient.getJobStatus()
                      .get(
                          jobOperationTimeout.toSeconds(),
                          TimeUnit.SECONDS);

              assertJobRunning(
                  status,
                  identifier);

              long actualCount =
                  countRecords(identifier);

              log.info(
                  "Table={}, actual={}, expectedMinimum={}, jobStatus={}",
                  identifier,
                  actualCount,
                  minimumExpectedRecords,
                  status);

              assertTrue(
                  actualCount >= minimumExpectedRecords,
                  () ->
                      String.format(
                          "Table %s contains %d records, expected at least %d",
                          identifier,
                          actualCount,
                          minimumExpectedRecords));
            });
  }

  private void assertRecordCount(
      TableIdentifier identifier,
      long expectedCount)
      throws IOException {

    long actualCount =
        countRecords(identifier);

    assertEquals(
        expectedCount,
        actualCount,
        () ->
            String.format(
                "Table %s contains %d records, expected %d",
                identifier,
                actualCount,
                expectedCount));
  }

  private String assertAndExtractDiscardUuid(
      Record record) {

    Object payloadValue =
        record.getField("payload");

    Object errorValue =
        record.getField("error");

    Object processingTime =
        record.getField("processing_time");

    assertNotNull(
        payloadValue,
        "Discard payload must not be null");

    assertNotNull(
        errorValue,
        "Discard error must not be null");

    assertNotNull(
        processingTime,
        "Discard processing_time must not be null");

    String errorJson =
        errorValue.toString();

    try {
      JsonNode error =
          objectMapper.readTree(errorJson);

      assertEquals(
          "VALIDATION_ERROR",
          error.path("errorType").asText(),
          () ->
              "Unexpected discard error: " +
                  errorJson);

      JsonNode violations =
          error.path("violations");

      assertTrue(
          violations.isArray(),
          () ->
              "violations is not an array: " +
                  errorJson);

      boolean accessibilityViolationFound =
          false;

      for (JsonNode violation : violations) {
        if (violation.path("field")
            .asText()
            .endsWith("accessibility") &&
            "INVALID".equals(
                violation.path("invalid_value")
                    .asText()) &&
            "Pattern".equals(
                violation.path("constraint")
                    .asText())) {

          accessibilityViolationFound = true;
          break;
        }
      }

      assertTrue(
          accessibilityViolationFound,
          () ->
              "Expected accessibility violation not found: " +
                  errorJson);

      String payload =
          payloadValue.toString();

      return extractUuidFromPayload(payload);

    } catch (IOException exception) {
      throw new AssertionError(
          "Invalid discard error JSON: " +
              errorJson,
          exception);
    }
  }

  private String extractUuidFromPayload(
      String payload) {

    int uuidIndex =
        payload.indexOf("uuid");

    assertTrue(
        uuidIndex >= 0,
        () ->
            "UUID not found in discard payload: " +
                payload);

    /*
     * The Avro SpecificRecord toString() representation is JSON-like.
     * Parse it when possible.
     */
    try {
      JsonNode payloadJson =
          objectMapper.readTree(payload);

      JsonNode uuid =
          payloadJson.path("uuid");

      assertFalse(uuid.isMissingNode(), () ->
          "UUID missing from discard payload: " +
              payload);

      return uuid.asText();

    } catch (IOException exception) {
      throw new AssertionError(
          "Invalid discard payload JSON: " +
              payload,
          exception);
    }
  }

  private void assertJobRunning(
      JobStatus status,
      TableIdentifier identifier) {

    if (status == JobStatus.FAILED ||
        status == JobStatus.CANCELED ||
        status == JobStatus.FINISHED ||
        status == JobStatus.SUSPENDED) {

      throw new AssertionError(
          String.format(
              "Flink job reached state %s while waiting for table %s",
              status,
              identifier));
    }
  }

  private long countRecords(
      TableIdentifier identifier)
      throws IOException {

    return readRecords(identifier).size();
  }

  private List<Record> readRecords(
      TableIdentifier identifier)
      throws IOException {

    Table table =
        catalog.loadTable(identifier);

    table.refresh();

    if (table.currentSnapshot() == null) {
      return List.of();
    }

    List<Record> result =
        new ArrayList<>();

    try (CloseableIterable<Record> records =
             IcebergGenerics.read(table)
                 .useSnapshot(
                     table.currentSnapshot()
                         .snapshotId())
                 .build()) {

      for (Record record : records) {
        result.add(record);
      }
    }

    return result;
  }
}
