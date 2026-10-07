package eu.unicredit.document.dxstraceinfo.it;

import eu.unicredit.document.dxstraceinfo.avro.DossierTraceinfoEvent;
import org.junit.jupiter.api.Assertions;

import javax.validation.ConstraintViolation;
import javax.validation.Validation;
import javax.validation.Validator;
import javax.validation.ValidatorFactory;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.Set;
import java.util.stream.Collectors;
import java.util.HashMap;
import java.util.Map;

public final class DossierTestData {

  private final List<DossierTraceinfoEvent> validEvents;
  private final List<DossierTraceinfoEvent> invalidEvents;
  private final ExpectedRecordCounts expectedRecordCounts;

  private DossierTestData(
      List<DossierTraceinfoEvent> validEvents,
      List<DossierTraceinfoEvent> invalidEvents,
      ExpectedRecordCounts expectedRecordCounts) {

    this.validEvents = validEvents;
    this.invalidEvents = invalidEvents;
    this.expectedRecordCounts = expectedRecordCounts;
  }

  static DossierTestData generate(
      int validEventCount,
      int invalidEventCount) {

    List<DossierTraceinfoEvent> validEvents =
        DossierGenerator.getValidDossierTraceInfoEvents(
            validEventCount);

    List<DossierTraceinfoEvent> invalidEvents =
        DossierGenerator.getInvalidDossierTraceInfoEvents(
            invalidEventCount);

    validateGeneratedEvents(
        validEvents,
        invalidEvents);

    ExpectedRecordCounts expectedRecordCounts =
        calculateExpectedRecordCounts(
            validEvents,
            invalidEvents);

    return new DossierTestData(
        validEvents,
        invalidEvents,
        expectedRecordCounts);
  }

  List<DossierTraceinfoEvent> getAllEvents() {
    List<DossierTraceinfoEvent> allEvents =
        new ArrayList<>(
            validEvents.size() +
                invalidEvents.size());

    allEvents.addAll(validEvents);
    allEvents.addAll(invalidEvents);

    return allEvents;
  }

  List<DossierTraceinfoEvent> getInvalidEvents() {
    return Collections.unmodifiableList(
        invalidEvents);
  }

  ExpectedRecordCounts getExpectedRecordCounts() {
    return expectedRecordCounts;
  }

  private static void validateGeneratedEvents(
      List<DossierTraceinfoEvent> validEvents,
      List<DossierTraceinfoEvent> invalidEvents) {

    try (ValidatorFactory factory =
             Validation.buildDefaultValidatorFactory()) {

      Validator validator =
          factory.getValidator();

      for (DossierTraceinfoEvent event : validEvents) {
        Set<ConstraintViolation<DossierTraceinfoEvent>> violations =
            validator.validate(event);

        Assertions.assertTrue(
            violations.isEmpty(),
            () ->
                "Generated valid event contains violations:" +
                    System.lineSeparator() +
                    formatViolations(violations));
      }

      for (DossierTraceinfoEvent event : invalidEvents) {
        Set<ConstraintViolation<DossierTraceinfoEvent>> violations =
            validator.validate(event);

        Assertions.assertFalse(
            violations.isEmpty(),
            "Generated invalid event has no constraint violations");

        Assertions.assertTrue(
            violations.stream()
                .anyMatch(
                    violation ->
                        violation
                            .getPropertyPath()
                            .toString()
                            .endsWith("accessibility")),
            () ->
                "Expected an accessibility violation:" +
                    System.lineSeparator() +
                    formatViolations(violations));
      }
    }
  }

  private static ExpectedRecordCounts calculateExpectedRecordCounts(
      List<DossierTraceinfoEvent> validEvents,
      List<DossierTraceinfoEvent> invalidEvents) {

    long dossierCount =
        validEvents.size();

    long documentGroupCount =
        validEvents.stream()
            .filter(
                event ->
                    event.getDocumentGroups() != null)
            .mapToLong(
                event ->
                    event.getDocumentGroups().size())
            .sum();

    long documentCount =
        validEvents.stream()
            .filter(
                event ->
                    event.getDocumentGroups() != null)
            .flatMap(
                event ->
                    event.getDocumentGroups().stream())
            .filter(
                group ->
                    group.getDocuments() != null)
            .mapToLong(
                group ->
                    group.getDocuments().size())
            .sum();

    long signerCount =
        validEvents.stream()
            .filter(
                event ->
                    event.getDocumentGroups() != null)
            .flatMap(
                event ->
                    event.getDocumentGroups().stream())
            .filter(
                group ->
                    group.getSigners() != null)
            .mapToLong(
                group ->
                    group.getSigners().size())
            .sum();

    /*
     * Adjust this formula if the production history extractor uses
     * a different cardinality.
     */
    long historyLogCount = countHistoryLogRows(validEvents);
    return new ExpectedRecordCounts(
        dossierCount,
        documentGroupCount,
        documentCount,
        signerCount,
        historyLogCount,
        invalidEvents.size());
  }
  /**
   * Mirrors the production filter: a history row is written only when the status of the
   * entity differs from the last one seen for that entity (state is per dossier).
   */
  private static long countHistoryLogRows(List<DossierTraceinfoEvent> events) {
    Map<String, String> lastStatus = new HashMap<>();
    long rows = 0;

    for (DossierTraceinfoEvent event : events) {
      long dossierId = event.getDossierId();

      rows += trackChange(lastStatus, dossierId + "|DOSSIER|" + dossierId, event.getStatus());

      if (event.getDocumentGroups() == null) {
        continue;
      }
      for (var group : event.getDocumentGroups()) {
        rows += trackChange(
                lastStatus, dossierId + "|DOCUMENTS_GROUP|" + group.getId(), group.getStatus());

        if (group.getDocuments() == null) {
          continue;
        }
        for (var document : group.getDocuments()) {
          rows += trackChange(
                  lastStatus, dossierId + "|DOCUMENT|" + document.getId(), document.getStatus());
        }
      }
    }
    return rows;
  }

  private static long trackChange(Map<String, String> lastStatus, String key, Object status) {
    String current = String.valueOf(status);
    String previous = lastStatus.put(key, current);
    return current.equals(previous) ? 0 : 1;
  }
  private static String formatViolations(
      Set<? extends ConstraintViolation<?>> violations) {

    return violations.stream()
        .map(
            violation ->
                String.format(
                    "%s: %s, invalidValue=%s",
                    violation.getPropertyPath(),
                    violation.getMessage(),
                    violation.getInvalidValue()))
        .collect(
            Collectors.joining(
                System.lineSeparator()));
  }
}
