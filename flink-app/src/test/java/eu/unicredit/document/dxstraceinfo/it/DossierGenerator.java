package eu.unicredit.document.dxstraceinfo.it;

import static org.instancio.Select.field;

import eu.unicredit.document.dxstraceinfo.avro.Customer;
import eu.unicredit.document.dxstraceinfo.avro.Document;
import eu.unicredit.document.dxstraceinfo.avro.DocumentGroup;
import eu.unicredit.document.dxstraceinfo.avro.DossierTraceinfoEvent;
import eu.unicredit.document.dxstraceinfo.avro.Signer;
import java.time.Instant;
import java.time.temporal.ChronoUnit;
import java.util.List;
import java.util.UUID;
import org.instancio.Instancio;

public final class DossierGenerator {

  private DossierGenerator() {
  }

  public static List<DossierTraceinfoEvent> getValidDossierTraceInfoEvents(
      int eventCount) {

    return Instancio.ofList(DossierTraceinfoEvent.class)
        .size(eventCount)

        /*
         * Collections
         */
        .generate(
            field(DossierTraceinfoEvent.class, "customers"),
            generator -> generator.collection()
                .minSize(1)
                .maxSize(2))

        .generate(
            field(DossierTraceinfoEvent.class, "documentGroups"),
            generator -> generator.collection()
                .minSize(1)
                .maxSize(3))

        .generate(
            field(DocumentGroup.class, "signers"),
            generator -> generator.collection()
                .minSize(1)
                .maxSize(3))

        .generate(
            field(DocumentGroup.class, "documents"),
            generator -> generator.collection()
                .minSize(1)
                .maxSize(3))

        /*
         * DossierTraceinfoEvent
         */
        .supply(
            field(DossierTraceinfoEvent.class, "uuid"),
            random -> UUID.randomUUID().toString())

        .generate(
            field(DossierTraceinfoEvent.class, "eventType"),
            generator -> generator.oneOf(
                "INSERT",
                "UPDATE"))

        .supply(
            field(DossierTraceinfoEvent.class, "eventTimestamp"),
            random -> randomInstant())

        .generate(
            field(DossierTraceinfoEvent.class, "dossierId"),
            generator -> generator.longs()
                .range(
                    1_000_000L,
                    999_999_999L))

        .generate(
            field(DossierTraceinfoEvent.class, "applicationCode"),
            generator -> generator.oneOf(
                "IKP",
                "OFB",
                "AFA",
                "DXS"))

        .generate(
            field(DossierTraceinfoEvent.class, "selectedProcessType"),
            generator -> generator.oneOf(
                "PAPER",
                "PEC",
                "BRANCH_INDIVIDUALS",
                "DIGITAL"))

        .generate(
            field(DossierTraceinfoEvent.class, "reprint"),
            generator -> generator.oneOf(
                "Y",
                "N"))

        .generate(
            field(DossierTraceinfoEvent.class, "exeApp"),
            generator -> generator.oneOf(
                "DXS",
                "XPI",
                "DMB"))

        .supply(
            field(DossierTraceinfoEvent.class, "correlationKey"),
            random ->
                "CORR-" +
                    random.longRange(
                        10_000_000L,
                        99_999_999L))

        .supply(
            field(DossierTraceinfoEvent.class, "author"),
            random ->
                "USER_" +
                    random.longRange(
                        100_000L,
                        999_999L))

        .generate(
            field(DossierTraceinfoEvent.class, "status"),
            generator -> generator.oneOf(
                "CREATED",
                "DRAFT",
                "DRAFT_PEC",
                "PROCESSING",
                "RETRY_ON_PROCESS",
                "PEC_PROCESSING",
                "TO_BE_ACCEPTED",
                "TO_BE_CLOSED",
                "EXPIRED",
                "CANCELLED",
                "CONCLUDED",
                "CLOSED"))

        .generate(
            field(DossierTraceinfoEvent.class, "branch"),
            generator -> generator.string()
                .digits()
                .length(5))

        .generate(
            field(DossierTraceinfoEvent.class, "paperReason"),
            generator -> generator.oneOf(
                "FORCED_BY_VERTICAL",
                "SELECTED_BY_USER"))

        .generate(
            field(DossierTraceinfoEvent.class, "pecFlow"),
            generator -> generator.oneOf(
                "B2C",
                "C2B"))

        .generate(
            field(DossierTraceinfoEvent.class, "bankCode"),
            generator -> generator.string()
                .digits()
                .length(5))

        .supply(
            field(DossierTraceinfoEvent.class, "masterDossierId"),
            random -> Long.toString(
                random.longRange(
                    1_000_000_000L,
                    9_999_999_999L)))

        .generate(
            field(DossierTraceinfoEvent.class, "flowType"),
            generator -> generator.oneOf(
                "PF",
                "PG"))

        .generate(
            field(DossierTraceinfoEvent.class, "dossierType"),
            generator -> generator.oneOf(
                "ENTERPRISE",
                "UCX"))

        .generate(
            field(DossierTraceinfoEvent.class, "referenceBranchPec"),
            generator -> generator.string()
                .digits()
                .length(5))

        .supply(
            field(DossierTraceinfoEvent.class, "creationDate"),
            random -> randomInstant())

        .supply(
            field(DossierTraceinfoEvent.class, "endDate"),
            random -> randomInstant().plus(1, ChronoUnit.HOURS))

        /*
         * Customer
         */
        .generate(
            field(Customer.class, "ndg"),
            generator -> generator.string()
                .digits()
                .length(8))

        .generate(
            field(Customer.class, "type"),
            generator -> generator.oneOf(
                "PG",
                "PF",
                "CO"))

        .generate(
            field(Customer.class, "selectedChannels"),
            generator -> generator.oneOf(
                "8",
                "778",
                "57"))

        /*
         * DocumentGroup
         */
        .generate(
            field(DocumentGroup.class, "id"),
            generator -> generator.longs()
                .range(
                    10_000_000L,
                    999_999_999L))

        .generate(
            field(DocumentGroup.class, "status"),
            generator -> generator.oneOf(
                "CREATED",
                "DISABLED",
                "LOCKED",
                "READY",
                "PROCESSING_PARTIALLY_SIGNED",
                "TO_BE_ARCHIVED",
                "TO_BE_CONFIRMED",
                "PROCESSING",
                "UPLOADED",
                "TO_BE_PRINTED",
                "PRINTED",
                "CANCELLED",
                "CANCELLED_ARCHIVED",
                "CANCELLED_CREATED",
                "CANCELLED_DRAFT",
                "CANCELLED_LOCKED",
                "CANCELLED_SENT",
                "ARCHIVED",
                "AUTOMATIC_ARCHIVED",
                "CLOSED",
                "EXPIRED",
                "ERROR_SIGNAL",
                "TO_BE_UPLOADED",
                "TO_BE_ACKNOWLEDGED",
                "ARCHIVED_CANCELED",
                "SIGNED",
                "HIDDEN"))

        .supply(
            field(DocumentGroup.class, "digilayerId"),
            random ->
                "DIGI_" +
                    random.longRange(
                        10_000L,
                        99_999L))

        .supply(
            field(DocumentGroup.class, "envelopeId"),
            random ->
                "ENV_" +
                    random.longRange(
                        10_000L,
                        99_999L))

        .supply(
            field(DocumentGroup.class, "creationDate"),
            random -> randomInstant())

        .supply(
            field(DocumentGroup.class, "endDate"),
            random -> randomInstant().plus(1, ChronoUnit.HOURS))

        /*
         * Signer
         */
        .generate(
            field(Signer.class, "id"),
            generator -> generator.longs()
                .range(
                    10_000L,
                    999_999_999L))

        .generate(
            field(Signer.class, "ndg"),
            generator -> generator.string()
                .digits()
                .length(8))

        .generate(
            field(Signer.class, "mfaMethod"),
            generator -> generator.oneOf(
                "SMS",
                "PRI",
                "FIRMAMIA",
                "PMP"))

        .supply(
            field(Signer.class, "authenticationId"),
            random ->
                "AUTH_" +
                    random.longRange(
                        10_000_000L,
                        99_999_999L))

        .supply(
            field(Signer.class, "signerReb"),
            random -> Long.toString(
                random.longRange(
                    1_000_000L,
                    9_999_999L)))

        .generate(
            field(Signer.class, "signatureChannel"),
            generator -> generator.oneOf(
                "8",
                "778",
                "57"))

        .supply(
            field(Signer.class, "signatureDate"),
            random -> randomInstant())

        /*
         * Document
         */
        .generate(
            field(Document.class, "id"),
            generator -> generator.longs()
                .range(
                    100_000L,
                    999_999_999L))

        .supply(
            field(Document.class, "code"),
            random ->
                "BR" +
                    random.longRange(
                        10_000L,
                        99_999L))

        .generate(
            field(Document.class, "status"),
            generator -> generator.oneOf(
                "CREATED",
                "DOC_GENERATED",
                "PRINTED",
                "ARCHIVED",
                "ARCHIVED_SKIPPED",
                "IN_ARCHIVING",
                "TO_BE_ARCHIVED",
                "BO_APPROVAL",
                "RM_APPROVAL",
                "CONTROLS_FAILED",
                "CANCELLED",
                "CONFIRMED",
                "REJECTED",
                "UPLOADED"))

        .generate(
            field(Document.class, "categoryCode"),
            generator -> generator.oneOf(
                "AF",
                "AC",
                "LE",
                "TT"))

        .generate(
            field(Document.class, "productName"),
            generator -> generator.oneOf(
                "CARDS",
                "LUW_UCL230_48_SCADOC_FULL",
                "ACQUIRING_BRANCH"))

        .generate(
            field(Document.class, "processCode"),
            generator -> generator.oneOf(
                "SALE",
                "BANK_INSURANCE",
                "ACQUIRING",
                "DEPOSITO_TITOLI"))

        .generate(
            field(Document.class, "processType"),
            generator -> generator.oneOf(
                "UPLOAD",
                "SIGN",
                "PAPER",
                "PEC",
                "BRANCH_INDIVIDUALS"))

        .generate(
            field(Document.class, "size"),
            generator -> generator.longs()
                .range(
                    1L,
                    10_000_000L))

        .set(
            field(Document.class, "controlsOutcomePec"),
            "received|2026-07-30T15:00:00.123456789Z" +
                "|TO_BE_VERIFIED|BU2303-signed.pdf")

        .generate(
            field(Document.class, "signatureTypePec"),
            generator -> generator.oneOf(
                "AES",
                "QES",
                "SES"))

        .generate(
            field(Document.class, "certificateAuthorityPec"),
            generator -> generator.oneOf(
                "INTESA",
                "INFOCERT"))

        .generate(
            field(Document.class, "signaturePointTimestampPec"),
            generator -> generator.oneOf(
                "2026-07-30T15:00:00Z",
                "2026-07-30T15:00:00.123Z",
                "2026-07-30T15:00:00.123456789Z",
                "2026-07-30T17:00:00+02:00"))

        .generate(
            field(Document.class, "accessibility"),
            generator -> generator.oneOf(
                "Y",
                "N"))

        .supply(
            field(Document.class, "archiveLink"),
            random ->
                "./" +
                    random.longRange(
                        10_000_000L,
                        99_999_999L) +
                    "/" +
                    random.longRange(1L, 9L) +
                    "/" +
                    random.longRange(1L, 9L))

        .generate(
            field(Document.class, "numberOfPages"),
            generator -> generator.longs()
                .range(
                    1L,
                    1_000L))

        .generate(
            field(Document.class, "accountNumber"),
            generator -> generator.string()
                .digits()
                .length(12))

        .generate(
            field(Document.class, "signatureType"),
            generator -> generator.oneOf(
                "AES",
                "QES",
                "SES"))

        .generate(
            field(Document.class, "digitalization"),
            generator -> generator.oneOf(
                "Y",
                "N"))

        .supply(
            field(Document.class, "creationDate"),
            random -> randomInstant())

        .supply(
            field(Document.class, "endDate"),
            random -> randomInstant().plus(1, ChronoUnit.HOURS))

        .supply(
            field(Document.class, "signatureDate"),
            random -> randomInstant())

        .create();
  }

  public static List<DossierTraceinfoEvent> getInvalidDossierTraceInfoEvents(
      int eventCount) {

    List<DossierTraceinfoEvent> invalidEvents =
        getValidDossierTraceInfoEvents(eventCount);

    for (DossierTraceinfoEvent event : invalidEvents) {
      invalidateAccessibility(event);
    }

    return invalidEvents;
  }

  private static void invalidateAccessibility(
      DossierTraceinfoEvent event) {

    if (event.getDocumentGroups() == null ||
        event.getDocumentGroups().isEmpty()) {

      throw new IllegalStateException(
          "Cannot invalidate an event without document groups");
    }

    DocumentGroup documentGroup =
        event.getDocumentGroups().get(0);

    if (documentGroup.getDocuments() == null ||
        documentGroup.getDocuments().isEmpty()) {

      throw new IllegalStateException(
          "Cannot invalidate an event without documents");
    }

    Document document =
        documentGroup.getDocuments().get(0);

    /*
     * Avro-valid value because accessibility is a string,
     * but invalid according to:
     *
     * @Pattern(regexp = "^(Y|N)$")
     */
    document.setAccessibility("INVALID");
  }

  private static Instant randomInstant() {
    return Instant.now()
        .truncatedTo(ChronoUnit.MILLIS);
  }
}
