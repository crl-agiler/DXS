package eu.unicredit.document.dxstraceinfo.transform;

import eu.unicredit.document.dxstraceinfo.api.ErrorHandler;
import eu.unicredit.document.dxstraceinfo.avro.DossierTraceinfoEvent;
import eu.unicredit.document.dxstraceinfo.mapping.AvroRowDataConverters;
import eu.unicredit.document.dxstraceinfo.tools.JsonUtils;
import org.apache.commons.collections.CollectionUtils;
import org.apache.commons.lang3.ObjectUtils;
import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.api.functions.KeyedProcessFunction;
import org.apache.flink.table.data.GenericRowData;
import org.apache.flink.table.data.RowData;
import org.apache.flink.table.data.TimestampData;
import org.apache.flink.util.Collector;
import org.apache.flink.util.OutputTag;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import javax.validation.ConstraintViolation;
import javax.validation.Validation;
import javax.validation.Validator;
import javax.validation.ValidatorFactory;
import java.time.Instant;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.Set;
import java.util.stream.Collectors;
import eu.unicredit.document.dxstraceinfo.model.ChangeDetectable;
import org.apache.flink.api.common.state.MapState;
import org.apache.flink.api.common.state.MapStateDescriptor;
import org.apache.flink.api.common.state.StateTtlConfig;
import org.apache.flink.api.common.time.Time;
import org.apache.flink.api.common.typeinfo.Types;
/**
 * Validates and transforms dossier trace-info events.
 *
 * <p>Each input event is validated once before executing the configured split contexts. An invalid
 * event produces exactly one record in the discard side output.
 *
 * <p>For a valid event, every split context extracts its business objects, maps them to {@link
 * RowData}, and emits them to the corresponding side output.
 *
 * <p>If extraction or mapping fails, the event is written once to the discard side output with a
 * structured JSON error.
 */
public class SplitTransformLogic extends KeyedProcessFunction<Long, DossierTraceinfoEvent, RowData> {

  private static final Logger LOGGER =
      LoggerFactory.getLogger(SplitTransformLogic.class);

  private final List<SplitContext<?>> splitContexts;
  private final ErrorHandler<DossierTraceinfoEvent> errorHandler;

  private transient ValidatorFactory validatorFactory;
  private transient Validator validator;
  private transient StatusChangeFilter statusChangeFilter;  /**
   * Creates the transformation function.
   *
   * @param splitContexts contexts used to extract, map, and emit business records
   * @param errorHandler side output used for rejected events
   * @throws NullPointerException if an argument is {@code null}
   */
  public SplitTransformLogic(
      List<SplitContext<?>> splitContexts,
      ErrorHandler<DossierTraceinfoEvent> errorHandler) {

    this.splitContexts =
        Objects.requireNonNull(
            splitContexts,
            "splitContexts must not be null");

    this.errorHandler =
        Objects.requireNonNull(
                errorHandler,
            "errorHandler must not be null");
  }

  /**
   * Initializes Bean Validation.
   *
   * @param parameters Flink function configuration
   * @throws Exception if initialization fails
   */
  @Override
  public void open(Configuration parameters) throws Exception {
    super.open(parameters);

    validatorFactory =
        Validation.buildDefaultValidatorFactory();

    validator =
        validatorFactory.getValidator();
    MapStateDescriptor<String, String> descriptor =
            new MapStateDescriptor<>("history-last-status", Types.STRING, Types.STRING);
    descriptor.enableTimeToLive(
            StateTtlConfig.newBuilder(Time.days(90))
                    .setUpdateType(StateTtlConfig.UpdateType.OnCreateAndWrite)
                    .setStateVisibility(StateTtlConfig.StateVisibility.NeverReturnExpired)
                    .build());
    statusChangeFilter = new StatusChangeFilter(getRuntimeContext().getMapState(descriptor));
  }

  /**
   * Validates and processes one event.
   *
   * <p>Validation is performed once, before iterating through the split contexts. Therefore, one
   * invalid input event produces exactly one discard record.
   *
   * @param event     input Avro event
   * @param context   Flink processing context
   * @param collector unused main-output collector
   */
  @Override
  public void processElement(
      DossierTraceinfoEvent event,
      Context context,
      Collector<RowData> collector) {

    LOGGER.debug("Reading event {}", event);

    Set<ConstraintViolation<DossierTraceinfoEvent>> violations =
        validator.validate(event);

    if (!violations.isEmpty()) {
      String validationError =
          createValidationError(violations);

      LOGGER.info(
          "Event validation failed: {}",
          validationError);
      this.errorHandler.handle(event, validationError, context::output);
      return;
    }

    try {
      for (SplitContext<?> splitContext : splitContexts) {
        emit(
            splitContext,
            event,
            context);
      }

      LOGGER.debug("Event processed correctly");

    } catch (Exception exception) {
      LOGGER.error(
          "Exception occurred during extraction or mapping. Sending event to the discard table",
          exception);
      this.errorHandler.handle(event, createProcessingError(exception), context::output);
    }
  }

  /**
   * Extracts, maps, and emits the objects associated with one split context.
   *
   * @param splitContext split context containing extractor, mapper, and output tag
   * @param event        input event
   * @param context      Flink processing context
   * @param <T>          extracted object type
   * @throws Exception if extraction or mapping fails
   */
  private <T> void emit(
      SplitContext<T> splitContext,
      DossierTraceinfoEvent event,
      Context context)
      throws Exception {

    List<T> objects =
        splitContext.getExtractor()
            .map(event);

    if (CollectionUtils.isEmpty(objects)) {
      return;
    }

    MapFunction<T, RowData> mapper =
        splitContext.getMapper();

    OutputTag<RowData> outputTag =
        splitContext.getOutputTag();
    TimestampData eventTimestamp = AvroRowDataConverters.timestamp(event.getEventTimestamp());
    TimestampData processingTimestamp = AvroRowDataConverters.timestamp(Instant.now());
    for (T object : objects) {
      if (object instanceof ChangeDetectable
              && !statusChangeFilter.hasChanged((ChangeDetectable) object)) {
        continue;
      }
      GenericRowData basicRowData = (GenericRowData) mapper.map(object);
      GenericRowData map = enrichRowData(basicRowData, eventTimestamp, processingTimestamp);
      context.output(outputTag, map);
    }
  }

  private boolean hasChanged(ChangeDetectable detectable) throws Exception {
    String key = detectable.changeKey();
    String current = detectable.changeValue();
    if (current.equals(lastStatusState.get(key))) {
      return false;
    }
    lastStatusState.put(key, current);
    return true;
  }


  private GenericRowData enrichRowData(GenericRowData basicRowData, TimestampData eventTimestamp, TimestampData processingTimestamp) {
    int arity = basicRowData.getArity();
    GenericRowData genericRowData = new GenericRowData(arity + 2);
    int i = 0;
    for (; i < arity; i++) {
      genericRowData.setField(i, basicRowData.getField(i));
    }
    genericRowData.setField(i, processingTimestamp);
    genericRowData.setField(++i, eventTimestamp);
    return genericRowData;
  }

  /**
   * Creates the JSON representation of the validation errors.
   *
   * @param violations validation errors found on the event
   * @return validation errors serialized as JSON
   */
  private String createValidationError(
      Set<ConstraintViolation<DossierTraceinfoEvent>> violations) {

    List<Map<String, Object>> errors =
        violations.stream()
            .map(this::toValidationError)
            .collect(Collectors.toList());

    return JsonUtils.toJson(
        Map.of(
            "errorType", "VALIDATION_ERROR",
            "violationCount", errors.size(),
            "violations", errors));
  }

  /**
   * Converts one constraint violation to a JSON-compatible map.
   *
   * <p>{@link Map#of(Object, Object, Object, Object, Object, Object, Object, Object)} does not
   * support {@code null} values. The invalid value is therefore represented by the string {@code
   * "null"} when absent.
   *
   * @param violation constraint violation
   * @return validation-error details
   */
  private Map<String, Object> toValidationError(
      ConstraintViolation<DossierTraceinfoEvent> violation) {

    return Map.of(
        "field",
        violation.getPropertyPath().toString(),
        "invalid_value",
        ObjectUtils.defaultIfNull(
            violation.getInvalidValue(),
            "null"),
        "error",
        violation.getMessage(),
        "constraint",
        violation
            .getConstraintDescriptor()
            .getAnnotation()
            .annotationType()
            .getSimpleName());
  }

  /**
   * Creates structured JSON for an extraction or mapping error.
   *
   * @param exception processing exception
   * @return processing error serialized as JSON
   */
  private String createProcessingError(
      Exception exception) {

    return JsonUtils.toJson(
        Map.of(
            "errorType",
            "PROCESSING_ERROR",
            "exceptionType",
            exception.getClass().getName(),
            "message",
            ObjectUtils.defaultIfNull(
                exception.getMessage(),
                "")));
  }


  /**
   * Closes the Bean Validation factory.
   *
   * @throws Exception if resource cleanup fails
   */
  @Override
  public void close() throws Exception {
    if (validatorFactory != null) {
      validatorFactory.close();
    }

    super.close();
  }
}
