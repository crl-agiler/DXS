package eu.unicredit.dpu.pipeline.mapping;

import org.junit.jupiter.api.Test;

import java.util.Map;

import static org.assertj.core.api.Assertions.assertThat;

class OutcomeMapperTest {

    @Test
    void shouldMapTerminalOkStatusesToOk() {
        OutcomeMapper mapper = OutcomeMapper.defaultMapper();

        assertThat(mapper.map("CONCLUDED")).isEqualTo(OutcomeMapper.OUTCOME_OK);
        assertThat(mapper.map("CLOSED")).isEqualTo(OutcomeMapper.OUTCOME_OK);
        assertThat(mapper.map("ARCHIVED")).isEqualTo(OutcomeMapper.OUTCOME_OK);
        assertThat(mapper.map("SIGNED")).isEqualTo(OutcomeMapper.OUTCOME_OK);
    }

    @Test
    void shouldMapTerminalKoStatusesToKo() {
        OutcomeMapper mapper = OutcomeMapper.defaultMapper();

        assertThat(mapper.map("EXPIRED")).isEqualTo(OutcomeMapper.OUTCOME_KO);
        assertThat(mapper.map("CANCELLED")).isEqualTo(OutcomeMapper.OUTCOME_KO);
        assertThat(mapper.map("REJECTED")).isEqualTo(OutcomeMapper.OUTCOME_KO);
    }

    @Test
    void shouldMapNonTerminalStatusesToInProgress() {
        OutcomeMapper mapper = OutcomeMapper.defaultMapper();

        assertThat(mapper.map("CREATED")).isEqualTo(OutcomeMapper.OUTCOME_IN_PROGRESS);
        assertThat(mapper.map("PROCESSING")).isEqualTo(OutcomeMapper.OUTCOME_IN_PROGRESS);
        assertThat(mapper.map("READY")).isEqualTo(OutcomeMapper.OUTCOME_IN_PROGRESS);
    }

    @Test
    void shouldDefaultToInProgressForNullStatus() {
        OutcomeMapper mapper = OutcomeMapper.defaultMapper();

        assertThat(mapper.map(null)).isEqualTo(OutcomeMapper.OUTCOME_IN_PROGRESS);
    }

    @Test
    void shouldDefaultToInProgressForUnknownStatus() {
        OutcomeMapper mapper = OutcomeMapper.defaultMapper();

        // An unrecognised status must not throw — pipeline must stay up
        // even if DXS introduces a new status value we don't know about yet.
        assertThat(mapper.map("SOME_FUTURE_STATUS")).isEqualTo(OutcomeMapper.OUTCOME_IN_PROGRESS);
    }

    @Test
    void shouldBeCaseInsensitive() {
        OutcomeMapper mapper = OutcomeMapper.defaultMapper();

        assertThat(mapper.map("concluded")).isEqualTo(OutcomeMapper.OUTCOME_OK);
        assertThat(mapper.map("Cancelled")).isEqualTo(OutcomeMapper.OUTCOME_KO);
    }

    @Test
    void shouldBuildFromExplicitLookupTable() {
        OutcomeMapper mapper = OutcomeMapper.fromLookupTable(Map.of(
                "DONE", "OK",
                "FAILED", "KO"
        ));

        assertThat(mapper.map("DONE")).isEqualTo(OutcomeMapper.OUTCOME_OK);
        assertThat(mapper.map("FAILED")).isEqualTo(OutcomeMapper.OUTCOME_KO);
        assertThat(mapper.map("PENDING")).isEqualTo(OutcomeMapper.OUTCOME_IN_PROGRESS);
    }
}
