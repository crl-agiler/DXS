package eu.unicredit.document.dxstraceinfo.transform;

import org.junit.jupiter.api.Test;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

class SignerIdGeneratorTest {

  @Test
  void shouldGenerateCompositeKey() {
    String signerId = SignerIdGenerator.generate(1400340L, 482119L, "0000000022516018");

    assertThat(signerId).isEqualTo("1400340-482119-0000000022516018");
  }

  @Test
  void shouldBeDeterministic() {
    // Same inputs must always produce the same key — required for
    // Iceberg upsert correctness when the same signer reappears in a
    // later event (e.g. a second status change on the same group).
    String first = SignerIdGenerator.generate(1400340L, 482119L, "018");
    String second = SignerIdGenerator.generate(1400340L, 482119L, "018");

    assertThat(first).isEqualTo(second);
  }

  @Test
  void shouldDifferentiateSameSignerOnDifferentGroups() {
    // Same person (ndg) signing two different DocumentGroups of the
    // same Dossier (e.g. main contract + later addendum) must produce
    // two distinct rows, not collapse into one.
    String onFirstGroup = SignerIdGenerator.generate(1400340L, 482119L, "018");
    String onSecondGroup = SignerIdGenerator.generate(1400340L, 482120L, "018");

    assertThat(onFirstGroup).isNotEqualTo(onSecondGroup);
  }

  @Test
  void shouldThrowWhenNdgIsNull() {
    assertThatThrownBy(() -> SignerIdGenerator.generate(1400340L, 482119L, null))
        .isInstanceOf(IllegalArgumentException.class)
        .hasMessageContaining("ndg");
  }

  @Test
  void shouldThrowWhenNdgIsBlank() {
    assertThatThrownBy(() -> SignerIdGenerator.generate(1400340L, 482119L, "   "))
        .isInstanceOf(IllegalArgumentException.class);
  }

  @Test
  void shouldTrimNdgWhitespace() {
    String signerId = SignerIdGenerator.generate(1L, 2L, "  018  ");

    assertThat(signerId).isEqualTo("1-2-018");
  }
}
