package io.kahshe.analysis;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNull;

import java.math.BigDecimal;
import java.nio.ByteBuffer;
import java.util.UUID;
import org.junit.jupiter.api.Test;

/**
 * The canonical forms as golden strings: what the index WRITES for each kind, pinned outside the
 * code that computes them.
 *
 * <p>Every other test of these forms is a self-consistency test — build a value, probe with the
 * same value, expect a hit — and stays green when a form changes on both sides at once. That is
 * the wrong kind of green: every index already written holds the OLD form, and a probe computing
 * the new one silently finds nothing in it. FORMAT.md §6.4 is the contract; these are its bytes.
 */
class CanonicalTest {

  @Test
  void integralIsDecimalTextWhateverTheBoxedType() {
    assertEquals("42", Canonical.form(ValueKind.INTEGRAL, 42));
    assertEquals("42", Canonical.form(ValueKind.INTEGRAL, 42L));
    assertEquals("-7", Canonical.form(ValueKind.INTEGRAL, -7));
    assertEquals("42", Canonical.form(ValueKind.INTEGRAL, " 42 "), "a text literal is trimmed and parsed");
    assertNull(Canonical.form(ValueKind.INTEGRAL, "4x2"), "unparseable text is not a form: keep every file");
  }

  @Test
  void decimalStripsTrailingZerosSoScaleAndLiteralAgree() {
    assertEquals("12.5", Canonical.form(ValueKind.DECIMAL, new BigDecimal("12.50")));
    assertEquals("12.5", Canonical.form(ValueKind.DECIMAL, "12.5"));
    assertEquals("7", Canonical.form(ValueKind.DECIMAL, new BigDecimal("7.00")),
        "an integer-valued decimal is its digits, with no point");
    assertEquals("0.001", Canonical.form(ValueKind.DECIMAL, "0.001000"));
  }

  @Test
  void uuidIsThirtyTwoLowercaseHexDigitsWithNoDashes() {
    UUID u = UUID.fromString("550E8400-E29B-41D4-A716-446655440000");
    assertEquals("550e8400e29b41d4a716446655440000", Canonical.form(ValueKind.UUID, u));
    assertEquals("550e8400e29b41d4a716446655440000",
        Canonical.form(ValueKind.UUID, "550e8400-e29b-41d4-a716-446655440000"),
        "a dashed text literal -- a JSON plan filter writes them that way -- reaches the same form");
    assertNull(Canonical.form(ValueKind.UUID, "not-a-uuid"));
  }

  @Test
  void binaryIsLowercaseHexOfTheBytes() {
    byte[] bytes = {(byte) 0xDE, (byte) 0xAD, 0x00, 0x7f};
    assertEquals("dead007f", Canonical.form(ValueKind.BINARY, bytes));
    assertEquals("dead007f", Canonical.form(ValueKind.BINARY, ByteBuffer.wrap(bytes)));
    assertNull(Canonical.form(ValueKind.BINARY, "dead007f"),
        "text on a binary column is NOT parsed: the predicate is not indexable");
  }

  @Test
  void stringIsItselfAndNullIsNull() {
    assertEquals("Hello WORLD", Canonical.form(ValueKind.STRING, "Hello WORLD"),
        "case-preserving: the analyzer contract, not the canonical form, decides folding");
    assertNull(Canonical.form(ValueKind.STRING, null));
  }

  @Test
  void aKindWithNoFormAnswersNullWhichTheReaderTreatsAsKeepEveryFile() {
    for (ValueKind kind : new ValueKind[] {
        ValueKind.FLOATING, ValueKind.BOOLEAN, ValueKind.DATE, ValueKind.TIMESTAMP,
        ValueKind.TIMESTAMPTZ, ValueKind.OTHER}) {
      assertNull(Canonical.form(kind, "anything"), kind.name());
    }
  }
}
