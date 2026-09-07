package io.kahshe.format.type.gram;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNotEquals;
import static org.junit.jupiter.api.Assertions.assertTrue;

import java.nio.charset.StandardCharsets;
import org.junit.jupiter.api.Test;

class ByteKeyTest {

  @Test
  void valueEqualityNotIdentity() {
    GramIndexWriter.ByteKey a = GramIndexWriter.ByteKey.of("abc");
    GramIndexWriter.ByteKey b = new GramIndexWriter.ByteKey("abc".getBytes(StandardCharsets.UTF_8));
    assertEquals(a, b);
    assertEquals(a.hashCode(), b.hashCode());
    assertNotEquals(a, GramIndexWriter.ByteKey.of("abd"));
    assertNotEquals(a, GramIndexWriter.ByteKey.of("ab"));
  }

  @Test
  void utf8Encoding() {
    assertEquals(
        GramIndexWriter.ByteKey.of("νας"),
        new GramIndexWriter.ByteKey("νας".getBytes(StandardCharsets.UTF_8)));
  }

  @Test
  void unsignedLexicographicOrder() {
    // 0x80 as a signed byte is negative; unsigned ordering must still put it after 0x7f
    GramIndexWriter.ByteKey high = new GramIndexWriter.ByteKey(new byte[] {(byte) 0x80});
    GramIndexWriter.ByteKey low = new GramIndexWriter.ByteKey(new byte[] {0x7f});
    assertTrue(high.compareTo(low) > 0);
    assertTrue(low.compareTo(high) < 0);
    // prefix sorts before its extension
    assertTrue(GramIndexWriter.ByteKey.of("ab").compareTo(GramIndexWriter.ByteKey.of("abc")) < 0);
    assertEquals(0, GramIndexWriter.ByteKey.of("abc").compareTo(GramIndexWriter.ByteKey.of("abc")));
  }
}
