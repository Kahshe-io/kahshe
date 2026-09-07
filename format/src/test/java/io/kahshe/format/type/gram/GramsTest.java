package io.kahshe.format.type.gram;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

import java.util.List;
import java.util.Set;
import org.junit.jupiter.api.Test;
import io.kahshe.format.type.bloom.NgramBloom;

/**
 * The gram rule. v2 cuts whole code points, so a gram is something a Rust or Go implementation can
 * reproduce byte for byte; v1 cut UTF-16 units and split surrogate pairs. Verified red with the
 * code-point branch removed from
 * {@code Grams.Contract.forEachWindow}: the v2 grams then hold lone surrogates.
 */
class GramsTest {
  /** a, b, U+1F600 (two UTF-16 units), c, d. */
  private static final String MIXED = "ab😀cd";

  @Test
  void v2WindowsAreWholeCodePoints() {
    Set<String> grams = Grams.Contract.current(3).gramsOf(MIXED);
    assertEquals(Set.of("ab😀", "b😀c", "😀cd"), grams);
    for (String gram : grams) {
      assertEquals(3, gram.codePointCount(0, gram.length()), gram);
      assertFalse(hasLoneSurrogate(gram), "a v2 gram must be a well-formed string: " + gram);
    }
  }

  @Test
  void v1WindowsSplitSurrogatePairsAndAreStillReproduced() {
    Set<String> grams = Grams.Contract.v1().gramsOf(MIXED);
    assertEquals(4, grams.size(), "four windows of three UTF-16 units over six units");
    assertTrue(grams.stream().anyMatch(GramsTest::hasLoneSurrogate),
        "the legacy rule splits the pair; a reader of a v1 index must reproduce that cut");
  }

  @Test
  void theAccumulatorReproducesTheReferenceCutAtEverySize() {
    for (int size : List.of(2, 3, 4, 5, 8)) {
      Grams.Contract c = Grams.Contract.current(size);
      for (String value : List.of(MIXED, "Hello, World", "xy", "", "😀", "0123456789abcdef")) {
        GramAccumulator acc = new GramAccumulator(c, 16);
        acc.addAll(value);
        assertEquals(c.gramsOf(value), acc.toStrings(), "size " + size + " value " + value);
      }
    }
    GramAccumulator v1 = new GramAccumulator(Grams.Contract.v1(), 16);
    v1.addAll(MIXED);
    assertEquals(Grams.Contract.v1().gramsOf(MIXED), v1.toStrings());
  }

  @Test
  void idsRoundTripAndUnknownFamiliesAreRefused() {
    assertEquals("kahshe-grams-v2-n4", Grams.Contract.current(4).id());
    assertEquals(Grams.Contract.current(4), Grams.Contract.of("kahshe-grams-v2-n4"));
    assertEquals(Grams.Contract.v1(), Grams.Contract.of(null), "a document without the property is v1");
    assertEquals(Grams.Contract.v1(), Grams.Contract.of("kahshe-grams-v1"));
    assertThrows(IllegalStateException.class, () -> Grams.Contract.of("kahshe-grams-v9"));
    assertThrows(IllegalStateException.class, () -> Grams.Contract.of("kahshe-grams-v2-n12"));
    assertThrows(IllegalArgumentException.class, () -> Grams.Contract.current(1));
    assertThrows(IllegalArgumentException.class, () -> Grams.Contract.current(9));
    assertThrows(IllegalArgumentException.class, () -> new Grams.Contract(Grams.Rule.UTF16_V1, 4));
  }

  @Test
  void aBloomProbesUnderTheRuleItIsDeserializedWith() {
    Grams.Contract v1 = Grams.Contract.v1();
    NgramBloom bloom = NgramBloom.build(v1.gramsOf(MIXED), v1, 0.01);
    byte[] bytes = bloom.serialize();
    assertTrue(NgramBloom.deserialize(bytes, Grams.Rule.UTF16_V1)
        .mightContain("b😀c", NgramBloom.Mode.CONTAINS), "v1 probe reproduces v1's cut");
    assertFalse(NgramBloom.deserialize(bytes, Grams.Rule.CODEPOINT_V2)
        .mightContain("b😀c", NgramBloom.Mode.CONTAINS),
        "the same blob probed under v2 asks for grams v1 never wrote: the rule is the reader's to get right");
    NgramBloom four = NgramBloom.build(Grams.Contract.current(4).gramsOf("hello world"), Grams.Contract.current(4), 0.01);
    assertEquals(4, NgramBloom.deserialize(four.serialize(), Grams.Rule.CODEPOINT_V2).ngram());
    assertTrue(four.mightContain("lo w", NgramBloom.Mode.CONTAINS));
    assertFalse(four.mightContain("zzzz", NgramBloom.Mode.CONTAINS));
  }

  private static boolean hasLoneSurrogate(String s) {
    for (int i = 0; i < s.length(); i++) {
      char c = s.charAt(i);
      if (Character.isHighSurrogate(c)) {
        if (i + 1 >= s.length() || !Character.isLowSurrogate(s.charAt(i + 1))) {
          return true;
        }
        i++;
      } else if (Character.isLowSurrogate(c)) {
        return true;
      }
    }
    return false;
  }
}
