package io.kahshe.format.type.bloom;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertTrue;

import java.util.HashSet;
import java.util.List;
import java.util.Set;
import org.junit.jupiter.api.Test;
import io.kahshe.format.type.gram.Grams;

class NgramBloomTest {
  private static final List<String> CORPUS =
      List.of(
          "connection timeout to broker seq=3000",
          "cache warm complete",
          "marker=quorum-epoch-777",
          "disk pressure rising");

  private static NgramBloom build() {
    Set<String> grams = new HashSet<>();
    CORPUS.forEach(v -> grams.addAll(NgramBloom.gramsOf(v, Grams.Contract.current(3))));
    return NgramBloom.build(grams, Grams.Contract.current(3), 0.01);
  }

  @Test
  void neverFalseNegativeForContains() {
    NgramBloom bloom = build();
    // every substring of every indexed value must probe true (the correctness invariant)
    for (String value : CORPUS) {
      for (int start = 0; start + 3 <= value.length(); start += 2) {
        for (int end = start + 3; end <= Math.min(value.length(), start + 12); end++) {
          String needle = value.substring(start, end);
          assertTrue(
              bloom.mightContain(needle, NgramBloom.Mode.CONTAINS),
              "false negative for substring: " + needle);
        }
      }
    }
  }

  @Test
  void caseInsensitive() {
    NgramBloom bloom = build();
    assertTrue(bloom.mightContain("QUORUM-EPOCH", NgramBloom.Mode.CONTAINS));
  }

  @Test
  void definitelyAbsentGramProbesFalse() {
    NgramBloom bloom = build();
    // not guaranteed per-probe (FP possible), but overwhelmingly true at 1% FPP:
    int hits = 0;
    for (int i = 0; i < 200; i++) {
      if (bloom.mightContain("zzq" + i + "xvj", NgramBloom.Mode.CONTAINS)) {
        hits++;
      }
    }
    assertTrue(hits < 40, "false-positive rate far above target: " + hits + "/200");
  }

  @Test
  void shortLiteralsOnlyPruneEq() {
    NgramBloom bloom = build();
    assertTrue(bloom.mightContain("zq", NgramBloom.Mode.CONTAINS)); // too short: never prune
    assertTrue(bloom.mightContain("zq", NgramBloom.Mode.STARTS_WITH));
  }

  @Test
  void serdeRoundTrip() {
    NgramBloom bloom = build();
    NgramBloom back = NgramBloom.deserialize(bloom.serialize(), Grams.Rule.CODEPOINT_V2);
    assertEquals(bloom.sizeBytes(), back.sizeBytes());
    assertTrue(back.mightContain("quorum-epoch-777", NgramBloom.Mode.CONTAINS));
  }
}
