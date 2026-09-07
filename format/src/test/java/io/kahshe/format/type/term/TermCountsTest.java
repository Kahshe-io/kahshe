package io.kahshe.format.type.term;

import static org.junit.jupiter.api.Assertions.assertEquals;

import java.util.HashMap;
import java.util.Map;
import java.util.Random;
import java.util.TreeMap;
import org.junit.jupiter.api.Test;

/**
 * TermCounts must count exactly what a HashMap would.
 *
 * <p>It replaces {@code HashMap<String, Long>} on the build's hottest path, and the counts it
 * produces go straight into the aggregate leaf as each term's occurrence total. A count that is
 * wrong is a wrong answer from _count; a term that is LOST is a file wrongly pruned, which is worse.
 * So this asserts equality against the structure it replaced rather than testing it in isolation.
 */
class TermCountsTest {

  @Test
  void countsExactlyWhatAHashMapWould() {
    Random random = new Random(31337L);
    // deliberately few distinct values relative to occurrences, so most adds hit an existing key,
    // and deliberately similar strings, which is what a corpus of hex identifiers looks like
    String[] vocabulary = new String[5_000];
    for (int i = 0; i < vocabulary.length; i++) {
      vocabulary[i] = Long.toHexString(0x1000_0000_0000_0000L + random.nextInt(1_000_000));
    }

    Map<String, Long> expected = new HashMap<>();
    TermCounts actual = new TermCounts(16); // start tiny to force many rehashes
    for (int i = 0; i < 200_000; i++) {
      String term = vocabulary[random.nextInt(vocabulary.length)];
      expected.merge(term, 1L, Long::sum);
      actual.add(term);
    }

    assertEquals(expected.size(), actual.size(), "distinct term count disagrees");
    Map<String, Long> drained = new TreeMap<>();
    try {
      actual.forEach(drained::put);
    } catch (java.io.IOException e) {
      throw new AssertionError(e);
    }
    assertEquals(new TreeMap<>(expected), drained, "the counts disagree with a HashMap's");

    // and point lookups agree, including for terms that are absent
    for (String term : vocabulary) {
      assertEquals(expected.getOrDefault(term, 0L), actual.countOf(term), term);
    }
    assertEquals(0L, actual.countOf("no-such-term"), "an absent term must count zero");
  }

  // No size-estimate guard: the sort-and-merge build decides nothing from an estimate of what a
  // growable structure costs, and what TermCounts does -- one file's counts, probed by token for a
  // watch rule -- is covered by the HashMap oracle above.

  @Test
  void handlesWeightsAndEmptyAndUnicodeKeys() {
    TermCounts counts = new TermCounts(16);
    counts.add("a", 5);
    counts.add("a", 7);
    assertEquals(12L, counts.countOf("a"), "weighted adds must accumulate");

    counts.add("");
    counts.add("中文");
    counts.add("😀");
    assertEquals(1L, counts.countOf(""));
    assertEquals(1L, counts.countOf("中文"));
    assertEquals(1L, counts.countOf("😀"));
    assertEquals(4, counts.size());
  }
}
