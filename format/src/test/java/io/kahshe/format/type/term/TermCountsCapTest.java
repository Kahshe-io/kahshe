package io.kahshe.format.type.term;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertTrue;

import org.junit.jupiter.api.Test;

/**
 * The per-file term map is capped, and says so when the cap bites.
 *
 * <p>This is the structure whose unbounded growth exhausts a build's heap. The indexing path does
 * not build it — readers stream into a fixed arena and hold nothing per file — but a watch rule
 * matching on tokens genuinely needs one file's counts probeable by token, and there is no smaller
 * shape that answers that. So it is capped rather than removed.
 *
 * <p>The flag matters as much as the cap. Counts that silently stop growing are counts a
 * {@code min_count} rule reads as too low, so it never fires and nothing says why. A capped file
 * must be loudly skipped instead.
 *
 * <p>The cap is passed to the constructor rather than set through a system property, and that is
 * deliberate: a property is read ONCE at class-initialization for production, so a test that sets
 * it afterwards reaches nothing while appearing to pass.
 */
class TermCountsCapTest {

  /**
   * Verified by breaking it: dropping the {@code truncated = true} assignment leaves the flag false
   * while the counts are silently short, which is the failure this exists to prevent.
   */
  @Test
  void pastTheCapTheMapStopsGrowingAndSaysSo() {
    TermCounts counts = new TermCounts(16, 64);
    for (int i = 0; i < 500; i++) {
      counts.add("term" + i);
    }
    assertTrue(counts.truncated(), "the map stopped growing but did not report it");
    assertEquals(64, counts.size(), "grew past the cap");

    // occurrences of terms already present must still accumulate exactly -- the cap withholds new
    // TERMS, it does not stop counting the ones it holds
    long before = counts.countOf("term0");
    assertTrue(before > 0, "precondition: term0 was admitted before the cap");
    counts.add("term0");
    assertEquals(before + 1, counts.countOf("term0"),
        "a capped map stopped counting a term it already holds");

    // and a term rejected by the cap reads as absent rather than as a wrong number
    assertEquals(0, counts.countOf("term499"));
  }

  /** An ordinary file must not be flagged, or every build would degrade its own alerting. */
  @Test
  void anOrdinaryFileIsNotTruncated() {
    TermCounts counts = new TermCounts(16, 16_000_000);
    for (int i = 0; i < 5_000; i++) {
      counts.add("term" + i);
    }
    assertFalse(counts.truncated(), "a 5,000-term file was reported as capped");
    assertEquals(5_000, counts.size());
  }
}
