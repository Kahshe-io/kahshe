package io.kahshe.proxy.catalog;

import io.kahshe.common.Metrics;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.junit.jupiter.api.Assertions.assertTrue;

import java.nio.charset.StandardCharsets;
import org.junit.jupiter.api.Test;

/**
 * Degradations that are correct and invisible.
 *
 * <p>Every one of these does the safe thing: pass the body through, drop the job, treat the table
 * as unindexed. Advisory-keep means none of them can produce a wrong answer — which is exactly
 * what makes them dangerous. The observable symptom of all three is "queries are not getting
 * faster", indistinguishable from data that is simply not selective.
 *
 * <p>These assert the COUNTERS move, because a log line alone cannot be alerted on and a
 * degradation nobody can alert on is one nobody finds.
 */
class SilentDegradationTest {

  /**
   * A malformed config response leaves the plan endpoints unadvertised, so every client keeps
   * planning locally and kahshe does nothing at all.
   *
   * <p>Verified by breaking it: removing the {@code REWRITE_FAILURES.increment()} leaves the count
   * flat while the rewrite has silently stopped happening fleet-wide.
   */
  @Test
  void aFailedConfigRewriteIsCounted() {
    long before = Mutations.REWRITE_FAILURES.sum();
    byte[] notJson = "{{{ this is not a config response".getBytes(StandardCharsets.UTF_8);

    byte[] out = Mutations.mergeConfigEndpoints(notJson);

    assertEquals(
        new String(notJson, StandardCharsets.UTF_8),
        new String(out, StandardCharsets.UTF_8),
        "a body that cannot be rewritten must pass through unmodified");
    assertEquals(
        before + 1,
        Mutations.REWRITE_FAILURES.sum(),
        "the rewrite failed and nothing counted it; the plan endpoints are now unadvertised and "
            + "the only symptom is that kahshe appears to do nothing");
  }

  /** Same for the loadTable injection: no injection means the client plans locally, silently. */
  @Test
  void aFailedLoadTableRewriteIsCounted() {
    long before = Mutations.REWRITE_FAILURES.sum();
    byte[] notJson = "not a LoadTableResponse".getBytes(StandardCharsets.UTF_8);

    assertEquals(
        new String(notJson, StandardCharsets.UTF_8),
        new String(Mutations.injectServerPlanning(notJson).body(), StandardCharsets.UTF_8));
    assertEquals(before + 1, Mutations.REWRITE_FAILURES.sum());
  }

  /**
   * A malformed body must not be read as "this table declares no index policy".
   *
   * <p>Returning null is right — there is nothing to act on — but it means a table that DOES
   * declare `kahshe.index` is never indexed, with no error anywhere.
   */
  @Test
  void anUnparseableBodyYieldsNoPolicyRatherThanAWrongOne() {
    assertNull(Mutations.indexPolicy("not json at all".getBytes(StandardCharsets.UTF_8)));
  }

  /** Both counters must reach the scrape, or nothing can alert on them. */
  @Test
  void theNewCountersAreExposed() {
    String text = new Metrics().scrape();
    assertTrue(text.contains("kahshe_response_rewrite_failures_total"), text);
    assertTrue(text.contains("kahshe_indexer_jobs_dropped_total"), text);
  }
}
