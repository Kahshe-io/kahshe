package io.kahshe.indexer.maintain;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertTrue;

import io.kahshe.common.Metrics;
import java.util.List;
import org.junit.jupiter.api.Test;
import io.kahshe.indexer.LocalTableFixture;

/**
 * A replica with the indexer OFF observes tables without enqueueing builds nobody will drain.
 *
 * <p>{@code enabled} has to gate the enqueue, not only the worker thread: gating the worker
 * alone, a proxy-only replica fills its 256-slot queue on the 257th distinct observation and then
 * WARNs a drop and moves {@code kahshe_indexer_jobs_dropped_total} on every later one — a replica
 * doing exactly what it was configured to do, reading as broken. Freshness is still recorded, so
 * the per-replica gauges stay honest. Verified red with the early return removed: 300
 * observations drop 44.
 *
 * <p>Nor does it read behind: a pod that cannot build is not
 * the one to raise the gap, and a gauge climbing without bound on a healthy pod by configuration
 * is the page that gets muted. Verified red with {@code builds} ignored in {@code IndexFreshness}:
 * behind 300, max behind 900.
 */
class IndexerObserveTest {
  @Test
  void aDisabledIndexerRecordsFreshnessAndEnqueuesNothing() {
    Metrics metrics = new Metrics();
    IndexerService indexer =
        new IndexerService(
            LocalTableFixture.noTables(), metrics, false,
            LocalTableFixture.config());
    for (int i = 0; i < 300; i++) {
      indexer.observe("lake", "logs", "t" + i, List.of("msg"), 1000L + i);
    }
    assertEquals(
        0, metrics.indexerJobsDropped.sum(),
        "no queue to fill: an indexer that is off must not report drops");
    assertTrue(
        metrics.indexTablesTracked.getAsLong() >= 300,
        "but every observation is still tracked for the freshness gauges");
  }

  @Test
  void aDisabledIndexerTracksWhatItSeesButReportsNothingBehindAndWarnsOfNothing() {
    Metrics metrics = new Metrics();
    IndexerService indexer =
        new IndexerService(
            LocalTableFixture.noTables(), metrics, false,
            LocalTableFixture.config());
    java.util.concurrent.atomic.AtomicLong clock = new java.util.concurrent.atomic.AtomicLong(1_000_000);
    java.util.List<String> warnings = new java.util.ArrayList<>();
    indexer.freshness.nowMs = clock::get;
    indexer.freshness.warnSink = warnings::add;

    for (int i = 0; i < 300; i++) {
      indexer.observe("lake", "logs", "t" + i, List.of("msg"), 1000L + i);
    }
    clock.addAndGet(900_000); // well past any stale-warn threshold
    for (int i = 0; i < 300; i++) {
      indexer.observe("lake", "logs", "t" + i, List.of("msg"), 1000L + i);
    }

    assertEquals(300, metrics.indexTablesTracked.getAsLong(), "what it saw is still counted");
    assertEquals(0, metrics.indexTablesBehind.getAsLong(), "nothing is behind on a pod that cannot build");
    assertEquals(0, metrics.indexMaxBehindSeconds.getAsLong());
    assertTrue(warnings.isEmpty(), "and it warns of nothing: " + warnings);
  }
}
