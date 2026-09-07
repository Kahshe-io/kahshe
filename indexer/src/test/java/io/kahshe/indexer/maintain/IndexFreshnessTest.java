package io.kahshe.indexer.maintain;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertTrue;

import io.kahshe.common.Metrics;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.atomic.AtomicLong;
import org.junit.jupiter.api.Test;
import io.kahshe.indexer.LocalTableFixture;

class IndexFreshnessTest {
  private static final String KEY = "p|ns|tbl";
  private static final long WARN_MS = 60_000;

  private final Metrics metrics = new Metrics();
  private final AtomicLong clock = new AtomicLong(1_000_000);
  private final List<String> warnings = new ArrayList<>();
  private final IndexFreshness freshness = freshness();

  private IndexFreshness freshness() {
    IndexFreshness tracker = new IndexFreshness(metrics, WARN_MS);
    tracker.nowMs = clock::get;
    tracker.warnSink = warnings::add;
    return tracker;
  }

  @Test
  void observedThenBuiltReadsCurrent() {
    freshness.observed(KEY, 5);
    freshness.built(KEY, 5, 5);

    assertEquals(1, metrics.indexTablesTracked.getAsLong());
    assertEquals(0, metrics.indexTablesBehind.getAsLong());
    assertEquals(0, metrics.indexMaxBehindSeconds.getAsLong());
    assertEquals(0, metrics.indexLastBuildAgeSeconds.getAsLong());

    // re-observing the built snapshot (the indexer's dedup path) stays tracked and current
    clock.addAndGet(30_000);
    freshness.observed(KEY, 5);
    assertEquals(1, metrics.indexTablesTracked.getAsLong());
    assertEquals(0, metrics.indexTablesBehind.getAsLong());
    assertEquals(0, metrics.indexMaxBehindSeconds.getAsLong());
    assertEquals(30, metrics.indexLastBuildAgeSeconds.getAsLong());
  }

  @Test
  void newerSnapshotWithoutBuildGrowsAndCatchingUpClearsIt() {
    freshness.observed(KEY, 5);
    freshness.built(KEY, 5, 5);

    // a commit lands and is observed; nothing builds it
    clock.addAndGet(1_000);
    freshness.observed(KEY, 9);
    assertEquals(1, metrics.indexTablesBehind.getAsLong());
    // just observed: a small number, never zero — zero is reserved for "nothing is behind"
    assertEquals(1, metrics.indexMaxBehindSeconds.getAsLong());

    clock.addAndGet(120_000);
    assertEquals(120, metrics.indexMaxBehindSeconds.getAsLong());

    // repeat observations of the same uncovered snapshot must not restart the clock
    freshness.observed(KEY, 9);
    assertEquals(120, metrics.indexMaxBehindSeconds.getAsLong());

    freshness.built(KEY, 9, 9);
    assertEquals(0, metrics.indexTablesBehind.getAsLong());
    assertEquals(0, metrics.indexMaxBehindSeconds.getAsLong());
    assertEquals(1, metrics.indexTablesTracked.getAsLong());
  }

  @Test
  void neverBuiltTableIsBehindFromFirstObservation() {
    freshness.observed(KEY, 5);
    assertEquals(1, metrics.indexTablesBehind.getAsLong());
    assertEquals(1, metrics.indexMaxBehindSeconds.getAsLong());
    assertEquals(0, metrics.indexLastBuildAgeSeconds.getAsLong());

    clock.addAndGet(3_600_000);
    assertEquals(3_600, metrics.indexMaxBehindSeconds.getAsLong());
  }

  @Test
  void failedBuildPutsTheTableBackBehind() {
    freshness.observed(KEY, 5);
    freshness.built(KEY, 5, 5);
    assertEquals(0, metrics.indexTablesBehind.getAsLong());

    clock.addAndGet(1_000);
    freshness.failed(KEY);
    assertEquals(1, metrics.indexTablesBehind.getAsLong());
    assertEquals(1, metrics.indexMaxBehindSeconds.getAsLong());
  }

  @Test
  void observationArrivingDuringABuildIsNotSatisfiedByIt() {
    freshness.observed(KEY, 5); // queued at snapshot 5
    clock.addAndGet(2_000);
    freshness.observed(KEY, 9); // a commit lands while the build runs
    clock.addAndGet(1_000);
    freshness.built(KEY, 5, 5); // the pass only covered 5

    assertEquals(1, metrics.indexTablesBehind.getAsLong());
    assertEquals(1, metrics.indexMaxBehindSeconds.getAsLong()); // dated from the 9 observation
  }

  @Test
  void trackerIsBounded() {
    for (int i = 0; i < IndexFreshness.MAX_TRACKED + 500; i++) {
      freshness.observed("p|ns|t" + i, i);
    }
    assertEquals(IndexFreshness.MAX_TRACKED, metrics.indexTablesTracked.getAsLong());
    assertEquals(IndexFreshness.MAX_TRACKED, metrics.indexTablesBehind.getAsLong());
  }

  @Test
  void staleWarnIsRateLimitedPerTable() {
    freshness.observed(KEY, 5);
    assertTrue(warnings.isEmpty(), "not yet past the threshold");

    clock.addAndGet(WARN_MS + 1_000);
    freshness.observed(KEY, 5);
    assertEquals(1, warnings.size());
    String warning = warnings.get(0);
    assertTrue(warning.contains(KEY), warning);
    assertTrue(warning.contains("snapshot 5"), warning);
    assertTrue(warning.contains("last built none"), warning);
    assertTrue(warning.contains("behind for 61s"), warning);

    // repeated staleness inside the interval stays at one warning
    for (int i = 0; i < 20; i++) {
      clock.addAndGet(1_000);
      freshness.observed(KEY, 5);
    }
    assertEquals(1, warnings.size());

    clock.addAndGet(WARN_MS);
    freshness.observed(KEY, 5);
    assertEquals(2, warnings.size());

    // catching up ends the warnings
    freshness.built(KEY, 5, 5);
    clock.addAndGet(WARN_MS * 10);
    freshness.observed(KEY, 5);
    assertEquals(2, warnings.size());
  }

  /**
   * Wired through a LIVE indexer: one whose worker is parked in a load that never returns, so the
   * only state changes are the ones this test makes. An indexer that is off reads nothing behind
   * by design ({@code IndexerObserveTest}), so it cannot be the subject here.
   */
  @Test
  void indexerObservationsReachTheScrape() {
    Metrics wired = new Metrics();
    IndexerService indexer =
        new IndexerService(LocalTableFixture.parked(), wired, true, LocalTableFixture.config());
    indexer.freshness.nowMs = clock::get;

    indexer.observe("p", "ns", "tbl", List.of("msg"), 5);
    clock.addAndGet(45_000);

    String scrape = wired.scrape();
    assertTrue(scrape.contains("kahshe_index_tables_tracked 1"), scrape);
    assertTrue(scrape.contains("kahshe_index_tables_behind 1"), scrape);
    assertTrue(scrape.contains("kahshe_index_max_behind_seconds 45"), scrape);
    assertTrue(scrape.contains("kahshe_index_last_build_age_seconds 0"), scrape);

    // the worker's completion, as runWorker records it: back to current
    indexer.freshness.built("p|ns|tbl", 5, 5);
    // and the deduped re-observation that follows keeps it that way
    indexer.observe("p", "ns", "tbl", List.of("msg"), 5);
    assertEquals(1, wired.indexTablesTracked.getAsLong());
    assertEquals(0, wired.indexTablesBehind.getAsLong());
    assertEquals(0, wired.indexMaxBehindSeconds.getAsLong());
  }

  @Test
  void thePerTableGaugeListsOnlyWhatIsBehindByDecodedName() {
    String key = "p|logs%1Fweb|events%20raw";
    freshness.observed(key, 5);
    freshness.built(key, 5, 5);
    assertEquals(java.util.Map.of(), metrics.indexBehindByTable.get(), "current: no line at all");

    clock.addAndGet(2_500);
    freshness.observed(key, 9);
    assertEquals(java.util.Map.of("logs.web.events raw", 1L), metrics.indexBehindByTable.get(),
        "just behind: at least 1, keyed namespace.table, prefix dropped, segments decoded");
    clock.addAndGet(60_000);
    assertEquals(60L, metrics.indexBehindByTable.get().get("logs.web.events raw"));
    assertTrue(
        metrics.scrape().contains("kahshe_index_behind_seconds{table=\"logs.web.events raw\"} 60"),
        metrics.scrape());

    freshness.built(key, 9, 9);
    assertEquals(java.util.Map.of(), metrics.indexBehindByTable.get(), "caught up: gone again");
  }

  @Test
  void aProcessThatDoesNotBuildListsNoTableBehind() {
    IndexFreshness observer = new IndexFreshness(metrics, WARN_MS, false);
    observer.observed(KEY, 5);
    assertEquals(java.util.Map.of(), metrics.indexBehindByTable.get());
    assertEquals("ns.tbl", IndexFreshness.tableLabel(KEY));
    assertEquals("odd", IndexFreshness.tableLabel("odd"), "a key of the wrong shape stays as it is");
  }
}
