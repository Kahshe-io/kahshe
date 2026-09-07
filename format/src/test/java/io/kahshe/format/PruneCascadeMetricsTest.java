package io.kahshe.format;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertTrue;

import io.kahshe.common.Metrics;
import java.util.concurrent.atomic.LongAdder;
import org.junit.jupiter.api.Test;

/**
 * Per-tier pruning counters: which tier pruned, and whether a second predicate narrowed anything
 * at all, is otherwise unanswerable from a running process — a plan's files-in and files-kept,
 * recorded once at the end, cannot say.
 *
 * <p>Labelled by the type's own key rather than a field per tier, because index types are a
 * ServiceLoader seam: a deployment can carry one this repository has never seen, and it should
 * appear in the scrape the day it runs.
 */
class PruneCascadeMetricsTest {

  @Test
  void aTiersTurnIsRecordedAsWhatItWasHandedAndWhatItKept() {
    Metrics metrics = new Metrics();
    metrics.prunePass("term", 1000, 4);
    metrics.prunePass("ngram-bloom", 1000, 900);
    metrics.prunePass("term", 500, 500);

    assertEquals(1500, metrics.pruneFilesIn.get("term").sum());
    assertEquals(504, metrics.pruneFilesKept.get("term").sum(),
        "a tier that kept everything on its second turn still contributes its keeps");
    assertEquals(900, metrics.pruneFilesKept.get("ngram-bloom").sum(),
        "the tiers are told apart, which is the whole point");
  }

  @Test
  void theScrapeCarriesOneSeriesPerTierAndIsStableToDiff() {
    Metrics metrics = new Metrics();
    metrics.prunePass("term", 1000, 4);
    metrics.prunePass("ngram-bloom", 1000, 900);
    String scrape = metrics.scrape();

    assertTrue(scrape.contains("# TYPE kahshe_prune_files_in_total counter"), scrape);
    assertTrue(scrape.contains("kahshe_prune_files_kept_total{tier=\"term\"} 4"), scrape);
    assertTrue(scrape.contains("kahshe_prune_files_kept_total{tier=\"ngram-bloom\"} 900"), scrape);
    // Sorted, so two scrapes of the same state diff to nothing.
    assertTrue(scrape.indexOf("tier=\"ngram-bloom\"") < scrape.indexOf("tier=\"term\""), scrape);
  }

  /**
   * A metric family with no series must emit nothing at all — not a bare TYPE line, which some
   * Prometheus parsers reject and which would appear in every scrape of a proxy-only process.
   */
  @Test
  void aProcessThatHasPrunedNothingEmitsNoSeriesAndNoTypeLine() {
    String scrape = new Metrics().scrape();
    assertFalse(scrape.contains("kahshe_prune_files_in_total"), scrape);
    assertFalse(scrape.contains("kahshe_prune_files_kept_total"), scrape);
  }

  /**
   * Every family populated, the multi-label and fractional ones included, so the shape check
   * covers the whole exposition and not only the tier counters: each series line names a family
   * whose TYPE line came just before it (no family split, none missing), with labels in the
   * escaped form and a value a parser accepts.
   */
  @Test
  void everyLineOfTheScrapeIsWellFormed() {
    Metrics metrics = new Metrics();
    metrics.prunePass("term", 10, 1);
    metrics.indexPublished("ns.t", "msg", "full", 1_234);
    metrics.planServed("ns.t", 3);
    metrics.indexBehindByTable = () -> java.util.Map.of("ns.t", 5L, "a\"b\\c\nd", 1L);
    String family = null;
    for (String line : metrics.scrape().split("\n")) {
      if (line.isBlank()) {
        continue;
      }
      if (line.startsWith("# TYPE ")) {
        family = line.split(" ")[2];
        continue;
      }
      assertTrue(line.matches(
          "^[a-z_]+(\\{[a-z_]+=\"(?:[^\"\\\\]|\\\\.)*\"(,[a-z_]+=\"(?:[^\"\\\\]|\\\\.)*\")*\\})? -?\\d+(\\.\\d+)?$"),
          "a scrape line a parser would reject: " + line);
      assertEquals(family, line.replaceAll("[{ ].*$", ""),
          "a series under another family's TYPE line: " + line);
    }
  }

  /**
   * The end-to-end one: a real build, a real probe, and the counters that come out of the cascade
   * rather than out of a hand call. Without this the other tests only prove the counter works.
   */
  @Test
  void arealPruneRecordsEachTiersTurnAndTheyChainNoseToTail(
      @org.junit.jupiter.api.io.TempDir java.nio.file.Path dir) throws Exception {
    io.kahshe.indexer.BuildConfig config = io.kahshe.indexer.LocalTableFixture.config();
    org.apache.iceberg.Table table = io.kahshe.indexer.LocalTableFixture.createTable(dir,
        "connection refused from gateway");
    io.kahshe.indexer.LocalTableFixture.appendFile(table, "b.parquet", "request completed");
    io.kahshe.indexer.LocalTableFixture.appendFile(table, "c.parquet", "quorum lost");
    table.refresh();
    io.kahshe.indexer.build.IndexBuilder.buildColumn(
        table, io.kahshe.indexer.LocalTableFixture.COLUMN, config);

    Metrics metrics = new Metrics();
    IndexPruner pruner = new IndexPruner(
        new io.kahshe.format.type.term.TermIndex(config.format(), metrics), metrics,
        config.format());
    int fieldId = table.schema().findField(io.kahshe.indexer.LocalTableFixture.COLUMN).fieldId();
    int kept = pruner.prune(table, org.apache.iceberg.expressions.Expressions.alwaysTrue(),
        java.util.List.of(new IndexPruner.ContainsHint(
            io.kahshe.indexer.LocalTableFixture.COLUMN, fieldId, "refused",
            IndexPruner.HintKind.MATCH)),
        io.kahshe.indexer.LocalTableFixture.planTasks(table)).size();

    assertEquals(1, kept, "the fixture must actually prune, or this measures nothing");
    assertFalse(metrics.pruneFilesIn.isEmpty(), "no tier recorded a turn");

    // Every registered tier took a turn, and the tiers chain: what one kept is what the next was
    // handed. That ordering property is the thing a per-plan counter cannot express.
    int expected = 3;
    for (io.kahshe.format.type.IndexType type : io.kahshe.format.type.IndexTypes.inCostOrder()) {
      LongAdder in = metrics.pruneFilesIn.get(type.key());
      assertEquals((long) expected, in == null ? -1L : in.sum(),
          type.key() + " was handed what the tier before it kept");
      expected = (int) metrics.pruneFilesKept.get(type.key()).sum();
    }
    assertEquals((long) kept, (long) expected, "the last tier's keeps are the plan's answer");
  }

  @Test
  void countingIsConcurrentSafeBecauseAPlanIsServedOnManyThreads() throws Exception {
    Metrics metrics = new Metrics();
    int threads = 8;
    int each = 2000;
    Thread[] workers = new Thread[threads];
    for (int i = 0; i < threads; i++) {
      workers[i] = new Thread(() -> {
        for (int n = 0; n < each; n++) {
          metrics.prunePass("term", 2, 1);
        }
      });
      workers[i].start();
    }
    for (Thread worker : workers) {
      worker.join();
    }
    LongAdder in = metrics.pruneFilesIn.get("term");
    assertEquals((long) threads * each * 2, in.sum(), "a lost update here understates pruning");
    assertEquals((long) threads * each, metrics.pruneFilesKept.get("term").sum());
  }
}
