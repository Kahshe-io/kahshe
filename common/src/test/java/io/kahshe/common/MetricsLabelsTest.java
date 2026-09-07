package io.kahshe.common;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertTrue;

import java.util.Map;
import org.junit.jupiter.api.Test;

/**
 * The per-table families: a label set formatted the way the exposition format reads it, a
 * counter that carries three labels, build time emitted in seconds without losing the
 * milliseconds, a gauge family that is absent until something is behind — and the two tier
 * series, which existed before any of this, unchanged to the byte.
 */
class MetricsLabelsTest {

  @Test
  void aLabelSetIsWrittenInOrderAndEscaped() {
    assertEquals("table=\"ns.t\",column=\"msg\",kind=\"full\"",
        Metrics.labels("table", "ns.t", "column", "msg", "kind", "full"));
    assertEquals("table=\"a\\\"b\\\\c\\nd\"", Metrics.labels("table", "a\"b\\c\nd"),
        "a quote, a backslash and a newline in a value would each break the line otherwise");
  }

  @Test
  void aPublishCountsUnderItsThreeLabelsAndItsSecondsKeepTheirMilliseconds() {
    Metrics metrics = new Metrics();
    metrics.indexPublished("ns.t", "msg", "full", 1_234);
    metrics.indexPublished("ns.t", "msg", "incremental", 400);
    metrics.indexPublished("ns.t", "msg", "incremental", 400);
    String scrape = metrics.scrape();
    assertTrue(scrape.contains("# TYPE kahshe_table_index_builds_total counter\n"), scrape);
    assertTrue(scrape.contains(
        "kahshe_table_index_builds_total{table=\"ns.t\",column=\"msg\",kind=\"full\"} 1\n"), scrape);
    assertTrue(scrape.contains(
        "kahshe_table_index_builds_total{table=\"ns.t\",column=\"msg\",kind=\"incremental\"} 2\n"),
        scrape);
    assertTrue(scrape.contains(
        "kahshe_table_index_build_seconds_total{table=\"ns.t\",column=\"msg\"} 2.034\n"),
        "two 0.4 s builds must add 0.8 s, not 0: " + scrape);
  }

  @Test
  void aServedPlanCountsAgainstItsTable() {
    Metrics metrics = new Metrics();
    metrics.planServed("ns.t", 3);
    metrics.planServed("ns.t", 5);
    metrics.planServed("ns.u", 0);
    String scrape = metrics.scrape();
    assertTrue(scrape.contains("kahshe_table_plan_requests_total{table=\"ns.t\"} 2\n"), scrape);
    assertTrue(scrape.contains("kahshe_table_plan_files_kept_total{table=\"ns.t\"} 8\n"), scrape);
    assertTrue(scrape.contains("kahshe_table_plan_requests_total{table=\"ns.u\"} 1\n"), scrape);
  }

  @Test
  void theBehindGaugeIsAbsentUntilInstalledAndReadsTheSupplierAtScrape() {
    Metrics metrics = new Metrics();
    assertFalse(metrics.scrape().contains("kahshe_index_behind_seconds"),
        "no series and no TYPE line for a process tracking nothing");
    Map<String, Long> behind = new java.util.concurrent.ConcurrentHashMap<>();
    metrics.indexBehindByTable = () -> behind;
    assertFalse(metrics.scrape().contains("kahshe_index_behind_seconds"), "installed but empty");
    behind.put("ns.t", 90L);
    behind.put("ns.a", 5L);
    String scrape = metrics.scrape();
    assertTrue(scrape.contains("# TYPE kahshe_index_behind_seconds gauge\n"), scrape);
    assertTrue(scrape.contains("kahshe_index_behind_seconds{table=\"ns.a\"} 5\n"
        + "kahshe_index_behind_seconds{table=\"ns.t\"} 90\n"), "sorted, one line per table: " + scrape);
  }

  @Test
  void theTierSeriesAreUnchangedToTheByte() {
    Metrics metrics = new Metrics();
    metrics.prunePass("term", 10, 4);
    metrics.prunePass("bloom", 4, 1);
    String scrape = metrics.scrape();
    assertTrue(scrape.contains("# TYPE kahshe_prune_files_in_total counter\n"
        + "kahshe_prune_files_in_total{tier=\"bloom\"} 4\n"
        + "kahshe_prune_files_in_total{tier=\"term\"} 10\n"
        + "# TYPE kahshe_prune_files_kept_total counter\n"
        + "kahshe_prune_files_kept_total{tier=\"bloom\"} 1\n"
        + "kahshe_prune_files_kept_total{tier=\"term\"} 4\n"), scrape);
  }
}
