package io.kahshe.watch.rules;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.eq;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.never;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

import io.kahshe.common.Metrics;
import io.kahshe.indexer.LocalTableFixture;
import io.kahshe.indexer.maintain.IndexerService;
import io.kahshe.indexer.TableSource;
import java.net.URLDecoder;
import java.nio.charset.StandardCharsets;
import java.util.List;
import java.util.Map;
import org.apache.iceberg.Schema;
import org.apache.iceberg.Snapshot;
import org.apache.iceberg.Table;
import org.apache.iceberg.catalog.Namespace;
import org.apache.iceberg.catalog.TableIdentifier;
import org.apache.iceberg.types.Types;
import org.junit.jupiter.api.Test;
import org.mockito.ArgumentCaptor;
import io.kahshe.watch.TableDiscovery;
import io.kahshe.watch.WatchConfig;
import io.kahshe.watch.scan.ScanPass;

class TableDiscoveryTest {
  @Test
  void encodeRoundTripsThroughIndexerDecode() {
    // multi-level namespace with a dotted level: the case plain string splitting would corrupt
    TableIdentifier ident = TableIdentifier.of(Namespace.of("a.b", "prod logs"), "events.v2");
    String[] raw = TableDiscovery.encodeForObserve("my prefix", ident);
    assertEquals("my prefix", URLDecoder.decode(raw[0], StandardCharsets.UTF_8));
    assertEquals(ident, IndexerService.decodeIdent(raw[1], raw[2]));
  }

  @Test
  void encodeRoundTripsSimpleNames() {
    TableIdentifier ident = TableIdentifier.parse("logs.events");
    String[] raw = TableDiscovery.encodeForObserve("lakehouse", ident);
    assertEquals(ident, IndexerService.decodeIdent(raw[1], raw[2]));
  }

  private static WatchRule rule(String id, String prefix, String table, String column) {
    return WatchRule.singleColumn(id, id, WatchRule.Severity.LOW, prefix, table, column,
        List.of("error"), List.of(), WatchRule.Condition.ANY_OF, 1);
  }

  private static WatchRules fixedRules(WatchRule... fixed) {
    WatchRules rules = mock(WatchRules.class);
    when(rules.current()).thenReturn(List.of(fixed));
    return rules;
  }

  /** A table whose schema holds {@code msg}: a rule naming it is covered. */
  private static Table tableWith(Map<String, String> properties, long snapshotId) {
    return tableWith(properties, snapshotId,
        new Schema(Types.NestedField.optional(1, "msg", Types.StringType.get())));
  }

  private static Table tableWith(Map<String, String> properties, long snapshotId, Schema schema) {
    Table table = mock(Table.class);
    when(table.properties()).thenReturn(properties);
    when(table.schema()).thenReturn(schema);
    Snapshot snapshot = mock(Snapshot.class);
    when(snapshot.snapshotId()).thenReturn(snapshotId);
    when(table.currentSnapshot()).thenReturn(snapshot);
    return table;
  }

  private static WatchConfig config() {
    return new WatchConfig("", "webhook", "", "", 1_000, 60_000, "iceberg", false, true, 2);
  }

  @Test
  void pollOnceLoadsEachRuleNamedTableOnceAndObservesEncoded() {
    WatchRules rules = fixedRules(
        rule("r1", "my prefix", "prod logs.events", "msg"),
        rule("r2", "my prefix", "prod logs.events", "msg"));
    Table table = tableWith(Map.of("kahshe.index", "msg"), 42L);
    TableIdentifier ident = TableIdentifier.parse("prod logs.events");
    TableSource catalogs = mock(TableSource.class);
    when(catalogs.load("my prefix", ident)).thenReturn(table);
    IndexerService indexer = mock(IndexerService.class);
    Metrics metrics = new Metrics();

    new TableDiscovery(rules, catalogs, indexer, config(), metrics).pollOnce();

    verify(catalogs, times(1)).load(eq("my prefix"), any(TableIdentifier.class));
    ArgumentCaptor<String> prefixRaw = ArgumentCaptor.forClass(String.class);
    ArgumentCaptor<String> namespaceRaw = ArgumentCaptor.forClass(String.class);
    ArgumentCaptor<String> tableRaw = ArgumentCaptor.forClass(String.class);
    verify(indexer, times(1)).observe(prefixRaw.capture(), namespaceRaw.capture(),
        tableRaw.capture(), eq(List.of("msg")), eq(42L));
    // observe takes RAW url-encoded segments; the worker's decode must recover the identity
    assertEquals("my prefix", URLDecoder.decode(prefixRaw.getValue(), StandardCharsets.UTF_8));
    assertEquals(ident, IndexerService.decodeIdent(namespaceRaw.getValue(), tableRaw.getValue()));
    assertEquals(0, metrics.watchRulesUncovered.getAsLong());
  }

  @Test
  void pollOnceColumnMissingFromTheSchemaBumpsGaugeWithoutObserving() {
    WatchRules rules = fixedRules(rule("r1", "lakehouse", "logs.events", "msg"));
    // a schema with no msg column: no evaluator anywhere can answer this rule
    Table table = tableWith(Map.of(), 42L,
        new Schema(Types.NestedField.optional(1, "other", Types.StringType.get())));
    TableSource catalogs = mock(TableSource.class);
    when(catalogs.load("lakehouse", TableIdentifier.parse("logs.events"))).thenReturn(table);
    IndexerService indexer = mock(IndexerService.class);
    Metrics metrics = new Metrics();

    new TableDiscovery(rules, catalogs, indexer, config(), metrics).pollOnce();

    verify(indexer, never()).observe(any(), any(), any(), any(), org.mockito.ArgumentMatchers.anyLong());
    assertEquals(1, metrics.watchRulesUncovered.getAsLong());
  }

  /**
   * Index coverage is not a condition for firing: the scan reads the column whether or not
   * kahshe.index names it, so the gauge must stay at zero and every polled table must reach the
   * scan — including one the indexer will not observe.
   */
  @Test
  void pollOnceScansATableWithNoIndexedColumnsAndCountsItCovered() {
    WatchRules rules = fixedRules(rule("r1", "lakehouse", "logs.events", "msg"));
    Table table = tableWith(Map.of(), 42L); // schema has msg; kahshe.index names nothing
    TableIdentifier ident = TableIdentifier.parse("logs.events");
    TableSource catalogs = mock(TableSource.class);
    when(catalogs.load("lakehouse", ident)).thenReturn(table);
    IndexerService indexer = mock(IndexerService.class);
    ScanPass scan = mock(ScanPass.class);
    Metrics metrics = new Metrics();

    new TableDiscovery(rules, catalogs, indexer, config(), metrics, scan).pollOnce();

    verify(indexer, never()).observe(any(), any(), any(), any(), org.mockito.ArgumentMatchers.anyLong());
    verify(scan, times(1)).scan("lakehouse", ident, table);
    assertEquals(0, metrics.watchRulesUncovered.getAsLong());
  }

  /**
   * A watch instance with the indexer OFF still scans. A real, disabled {@link IndexerService}
   * takes the observation of a table that declares {@code kahshe.index} and enqueues nothing;
   * the table still reaches the scan.
   */
  @Test
  void pollOnceWithTheIndexerOffStillScansAndBuildsNothing() {
    WatchRules rules = fixedRules(rule("r1", "lakehouse", "logs.events", "msg"));
    Table table = tableWith(Map.of("kahshe.index", "msg"), 42L); // would be built, were it on
    TableIdentifier ident = TableIdentifier.parse("logs.events");
    TableSource catalogs = mock(TableSource.class);
    when(catalogs.load("lakehouse", ident)).thenReturn(table);
    Metrics metrics = new Metrics();
    IndexerService indexerOff = new IndexerService(catalogs, metrics, false, LocalTableFixture.config());
    ScanPass scan = mock(ScanPass.class);

    new TableDiscovery(rules, catalogs, indexerOff, config(), metrics, scan).pollOnce();

    verify(scan, times(1)).scan("lakehouse", ident, table);
    verify(catalogs, times(1)).load(eq("lakehouse"), any(TableIdentifier.class));
    assertEquals(0, metrics.indexerJobsDropped.sum(), "nothing was queued, so nothing was dropped");
    assertEquals(0, metrics.indexBuilds.sum());
    assertEquals(0, metrics.watchRulesUncovered.getAsLong());
  }

  @Test
  void pollOnceSurvivesLoadTableFailureAndContinues() {
    WatchRules rules = fixedRules(
        rule("r1", "lakehouse", "broken.table", "msg"),
        rule("r2", "lakehouse", "logs.events", "msg"));
    TableSource catalogs = mock(TableSource.class);
    when(catalogs.load("lakehouse", TableIdentifier.parse("broken.table")))
        .thenThrow(new RuntimeException("backend down"));
    Table table = tableWith(Map.of("kahshe.index", "msg"), 7L);
    when(catalogs.load("lakehouse", TableIdentifier.parse("logs.events"))).thenReturn(table);
    IndexerService indexer = mock(IndexerService.class);

    new TableDiscovery(rules, catalogs, indexer, config(), new Metrics()).pollOnce();

    String[] raw = TableDiscovery.encodeForObserve("lakehouse", TableIdentifier.parse("logs.events"));
    verify(indexer, times(1)).observe(eq(raw[0]), eq(raw[1]), eq(raw[2]), eq(List.of("msg")), eq(7L));
  }

}
