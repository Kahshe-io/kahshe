package io.kahshe.proxy.catalog;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.junit.jupiter.api.Assertions.assertTrue;

import io.kahshe.indexer.LocalTableFixture;
import java.nio.file.Path;
import org.apache.iceberg.Table;
import org.apache.iceberg.catalog.Namespace;
import org.apache.iceberg.catalog.TableIdentifier;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;
import io.kahshe.proxy.TestConfigs;

/**
 * kahshe must never plan a view older than the one it forwarded to the client.
 *
 * <p>A commit that reaches the catalog without passing through kahshe invalidates nothing, so the
 * only thing that eventually notices is the table cache's TTL. Inside that window a plan is built
 * from the pre-commit snapshot and the rows just written are missing from the result — silently,
 * because the plan response names no snapshot and nothing downstream can tell. Clients do not pin
 * {@code snapshot-id} on ordinary scans, so kahshe is the only party in a position
 * to notice.
 *
 * <p>It is in a position to notice because it is a proxy: the client learned this table through a
 * loadTable kahshe relayed straight to the backend, uncached, and that response named a snapshot.
 * Comparing the planning view against what was forwarded needs no timer, no polling and no
 * announcement from the catalog.
 */
class ObservedSnapshotTest {
  @TempDir Path tmp;

  private static final String PREFIX = "main";
  private static final TableIdentifier IDENT =
      TableIdentifier.of(Namespace.of("logs"), "events");

  /**
   * The decision, against two genuinely different Iceberg snapshots.
   *
   * <p>Membership rather than comparison, because snapshot ids are random longs and "older" cannot
   * be read off the number. The stale view here is a real one: the {@code Table} object is never
   * refreshed after the second commit, which is exactly the state a cached catalog entry is in.
   *
   * <p>Verified by breaking it: making {@code predatesObserved} return false unconditionally makes
   * the middle assertion fail.
   */
  @Test
  void aViewIsBehindOnlyWhenItDoesNotContainTheForwardedSnapshot() throws Exception {
    Table table = LocalTableFixture.createTable(tmp, "alpha");
    long first = table.currentSnapshot().snapshotId();

    // A genuinely INDEPENDENT handle, loaded before the next commit and never refreshed -- which
    // is what a cached catalog entry is. Aliasing `table` here instead would compare a table
    // against itself and pass the "behind" assertion for the wrong reason.
    Table staleView =
        new org.apache.iceberg.hadoop.HadoopTables(
                new org.apache.hadoop.conf.Configuration())
            .load(table.location());
    LocalTableFixture.appendFile(table, "f2.parquet", "bravo");
    table.refresh();
    long second = table.currentSnapshot().snapshotId();

    assertFalse(
        BackendCatalogs.predatesObserved(staleView, first),
        "a view holding exactly the forwarded snapshot is not behind it");
    assertTrue(
        BackendCatalogs.predatesObserved(staleView, second),
        "a view that has never seen the forwarded snapshot IS behind it, and planning from it "
            + "would omit the files committed since");
    assertFalse(
        BackendCatalogs.predatesObserved(table, first),
        "a view AHEAD of the forwarded snapshot is fine: being ahead cannot lose rows, and the "
            + "forwarded snapshot is still in its history");
    assertFalse(
        BackendCatalogs.predatesObserved(table, null),
        "a table this replica has never forwarded has no observation to be behind");
  }

  /** Recording is per (prefix, table) and survives nothing else being known. */
  @Test
  void anObservationIsRecordedAgainstTheTableItNames() {
    BackendCatalogs catalogs = new BackendCatalogs(TestConfigs.proxyConfig());
    assertNull(catalogs.observedSnapshot(PREFIX, IDENT), "nothing forwarded yet");

    catalogs.noteObserved(PREFIX, IDENT, 4242L);
    assertEquals(4242L, catalogs.observedSnapshot(PREFIX, IDENT));
    assertNull(
        catalogs.observedSnapshot("other", IDENT), "an observation must not leak across prefixes");
    assertNull(
        catalogs.observedSnapshot(PREFIX, TableIdentifier.of(Namespace.of("logs"), "other")),
        "or across tables");

    // A response that named no snapshot records nothing rather than recording -1, which would then
    // read as a snapshot no view can contain and force a reload on every plan.
    catalogs.noteObserved(PREFIX, TableIdentifier.of(Namespace.of("logs"), "empty"), -1L);
    assertNull(catalogs.observedSnapshot(PREFIX, TableIdentifier.of(Namespace.of("logs"), "empty")));
  }
}
