package io.kahshe;

import io.kahshe.indexer.TableSource;
import io.kahshe.watch.WatchEngine;
import org.apache.iceberg.Snapshot;
import org.apache.iceberg.Table;
import org.apache.iceberg.catalog.TableIdentifier;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Whether a snapshot carries delete files, answered from the catalog.
 *
 * <p>This lives in {@code app} because {@code watch} names no table format — the engine takes the
 * verdict as a seam ({@link WatchEngine.Deletes}) rather than reaching a snapshot itself, and
 * {@code PortBoundaryTest} is what keeps that true. Without this wired in, the engine answers
 * {@link WatchEngine.Deletes#UNPROVEN} and every alert is advisory, which is safe but carries no
 * information: a copy-on-write table that never had a delete file would read the same as one that
 * is half tombstones.
 *
 * <p><b>Fails closed.</b> Anything that stops it proving the snapshot is delete-free — the table
 * will not load, the snapshot is gone, the summary has no count — answers "bearing". The claim
 * being made is exactness, and an unproven claim of exactness is the error a watcher may not make.
 */
final class SnapshotDeletes implements WatchEngine.Deletes {
  private static final Logger LOG = LoggerFactory.getLogger(SnapshotDeletes.class);

  private final TableSource tables;

  SnapshotDeletes(TableSource tables) {
    this.tables = tables;
  }

  @Override
  public boolean bearing(String prefix, String namespace, String tableName, long snapshotId) {
    try {
      Table table = tables.load(prefix, TableIdentifier.parse(namespace + "." + tableName));
      Snapshot snapshot = table.snapshot(snapshotId);
      if (snapshot == null || snapshot.summary() == null) {
        return true;
      }
      // The same property and the same reading as the row scan's (ScanPass.deleteBearing): only an
      // explicit zero proves the absence, so a summary that omits the count is bearing.
      return !"0".equals(snapshot.summary().get("total-delete-files"));
    } catch (RuntimeException e) {
      LOG.debug("could not resolve deletes for {}.{} @ {}; treating as delete-bearing",
          namespace, tableName, snapshotId, e);
      return true;
    }
  }
}
