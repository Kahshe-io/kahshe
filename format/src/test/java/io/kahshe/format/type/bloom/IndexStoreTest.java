package io.kahshe.format.type.bloom;

import io.kahshe.indexer.BuildConfig;
import io.kahshe.indexer.build.IndexBuilder;
import io.kahshe.indexer.LocalTableFixture;
import io.kahshe.format.type.bloom.IndexMeta;
import io.kahshe.format.IndexPaths;
import io.kahshe.format.type.bloom.IndexStore;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertNotSame;
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.junit.jupiter.api.Assertions.assertSame;
import static org.junit.jupiter.api.Assertions.assertTrue;

import io.kahshe.common.Metrics;
import java.nio.file.Path;
import java.util.concurrent.atomic.AtomicLong;
import org.apache.iceberg.Table;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

class IndexStoreTest {
  private static final long PAST_TTL_MS = 31_000; // IndexStore.TTL_MS is 30s

  @TempDir Path tmp;

  @Test
  void loadRevalidateAndRecoverAcrossTtl() throws Exception {
    Table table = LocalTableFixture.createTable(tmp, "hello world", "goodbye moon");
    BuildConfig config = LocalTableFixture.config();
    assertNotNull(IndexBuilder.buildColumn(table, LocalTableFixture.COLUMN, config));

    IndexStore store = new IndexStore(config.format(), new Metrics());
    AtomicLong clock = new AtomicLong(1_000_000);
    store.nowMs = clock::get;

    IndexStore.LoadedIndex first = store.forColumn(table, LocalTableFixture.COLUMN);
    assertNotNull(first);
    assertTrue(first.weightBytes() > 0);

    // within TTL: served from cache with no metadata re-read (same-instance short-circuit)
    clock.addAndGet(1_000);
    assertSame(first, store.forColumn(table, LocalTableFixture.COLUMN));

    // past TTL, artifact unchanged: revalidation renews the entry but retains the payload
    clock.addAndGet(PAST_TTL_MS);
    assertSame(first, store.forColumn(table, LocalTableFixture.COLUMN));

    // artifact deleted: the next expiry observes absence
    String root = IndexPaths.root(table, config.format().indexRoot());
    table.io().deleteFile(IndexMeta.metaPath(root, table.schema().findField(LocalTableFixture.COLUMN).fieldId()));
    clock.addAndGet(PAST_TTL_MS);
    assertNull(store.forColumn(table, LocalTableFixture.COLUMN));

    // rebuild + expiry: a fresh index loads again — the absent entry does not stick
    assertNotNull(IndexBuilder.buildColumn(table, LocalTableFixture.COLUMN, config));
    clock.addAndGet(PAST_TTL_MS);
    IndexStore.LoadedIndex rebuilt = store.forColumn(table, LocalTableFixture.COLUMN);
    assertNotNull(rebuilt);
    assertNotSame(first, rebuilt);
    assertTrue(rebuilt.weightBytes() > 0);
  }
}
