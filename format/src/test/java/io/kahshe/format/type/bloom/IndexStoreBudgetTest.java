package io.kahshe.format.type.bloom;

import io.kahshe.indexer.BuildConfig;
import io.kahshe.indexer.build.IndexBuilder;
import io.kahshe.indexer.LocalTableFixture;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertNull;
import io.kahshe.common.Metrics;
import java.nio.file.Path;
import org.apache.iceberg.Table;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;
import io.kahshe.format.type.gram.GramIndex;

/** The bloom reader's budget guard, beside the reader it pins. */
class IndexStoreBudgetTest {
  @TempDir Path tmp;

  /**
   * An over-budget bloom tier is refused before it is loaded, not clamp-retained.
   *
   * <p>{@code WeighedCache} cannot evict the only entry it holds, so an artifact heavier than the
   * whole budget is loaded in full and then pinned — the budget bounding everything except the one
   * thing that broke it. {@code GramIndex} refuses for this reason, and {@code IndexStore} has to
   * as well.
   *
   * <p>Verified by breaking it: removing the refusal loads the tier and the assertion below fails.
   */
  @Test
  void anOverBudgetBloomTierIsRefusedRatherThanPinned() throws Exception {
    Table table = LocalTableFixture.createTable(tmp, "alpha alpha", "bravo bravo");
    BuildConfig build = LocalTableFixture.config();
    assertNotNull(IndexBuilder.buildColumn(table, LocalTableFixture.COLUMN, build));

    // The budget is shrunk on the store, not through Config: indexCacheBytes has an 8 MiB floor,
    // so no configuration can express a budget small enough to trip this on a fixture-sized tier.
    Metrics metrics = new Metrics();
    IndexStore refusing = new IndexStore(build.format(), metrics);
    refusing.maxLoadBytes = 1;
    assertNull(
        refusing.forColumn(table, LocalTableFixture.COLUMN),
        "a bloom tier heavier than the whole cache budget was loaded anyway; WeighedCache cannot "
            + "evict a lone oversized entry, so it is pinned for the life of the process");
    assertEquals(1, metrics.indexTooLarge.sum(), "the refusal was not counted");

    // and a sane budget still loads it, so the guard is not simply refusing everything
    assertNotNull(new IndexStore(build.format(), new Metrics()).forColumn(table, LocalTableFixture.COLUMN));
  }
}
