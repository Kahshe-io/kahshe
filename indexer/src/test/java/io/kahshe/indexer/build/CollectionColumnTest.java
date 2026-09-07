package io.kahshe.indexer.build;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

import io.kahshe.indexer.BuildConfig;
import io.kahshe.indexer.LocalTableFixture;
import java.nio.file.Path;
import java.util.List;
import java.util.Map;
import java.util.stream.Collectors;
import org.apache.iceberg.Table;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

/**
 * List and map columns: many values per row, indexed under the container's own field id.
 *
 * <p>What a set bit asserts, and nothing more: some row of this data file has some element whose
 * analysis emitted this term. Not which row, not which element, not how many. That is the only
 * thing a bitmap over FILE ordinals can prove about a repeated field, and it is why the pruner
 * refuses a leaf UNDER a container ({@code RepeatedPathTest}) while the container itself is
 * indexed here.
 *
 * <p>The test that matters most is {@link #everyElementIsWalkedIncludingTheLast}. A walk that
 * stopped early — at the first element, at a cap, or on a reader buffer it retained across rows —
 * would publish an index that COVERS the file while missing its terms, and a covered file with a
 * missing term is PRUNED. Advisory-keep does not save it: that invariant is about files the index
 * does not cover, and this file is covered. The failure would be a silently short answer.
 */
class CollectionColumnTest {

  @TempDir Path dir;

  /**
   * The totality requirement, stated as a test. The needle sits in the LAST element of the LAST
   * row of its file, so any walk that stops early prunes the file that holds the match.
   */
  @Test
  void everyElementIsWalkedIncludingTheLast() throws Exception {
    BuildConfig config = LocalTableFixture.config();
    Table table = LocalTableFixture.createCollectionTable(dir);
    LocalTableFixture.appendCollectionRows(table, "a.parquet",
        new Object[] {"one", List.of("alpha", "bravo"), null},
        new Object[] {"two", List.of("charlie", "delta", "zulunique"), null});
    LocalTableFixture.appendCollectionRows(table, "b.parquet",
        new Object[] {"three", List.of("echo", "foxtrot"), null});
    table.refresh();

    assertNotNull(IndexBuilder.buildColumn(table, LocalTableFixture.LIST_COLUMN, config),
        "a list column must be indexable at all");

    assertEquals(List.of("a.parquet"), names(prune(table, config, LocalTableFixture.LIST_COLUMN,
        "zulunique")), "the last element of the last row must be in the index");
    assertEquals(List.of("a.parquet"), names(prune(table, config, LocalTableFixture.LIST_COLUMN,
        "alpha")), "and so must the first");
    assertEquals(List.of("b.parquet"), names(prune(table, config, LocalTableFixture.LIST_COLUMN,
        "foxtrot")));
    assertEquals(List.of(), names(prune(table, config, LocalTableFixture.LIST_COLUMN, "absentzz")),
        "a token in no element prunes every file");
  }

  /**
   * Elements are cut on their own. Joining them into one string before analysis would invent terms
   * and grams that span a boundary — matching text that appears in no single value.
   */
  @Test
  void noTokenSpansAnElementBoundary() throws Exception {
    BuildConfig config = LocalTableFixture.config();
    Table table = LocalTableFixture.createCollectionTable(dir);
    LocalTableFixture.appendCollectionRows(table, "a.parquet",
        new Object[] {"one", List.of("connection", "refused"), null});
    table.refresh();
    assertNotNull(IndexBuilder.buildColumn(table, LocalTableFixture.LIST_COLUMN, config));

    assertEquals(1, prune(table, config, LocalTableFixture.LIST_COLUMN, "connection").size());
    assertEquals(1, prune(table, config, LocalTableFixture.LIST_COLUMN, "refused").size());
    assertEquals(0, prune(table, config, LocalTableFixture.LIST_COLUMN, "connectionrefused").size(),
        "the two elements must not have been concatenated into one analyzed value");
  }

  /**
   * A map is indexed by its VALUES. Keys are deliberately not indexed and not qualified onto
   * values: a key set is small and repeats on nearly every row, so its bitmaps would be all-ones —
   * full dictionary cost, zero pruning. The consequence is that a key-qualified predicate is
   * answered at the selectivity of "this value appears under some key", an over-approximation,
   * which is the direction the invariant permits.
   */
  @Test
  void aMapIsIndexedByItsValuesAndNotByItsKeys() throws Exception {
    BuildConfig config = LocalTableFixture.config();
    Table table = LocalTableFixture.createCollectionTable(dir);
    LocalTableFixture.appendCollectionRows(table, "a.parquet",
        new Object[] {"one", null, Map.of("env", "production")});
    LocalTableFixture.appendCollectionRows(table, "b.parquet",
        new Object[] {"two", null, Map.of("region", "staging")});
    table.refresh();
    assertNotNull(IndexBuilder.buildColumn(table, LocalTableFixture.MAP_COLUMN, config));

    assertEquals(List.of("a.parquet"),
        names(prune(table, config, LocalTableFixture.MAP_COLUMN, "production")),
        "a value prunes to the file holding it");
    assertEquals(List.of("b.parquet"),
        names(prune(table, config, LocalTableFixture.MAP_COLUMN, "staging")));
    assertEquals(List.of(), names(prune(table, config, LocalTableFixture.MAP_COLUMN, "env")),
        "a KEY is not a term: probing one finds nothing and prunes everything, which is why a "
            + "key-qualified predicate must not be routed here as though it were a value");
  }

  /** A null container and an empty one contribute no terms, exactly as a null scalar does. */
  @Test
  void aNullOrEmptyContainerContributesNothingAndDoesNotFailTheBuild() throws Exception {
    BuildConfig config = LocalTableFixture.config();
    Table table = LocalTableFixture.createCollectionTable(dir);
    LocalTableFixture.appendCollectionRows(table, "a.parquet",
        new Object[] {"one", null, null},
        new Object[] {"two", List.of(), null},
        new Object[] {"three", List.of("present"), null});
    table.refresh();

    assertNotNull(IndexBuilder.buildColumn(table, LocalTableFixture.LIST_COLUMN, config),
        "a file of mostly absent containers must still build");
    assertEquals(1, prune(table, config, LocalTableFixture.LIST_COLUMN, "present").size(),
        "and the one row that has a value is still indexed");
  }

  /**
   * The admission rule: a container is accepted on the kind of what it HOLDS, so a container of
   * something with no canonical form is refused exactly where that thing is. A path THROUGH a
   * container stays refused whatever it holds — that leaf is where the pruner cannot reason.
   */
  @Test
  void aContainerResolvesOnItsElementKindAndAPathThroughOneStaysRefused() throws Exception {
    BuildConfig config = LocalTableFixture.config();
    Table table = LocalTableFixture.createCollectionTable(dir);
    LocalTableFixture.appendCollectionRows(table, "a.parquet",
        new Object[] {"one", List.of("x"), Map.of("k", "v")});
    table.refresh();

    assertNotNull(IndexBuilder.buildColumn(table, LocalTableFixture.LIST_COLUMN, config));
    assertNotNull(IndexBuilder.buildColumn(table, LocalTableFixture.MAP_COLUMN, config));

    for (String refused : List.of("tags.element", "props.key", "props.value")) {
      IllegalArgumentException e = assertThrows(IllegalArgumentException.class,
          () -> IndexBuilder.buildColumn(table, refused, config), refused);
      assertTrue(e.getMessage().contains("repeated fields") || e.getMessage().contains("must exist"),
          refused + " -> " + e.getMessage());
    }
  }

  private static List<String> prune(
      Table table, BuildConfig config, String column, String token) throws Exception {
    io.kahshe.common.Metrics metrics = new io.kahshe.common.Metrics();
    io.kahshe.format.IndexPruner pruner = new io.kahshe.format.IndexPruner(
        new io.kahshe.format.type.term.TermIndex(config.format(), metrics), metrics,
        config.format());
    int fieldId = table.schema().findField(column).fieldId();
    List<io.kahshe.format.IndexPruner.ContainsHint> hints =
        List.of(new io.kahshe.format.IndexPruner.ContainsHint(column, fieldId, token,
            io.kahshe.format.IndexPruner.HintKind.MATCH));
    return pruner.prune(table, org.apache.iceberg.expressions.Expressions.alwaysTrue(), hints,
            LocalTableFixture.planTasks(table)).stream()
        .map(t -> t.file().location())
        .collect(Collectors.toList());
  }

  /** Just the file names, so assertions read as the fixture wrote them. */
  private static List<String> names(List<String> paths) {
    return paths.stream().map(p -> p.substring(p.lastIndexOf('/') + 1)).sorted().toList();
  }
}
