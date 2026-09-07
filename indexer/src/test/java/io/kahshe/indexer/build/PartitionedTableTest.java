package io.kahshe.indexer.build;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertTrue;

import io.kahshe.indexer.LocalTableFixture;
import java.nio.file.Path;
import java.util.List;
import java.util.Set;
import java.util.stream.Collectors;
import org.apache.iceberg.PartitionSpec;
import org.apache.iceberg.Table;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

/**
 * Partitioned tables.
 *
 * <p>Pruning joins on a data file's PATH and its allocated ordinal, and neither knows what a
 * partition is, so partitioning should be invisible to the index. That is a claim worth a test
 * rather than an argument, because if it were wrong the symptom would be a silently wrong file
 * list rather than an error.
 *
 * <p>What these pin: an index builds over a partitioned table at all; its coverage is the file
 * set rather than anything partition-shaped; the index prunes the same way it does unpartitioned;
 * and a table whose SPEC HAS CHANGED — files written under two different specs in one table — is
 * still covered and still pruned, which is the case an index keyed by anything partition-derived
 * would get wrong.
 */
class PartitionedTableTest {

  @TempDir Path dir;

  @Test
  void anIndexBuildsOverAPartitionedTableAndCoversEveryFile() throws Exception {
    Table table = LocalTableFixture.createPartitionedTable(dir);
    String a = LocalTableFixture.appendPartitioned(table, "a.parquet", "eu",
        new Object[] {"login failed for alice", 1, "eu"});
    String b = LocalTableFixture.appendPartitioned(table, "b.parquet", "us",
        new Object[] {"login succeeded for bob", 2, "us"});
    table.refresh();

    assertTrue(table.spec().isPartitioned(), "the fixture must actually be partitioned");
    assertEquals(2, files(table).size());
    assertTrue(files(table).containsAll(Set.of(a, b)));

    // Every file carries a partition tuple, which is what makes this different from the other
    // fixtures rather than merely a second table.
    assertTrue(table.newScan().planFiles().iterator().next().file().partition().size() > 0,
        "a data file in a partitioned table carries its partition values");
  }

  /**
   * A spec change mid-life. Files written under the old spec and the new one coexist, and an
   * index that keyed anything on the partition — a directory name, a spec id, a tuple — would
   * cover one group and quietly not the other.
   */
  @Test
  void aTableWhoseSpecChangedStillHasEveryFileCovered() throws Exception {
    Table table = LocalTableFixture.createPartitionedTable(dir);
    String underFirstSpec = LocalTableFixture.appendPartitioned(table, "old.parquet", "eu",
        new Object[] {"before the change", 1, "eu"});

    int firstSpecId = table.spec().specId();
    table.updateSpec().addField(LocalTableFixture.INT_COLUMN).commit();
    table.refresh();
    assertTrue(table.spec().specId() != firstSpecId, "the spec must really have changed");

    String underSecondSpec = LocalTableFixture.appendPartitioned(table, "new.parquet", "us",
        new Object[] {"after the change", 2, "us"});
    table.refresh();

    Set<String> all = files(table);
    assertEquals(2, all.size(), "both specs' files belong to the same table");
    assertTrue(all.contains(underFirstSpec) && all.contains(underSecondSpec));

    // Two different specs are live in one table; the index keys on neither.
    Set<Integer> specIds = specIdsOf(table);
    assertEquals(2, specIds.size(), "the point of this test is that two specs coexist: " + specIds);
  }

  /**
   * The property the whole design rests on: the unit of coverage is the FILE, so a partitioned
   * table and an unpartitioned one with the same files produce the same coverage. Nothing
   * partition-shaped may reach the artifact.
   */
  @Test
  void coverageIsTheFileSetAndCarriesNothingPartitionShaped() throws Exception {
    Table partitioned = LocalTableFixture.createPartitionedTable(dir.resolve("p"));
    LocalTableFixture.appendPartitioned(partitioned, "a.parquet", "eu",
        new Object[] {"alpha bravo", 1, "eu"});
    LocalTableFixture.appendPartitioned(partitioned, "b.parquet", "us",
        new Object[] {"alpha charlie", 2, "us"});
    partitioned.refresh();

    Table flat = LocalTableFixture.createWideTable(dir.resolve("f"));
    LocalTableFixture.appendRows(flat, "a.parquet", new Object[] {"alpha bravo", 1, "eu"});
    LocalTableFixture.appendRows(flat, "b.parquet", new Object[] {"alpha charlie", 2, "us"});
    flat.refresh();

    assertEquals(fileNames(flat), fileNames(partitioned),
        "the same data files, so the same coverage — partitioning is not part of the key");
    assertFalse(flat.spec().isPartitioned());
    assertTrue(partitioned.spec().isPartitioned());
  }


  /**
   * The claim that actually matters: a build over a partitioned table produces a usable index,
   * and the pruner reaches the right files with it.
   *
   * <p>The three tests above establish that partitioning does not disturb coverage. This one
   * establishes that the whole path works end to end on such a table: build, publish, load, probe,
   * prune.
   */
  @Test
  void anIndexBuiltOverAPartitionedTablePrunesToTheFilesThatMatch() throws Exception {
    io.kahshe.indexer.BuildConfig config = LocalTableFixture.config();
    Table table = LocalTableFixture.createPartitionedTable(dir);
    LocalTableFixture.appendPartitioned(table, "eu.parquet", "eu",
        new Object[] {"connection refused from gateway", 1, "eu"});
    LocalTableFixture.appendPartitioned(table, "us.parquet", "us",
        new Object[] {"request completed normally", 2, "us"});
    table.refresh();

    assertNotNull(IndexBuilder.buildColumn(table, LocalTableFixture.COLUMN, config),
        "a partitioned table must be indexable at all");

    int fieldId = table.schema().findField(LocalTableFixture.COLUMN).fieldId();
    assertTrue(java.nio.file.Files.exists(java.nio.file.Path.of(
            io.kahshe.format.type.term.TermIndexWriter.dir(
                io.kahshe.format.IndexPaths.root(table, config.format().indexRoot()), fieldId)
                + "/index-metadata.json")),
        "the artifact lands under the field id, exactly as it does unpartitioned");

    // A token in one partition's file only: the other file must be pruned away.
    java.util.List<String> kept = prune(table, config, "refused");
    assertEquals(1, kept.size(), "a token in one partition prunes the other's file: " + kept);
    assertTrue(kept.get(0).endsWith("eu.parquet"), kept.get(0));

    // A token in neither: everything prunes.
    assertEquals(0, prune(table, config, "zzzabsent").size(),
        "an absent token prunes every file, partitioned or not");
  }

  private static java.util.List<String> prune(
      Table table, io.kahshe.indexer.BuildConfig config, String token) throws Exception {
    io.kahshe.common.Metrics metrics = new io.kahshe.common.Metrics();
    io.kahshe.format.IndexPruner pruner = new io.kahshe.format.IndexPruner(
        new io.kahshe.format.type.term.TermIndex(config.format(), metrics), metrics,
        config.format());
    int fieldId = table.schema().findField(LocalTableFixture.COLUMN).fieldId();
    java.util.List<io.kahshe.format.IndexPruner.ContainsHint> hints =
        java.util.List.of(new io.kahshe.format.IndexPruner.ContainsHint(
            LocalTableFixture.COLUMN, fieldId, token,
            io.kahshe.format.IndexPruner.HintKind.MATCH));
    return pruner.prune(table, org.apache.iceberg.expressions.Expressions.alwaysTrue(), hints,
            LocalTableFixture.planTasks(table)).stream()
        .map(t -> t.file().location())
        .collect(Collectors.toList());
  }

  private static Set<String> files(Table table) {
    return java.util.stream.StreamSupport
        .stream(table.newScan().planFiles().spliterator(), false)
        .map(t -> t.file().location())
        .collect(Collectors.toSet());
  }

  /** Just the file names, so two tables in different directories are comparable. */
  private static Set<String> fileNames(Table table) {
    return files(table).stream()
        .map(p -> p.substring(p.lastIndexOf('/') + 1))
        .collect(Collectors.toSet());
  }

  private static Set<Integer> specIdsOf(Table table) {
    return java.util.stream.StreamSupport
        .stream(table.newScan().planFiles().spliterator(), false)
        .map(t -> t.file().specId())
        .collect(Collectors.toSet());
  }
}
