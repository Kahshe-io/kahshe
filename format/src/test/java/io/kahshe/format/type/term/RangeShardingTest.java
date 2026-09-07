package io.kahshe.format.type.term;

import io.kahshe.indexer.BuildConfig;
import io.kahshe.indexer.build.IndexBuilder;
import io.kahshe.indexer.LocalTableFixture;
import io.kahshe.format.type.bloom.IndexMeta;
import io.kahshe.format.IndexPaths;
import io.kahshe.format.IndexPruner;
import io.kahshe.format.type.bloom.IndexStore;
import io.kahshe.format.type.term.RunMerger;
import io.kahshe.format.type.term.TermIndex;
import io.kahshe.format.type.term.TermIndexWriter;
import io.kahshe.format.type.term.TermRanges;
import io.kahshe.format.type.term.TermRun;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertNotEquals;
import static org.junit.jupiter.api.Assertions.assertTrue;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import io.kahshe.common.Metrics;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.stream.Collectors;
import org.apache.iceberg.Table;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

/**
 * The term tier's merge runs one range at a time: runs are range-contiguous byte slices, each
 * range merges independently, an untouched range is carried forward by reference on an
 * incremental build, and a checkpointed build publishes in passes the incremental path consumes.
 *
 * <p>Guards verified red: the oracle with one range's merge skipped (that range's tokens vanish
 * and prune every file); copy-forward with it disabled (the untouched leaf is rewritten under a
 * new name); the checkpoint with the property ignored (one pass, one bloom leaf). The writer's
 * full write at a range boundary is NOT a guard and stays green when removed: terms in different
 * ranges differ in their first character, so the shared prefix across a boundary is zero by
 * construction; the slice test pins the offsets, not the reset.
 */
class RangeShardingTest {
  @TempDir Path tmp;

  private static JsonNode termSnapshot(Table table, BuildConfig config) throws Exception {
    int fieldId = table.schema().findField(LocalTableFixture.COLUMN).fieldId();
    Path meta =
        Path.of(
            TermIndexWriter.dir(IndexPaths.root(table, config.format().indexRoot()), fieldId)
                + "/index-metadata.json");
    return TermIndexWriter.snapshotNode(new ObjectMapper().readTree(Files.readString(meta)));
  }

  private static Set<String> kept(Table table, BuildConfig config, String token)
      throws Exception {
    Metrics metrics = new Metrics();
    IndexPruner pruner = new IndexPruner(new TermIndex(config.format(), metrics), metrics, config.format());
    return pruner
        .prune(
            table, null,
            List.of(new IndexPruner.ContainsHint(LocalTableFixture.COLUMN, token, IndexPruner.HintKind.MATCH)),
            LocalTableFixture.planTasks(table))
        .stream().map(t -> t.file().location()).collect(Collectors.toSet());
  }

  @Test
  void aRunsRangeSlicesReadBackExactlyItsRowsAndConcatenateToTheWhole() throws Exception {
    // terms in five ranges, several rows each, so slices have boundaries on both sides
    // sorted, as the writer requires: the arena sorts before it writes, this test writes directly
    String[] terms = {"0one", "0zero", "alpha", "apple", "mid", "mole", "zed", "zulu"};
    Path run = tmp.resolve("run.terms");
    TermRun.Writer writer = new TermRun.Writer(run);
    try (writer) {
      for (String t : terms) {
        byte[] b = t.getBytes(StandardCharsets.UTF_8);
        writer.append(b, 0, b.length, 1, 1L);
        writer.append(b, 0, b.length, 2, 3L);
      }
    }
    long[] bounds = writer.rangeOffsets();
    List<String> concatenated = new ArrayList<>();
    for (int r = 0; r < TermRanges.COUNT; r++) {
      try (TermRun.Cursor c = new TermRun.Cursor(run, bounds[r], bounds[r + 1])) {
        while (c.term() != null) {
          String term = new String(c.term(), StandardCharsets.UTF_8);
          assertEquals(r, TermRanges.of(term), "a slice yields only its own range's rows");
          concatenated.add(term + "/" + c.ordinal() + "/" + c.count());
          c.next();
        }
      }
    }
    List<String> whole = new ArrayList<>();
    try (TermRun.Cursor c = new TermRun.Cursor(run)) {
      while (c.term() != null) {
        whole.add(new String(c.term(), StandardCharsets.UTF_8) + "/" + c.ordinal() + "/" + c.count());
        c.next();
      }
    }
    assertEquals(whole, concatenated, "the slices concatenate to the whole run, in order");
    assertEquals(16, whole.size());
  }

  @Test
  void twoRunsSlicesOfOneRangeMergeToEveryTermOfThatRange() throws Exception {
    Map<Path, long[]> bounds = new HashMap<>();
    RunMerger.Names names =
        new RunMerger.Names() {
          @Override
          public Path next(int level, int sequence, boolean intermediate) {
            return tmp.resolve("merged-" + level + "-" + sequence + ".terms");
          }

          @Override
          public void released(Path run) {}

          @Override
          public void finished(Path run) {}

          @Override
          public void offsets(Path run, long[] b) {
            bounds.put(run, b);
          }

          @Override
          public long[] offsets(Path run) {
            return bounds.get(run);
          }
        };
    List<Path> runs = new ArrayList<>();
    String[][] contents = {{"0zero", "alpha", "mid"}, {"0one", "apple"}, {"9nine", "zed"}};
    for (int i = 0; i < contents.length; i++) {
      Path run = tmp.resolve("run-" + i + ".terms");
      TermRun.Writer writer = new TermRun.Writer(run);
      try (writer) {
        for (String t : contents[i]) {
          byte[] b = t.getBytes(StandardCharsets.UTF_8);
          writer.append(b, 0, b.length, i, 1L);
        }
      }
      names.offsets(run, writer.rangeOffsets());
      runs.add(run);
    }
    List<String> emitted = new ArrayList<>();
    RunMerger.mergeRange(
        runs, names, 0, List.of(), RunMerger.MAX_OPEN,
        (term, total, ordinals) -> emitted.add(term + "@" + ordinals));
    assertEquals(List.of("0one@{1}", "0zero@{0}"), emitted, "both range-0 terms, from two runs");
  }

  @Test
  void thePerRangeMergeMatchesAHashMapOracleAcrossRanges() throws Exception {
    BuildConfig config = LocalTableFixture.config();
    String[][] files = {
      {"0zero alpha mid zulu"}, {"0one apple mole"}, {"alpha zed zulu 9nine"}
    };
    Table table = LocalTableFixture.createTable(tmp, files[0]);
    List<String> paths = new ArrayList<>();
    paths.add(LocalTableFixture.planTasks(table).get(0).file().location());
    for (int i = 1; i < files.length; i++) {
      // f1.parquet is the createTable file: a same-named append would overwrite it on disk
      paths.add(LocalTableFixture.appendFile(table, "f" + (i + 1) + ".parquet", files[i]));
    }
    table.refresh();
    IndexBuilder.buildColumn(table, LocalTableFixture.COLUMN, config);

    Map<String, Set<String>> oracle = new HashMap<>();
    for (int i = 0; i < files.length; i++) {
      for (String token : files[i][0].split(" ")) {
        oracle.computeIfAbsent(token, k -> new HashSet<>()).add(paths.get(i));
      }
    }
    JsonNode snapshot = termSnapshot(table, config);
    long sum = 0;
    for (JsonNode n : snapshot.path("terms-per-range")) {
      sum += n.asLong();
    }
    long expectedTerms = oracle.size();
    List<String> wrong = new ArrayList<>();
    for (Map.Entry<String, Set<String>> e : new java.util.TreeMap<>(oracle).entrySet()) {
      Set<String> got = kept(table, config, e.getKey());
      if (!got.equals(e.getValue())) {
        wrong.add(e.getKey() + " expected " + e.getValue().size() + " got " + got.size());
      }
    }
    assertTrue(wrong.isEmpty(), "tokens pruned wrongly: " + wrong);
    assertTrue(kept(table, config, "absent").isEmpty());
  }

  @Test
  void anUntouchedRangeIsCarriedForwardByReferenceAndACompactionRewritesAll() throws Exception {
    BuildConfig config = LocalTableFixture.config();
    Table table = LocalTableFixture.createTable(tmp, "alpha bravo");
    IndexBuilder.buildColumn(table, LocalTableFixture.COLUMN, config);
    List<String> before = TermIndexWriter.aggregateLeaves(termSnapshot(table, config));

    String zFile = LocalTableFixture.appendFile(table, "f2.parquet", "zulu");
    table.refresh();
    IndexBuilder.buildColumn(table, LocalTableFixture.COLUMN, config);
    List<String> after = TermIndexWriter.aggregateLeaves(termSnapshot(table, config));

    int a = TermRanges.of("alpha");
    int z = TermRanges.of("zulu");
    assertEquals(before.get(a), after.get(a), "range 'a' received no row: its leaf is carried forward by reference");
    assertFalse(after.get(z).isEmpty());
    assertNotEquals(before.get(z), after.get(z), "range 'z' received rows: a fresh leaf");
    assertEquals(Set.of(zFile), kept(table, config, "zulu"));
    assertEquals(1, kept(table, config, "alpha").size(), "the carried-forward range still prunes");
  }

  @Test
  void aCheckpointedBuildPublishesInPassesAndEndsComplete() throws Exception {
    BuildConfig config = LocalTableFixture.config();
    Table table = LocalTableFixture.createTable(tmp, "one");
    for (int i = 2; i <= 5; i++) {
      LocalTableFixture.appendFile(table, "f" + i + ".parquet", "file" + i);
    }
    table.updateProperties()
        .set("kahshe.index." + LocalTableFixture.COLUMN + ".checkpoint-files", "2")
        .commit();
    table.refresh();
    long[] result = IndexBuilder.buildColumn(table, LocalTableFixture.COLUMN, config);

    assertEquals(5, result[2], "the wrapper reports every file read across passes");
    int fieldId = table.schema().findField(LocalTableFixture.COLUMN).fieldId();
    IndexMeta bloom =
        IndexMeta.parse(
            new ObjectMapper()
                .readTree(
                    Files.readString(
                        Path.of(IndexMeta.metaPath(IndexPaths.root(table, config.format().indexRoot()), fieldId)))));
    assertEquals(3, IndexStore.leaves(bloom).size(), "three passes (2 + 2 + 1), each a publish");
    JsonNode snapshot = termSnapshot(table, config);
    assertEquals(5, snapshot.path("files").size());
    Metrics metrics = new Metrics();
    TermIndex.Loaded loaded = new TermIndex(config.format(), metrics).forField(table, fieldId);
    assertFalse(loaded.partial(), "the final pass clears the checkpoint flag");
    assertEquals(1, kept(table, config, "file5").size(), "the last pass's file is indexed");
  }

  /**
   * A range whose merge CASCADES (more runs than the fan-in) must not collide with another range's
   * cascade, and must not delete the original runs, which every other range still reads through
   * its own slice. Naming intermediates by level and sequence alone collides across ranges, and a
   * cascade that deletes its inputs as it consumes them strands every range that has not merged
   * yet -- and neither shows on a corpus whose ranges never cascade. Red on the unfixed code: the
   * second range fails on a missing or already-existing file.
   */
  @Test
  void twoRangesThatEachCascadeShareNeitherNamesNorInputs() throws Exception {
    Map<Path, long[]> bounds = new HashMap<>();
    RunMerger.Names names =
        new RunMerger.Names() {
          @Override
          public Path next(int level, int sequence, boolean intermediate) {
            return tmp.resolve("merged-" + level + "-" + sequence + ".terms");
          }

          @Override
          public void released(Path run) {}

          @Override
          public void finished(Path run) {}

          @Override
          public void offsets(Path run, long[] b) {
            bounds.put(run, b);
          }

          @Override
          public long[] offsets(Path run) {
            return bounds.get(run);
          }
        };
    List<Path> runs = new ArrayList<>();
    for (int i = 0; i < 5; i++) { // five runs, fan-in two: every range cascades two levels
      Path run = tmp.resolve("run-" + i + ".terms");
      TermRun.Writer writer = new TermRun.Writer(run);
      try (writer) {
        for (String t : new String[] {"0zero" + i, "apple" + i}) {
          byte[] b = t.getBytes(StandardCharsets.UTF_8);
          writer.append(b, 0, b.length, i, 1L);
        }
      }
      names.offsets(run, writer.rangeOffsets());
      runs.add(run);
    }
    List<String> range0 = new ArrayList<>();
    RunMerger.mergeRange(runs, names, 0, List.of(), 2, (term, total, ordinals) -> range0.add(term));
    for (Path run : runs) {
      assertTrue(Files.exists(run), "an original run is shared by every range and is never deleted: " + run);
    }
    List<String> rangeA = new ArrayList<>();
    RunMerger.mergeRange(
        runs, names, TermRanges.of("apple"), List.of(), 2, (term, total, ordinals) -> rangeA.add(term));
    assertEquals(List.of("0zero0", "0zero1", "0zero2", "0zero3", "0zero4"), range0);
    assertEquals(List.of("apple0", "apple1", "apple2", "apple3", "apple4"), rangeA);
  }
}
