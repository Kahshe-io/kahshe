package io.kahshe.format.type.term;

import io.kahshe.indexer.BuildConfig;
import io.kahshe.indexer.build.IndexBuilder;
import io.kahshe.indexer.LocalTableFixture;
import io.kahshe.format.Coverage;
import io.kahshe.format.IndexPaths;
import io.kahshe.format.type.term.TermIndex;
import io.kahshe.format.type.term.TermIndexWriter;
import io.kahshe.format.type.term.TermRanges;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertNotSame;
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.junit.jupiter.api.Assertions.assertSame;
import static org.junit.jupiter.api.Assertions.assertTrue;

import io.kahshe.common.Metrics;
import java.nio.file.Path;
import java.util.List;
import java.util.concurrent.atomic.AtomicLong;
import org.apache.iceberg.Table;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

class TermIndexTest {
  private static final long PAST_TTL_MS = 31_000; // TermIndex.TTL_MS is 30s

  @TempDir Path tmp;

  /**
   * With KAHSHE_TERM_INDEX=false the build still writes blooms and grams, and the absence of a term
   * tier KEEPS files rather than skipping them.
   *
   * <p>The toggle exists because the term aggregate is the one tier whose size scales with the
   * table's DISTINCT VOCABULARY rather than its bytes. On a corpus whose text is mostly unique
   * identifiers -- trace ids, request ids, uuids -- nearly every term is a singleton, and the
   * aggregate approaches one row per row of source. On a large table of that shape the distinct-
   * term count reaches the order of the row count, which is more spill than the build's budget
   * allows and more heap than the reader can load. Substring pruning does not need it: the gram
   * layer answers contains
   * exactly for the files it covers, and the bloom layer answers it approximately for the rest.
   *
   * <p>What is asserted here is the safe direction. A missing term tier must make token MATCH
   * pruning UNAVAILABLE, never wrong -- an unavailable tier keeps every file, which costs a scan;
   * a wrong one skips a file that matches, which loses data from the answer.
   */
  @Test
  void aBuildWithTheTermTierOffStillPrunesOnGramsAndKeepsOnMatch() throws Exception {
    Table table = LocalTableFixture.createTable(tmp, "alpha bravo", "charlie delta");
    BuildConfig config = LocalTableFixture.configWithoutTermIndex();
    assertNotNull(IndexBuilder.buildColumn(table, LocalTableFixture.COLUMN, config));
    int fieldId = table.schema().findField(LocalTableFixture.COLUMN).fieldId();
    Path dir = Path.of(TermIndexWriter.dir(IndexPaths.root(table, config.format().indexRoot()), fieldId));

    // no aggregate leaf on disk, and none advertised
    try (var files = java.nio.file.Files.list(dir)) {
      assertTrue(
          files.map(f -> f.getFileName().toString()).noneMatch(n -> n.startsWith("aggregate-")),
          "the term tier was off but an aggregate leaf was written");
    }
    var meta =
        new com.fasterxml.jackson.databind.ObjectMapper()
            .readTree(dir.resolve("index-metadata.json").toFile());
    var leaves = TermIndexWriter.snapshotNode(meta).path("leaves");
    assertTrue(
        TermIndexWriter.aggregateLeaves(TermIndexWriter.snapshotNode(meta)).stream()
            .allMatch(String::isEmpty),
        "an aggregate range leaf is still named");
    // the gram layer IS written: it is what answers substring pruning without the term tier
    assertFalse(leaves.path("grams").asText("").isBlank(), "the gram leaf was lost with the terms");

    // and the reader treats it as absent rather than failing
    assertNull(
        new TermIndex(config.format(), new Metrics()).forField(table, fieldId),
        "a build with no term tier must read as no term index, not as an empty one");

    // and the row count is still right, because the metadata reports it and a later incremental
    // build reasons about it -- skipping tokenization must not skip counting rows
    var totals = TermIndexWriter.snapshotNode(meta).path("totals");
    assertEquals(2, totals.path("rows").asLong(), "rows were miscounted when tokenizing was off");

    // TOKENS must be zero, which is what proves tokenizing was actually SKIPPED rather than done
    // and discarded. A gate that keys on something other than the flag -- `listener != null`, say,
    // where NONE is a non-null no-op -- lets every build tokenize anyway: the index comes out
    // identical and nothing fails. Only the token total distinguishes the two.
    assertEquals(
        0, totals.path("tokens").asLong(),
        "tokens were counted despite the term tier being off, so tokenizing was not skipped");
  }

  /**
   * Turning the term tier back on rebuilds, rather than restamping and producing nothing.
   *
   * <p>Coverage records which FILES were read, not which tiers were written from them, and the two
   * come apart the moment the tier is optional. An index built with KAHSHE_TERM_INDEX=false covers
   * every file, so enabling the tier takes the incremental path, finds nothing new to do, restamps
   * the metadata in seconds and produces no term index -- while reporting success. That reads as a
   * completed build until someone notices the aggregate is missing.
   */
  @Test
  void enablingTheTermTierOverAnIndexBuiltWithoutItForcesAFullRebuild() throws Exception {
    Table table = LocalTableFixture.createTable(tmp, "alpha bravo", "charlie delta");
    int fieldId = table.schema().findField(LocalTableFixture.COLUMN).fieldId();

    // first build: blooms and grams only
    BuildConfig without = LocalTableFixture.configWithoutTermIndex();
    assertNotNull(IndexBuilder.buildColumn(table, LocalTableFixture.COLUMN, without));
    assertNull(
        new TermIndex(without.format(), new Metrics()).forField(table, fieldId),
        "the first build should have produced no term index");

    // second build over the SAME files, term tier enabled: coverage is complete, so nothing about
    // the file set says there is work to do
    BuildConfig with = LocalTableFixture.config();
    assertNotNull(IndexBuilder.buildColumn(table, LocalTableFixture.COLUMN, with));

    TermIndex index = new TermIndex(with.format(), new Metrics());
    TermIndex.Loaded loaded = index.forField(table, fieldId);
    assertNotNull(loaded, "enabling the term tier did not build one");
    assertTrue(
        index.entriesFor(table, loaded, List.of("alpha")).containsKey("alpha"),
        "the rebuilt term index does not contain a term the corpus holds");
  }

  /**
   * The term index writes nothing it does not read back. {@code TermIndex.load} opens the
   * aggregate leaf alone, so a postings or norms leaf written beside it would be storage nothing
   * opens — a dead tier that compiles, passes, and is visible only to someone who asks what the
   * reader opens. So it is asserted: every leaf the metadata advertises is one the read path
   * consumes.
   */
  @Test
  void theBuildWritesNoLeafTheReadPathNeverOpens() throws Exception {
    Table table = LocalTableFixture.createTable(tmp, "hello world", "goodbye moon");
    BuildConfig config = LocalTableFixture.config();
    assertNotNull(IndexBuilder.buildColumn(table, LocalTableFixture.COLUMN, config));
    int fieldId = table.schema().findField(LocalTableFixture.COLUMN).fieldId();
    Path dir = Path.of(TermIndexWriter.dir(IndexPaths.root(table, config.format().indexRoot()), fieldId));

    List<String> written;
    try (var files = java.nio.file.Files.list(dir)) {
      written =
          files.map(f -> f.getFileName().toString())
              .filter(n -> n.endsWith(".parquet"))
              .sorted()
              .toList();
    }
    assertFalse(written.isEmpty(), "the build wrote no leaves at all");
    for (String leaf : written) {
      assertFalse(
          leaf.startsWith("postings-") || leaf.startsWith("norms-"),
          "the build wrote " + leaf + ", which no read path opens: " + written);
    }

    // and the metadata does not advertise them either, so an incremental build cannot carry
    // forward a list of leaves that are no longer produced
    var meta =
        new com.fasterxml.jackson.databind.ObjectMapper()
            .readTree(dir.resolve("index-metadata.json").toFile());
    var leaves = TermIndexWriter.snapshotNode(meta).path("leaves");
    assertTrue(leaves.path("postings").isMissingNode(), "metadata still advertises a postings tier");
    assertTrue(leaves.path("norms").isMissingNode(), "metadata still advertises a norms tier");
    // positional: one slot per term range, and at least one holding this corpus's few terms
    var aggregates = TermIndexWriter.aggregateLeaves(TermIndexWriter.snapshotNode(meta));
    assertEquals(TermRanges.COUNT, aggregates.size(), "the leaf list is not one slot per range");
    assertTrue(
        aggregates.stream().anyMatch(leaf -> !leaf.isEmpty()), "no aggregate range leaf recorded");
  }

  @Test
  void loadRevalidateAndRecoverAcrossTtl() throws Exception {
    Table table = LocalTableFixture.createTable(tmp, "hello world", "goodbye moon");
    BuildConfig config = LocalTableFixture.config();
    assertNotNull(IndexBuilder.buildColumn(table, LocalTableFixture.COLUMN, config));
    int fieldId = table.schema().findField(LocalTableFixture.COLUMN).fieldId();

    TermIndex termIndex = new TermIndex(config.format(), new Metrics());
    AtomicLong clock = new AtomicLong(1_000_000);
    termIndex.nowMs = clock::get;

    TermIndex.Loaded first = termIndex.forField(table, fieldId);
    assertNotNull(first);
    assertTrue(first.weightBytes() > 0);
    assertTrue(termIndex.entriesFor(table, first, List.of("hello")).containsKey("hello"));

    // within TTL: served from cache with no metadata re-read (same-instance short-circuit)
    clock.addAndGet(1_000);
    assertSame(first, termIndex.forField(table, fieldId));

    // past TTL, artifact unchanged: revalidation renews the entry but retains the payload
    clock.addAndGet(PAST_TTL_MS);
    assertSame(first, termIndex.forField(table, fieldId));

    // artifact deleted: the next expiry observes absence
    String root = IndexPaths.root(table, config.format().indexRoot());
    table.io().deleteFile(TermIndexWriter.dir(root, fieldId) + "/index-metadata.json");
    clock.addAndGet(PAST_TTL_MS);
    assertNull(termIndex.forField(table, fieldId));

    // rebuild + expiry: a fresh index loads again — the absent entry does not stick
    assertNotNull(IndexBuilder.buildColumn(table, LocalTableFixture.COLUMN, config));
    clock.addAndGet(PAST_TTL_MS);
    TermIndex.Loaded rebuilt = termIndex.forField(table, fieldId);
    assertNotNull(rebuilt);
    assertNotSame(first, rebuilt);
    assertTrue(termIndex.entriesFor(table, rebuilt, List.of("goodbye")).containsKey("goodbye"));
  }
}
