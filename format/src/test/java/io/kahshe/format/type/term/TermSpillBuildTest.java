package io.kahshe.format.type.term;

import io.kahshe.indexer.RecordingListener;
import io.kahshe.indexer.BuildConfig;
import io.kahshe.indexer.build.IndexBuildListener;
import io.kahshe.indexer.build.IndexBuilder;
import io.kahshe.indexer.LocalTableFixture;
import io.kahshe.analysis.analyzer.Analyzer;
import io.kahshe.format.IndexPaths;
import io.kahshe.format.type.term.TermIndex;
import io.kahshe.format.type.term.TermIndexWriter;
import io.kahshe.format.type.term.TermRanges;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNotEquals;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import io.kahshe.common.Metrics;
import java.io.IOException;
import java.nio.ByteBuffer;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.ArrayList;
import java.util.List;
import org.apache.iceberg.Table;
import org.apache.iceberg.data.Record;
import org.apache.iceberg.data.parquet.GenericParquetReaders;
import org.apache.iceberg.io.CloseableIterable;
import org.apache.iceberg.parquet.Parquet;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;
import org.roaringbitmap.RoaringBitmap;

/**
 * The term index's build-time vocabulary accumulator spills to sorted local runs past its byte
 * budget and k-way merges them into the aggregate leaf. These tests pin what that may not change:
 * the leaf's rows, incremental coverage, and that no temp run survives a build either way.
 */
class TermSpillBuildTest {
  @TempDir Path tmp;

  /** Four data files with terms that recur across them, so runs really do have to merge. */
  private static final String[][] CORPUS = {
    {"alpha bravo charlie", "alpha delta"},
    {"bravo echo", "alpha foxtrot golf"},
    {"charlie hotel", "india juliett alpha"},
    {"kilo lima alpha", "bravo mike november"},
  };

  /**
   * One aggregate leaf row, with the ordinal bitmap resolved through the metadata's file list.
   * Iceberg plans manifests concurrently, so two full builds of one table number its data files
   * differently; the rows are compared by which files hold the term, which is what the numbering
   * means. Byte identity of the bitmaps themselves is pinned at the accumulator level, where the
   * ordinals are fixed — see RunMergerTest, which compares merged rows against a TreeMap oracle
   * over fixed ordinals rather than through a build.
   */
  private record Agg(String term, int fileCount, long totalCount, List<String> files) {}

  /**
   * A corpus dense enough that a small arena genuinely flushes mid-file.
   *
   * <p>{@link #CORPUS} carries only five indexable tokens per file, and the arena has a 64-byte
   * floor — small enough that a whole file can fit in one arena, at which point the "spilling"
   * configuration stops spilling and the comparison below is between two builds that behaved
   * identically. Density is what the property needs, so density is what this provides, rather than
   * reaching for a smaller arena that does not exist.
   */
  private static Table denseCorpus(Path dir) throws Exception {
    Files.createDirectories(dir);
    String[][] files = new String[4][];
    for (int f = 0; f < files.length; f++) {
      StringBuilder a = new StringBuilder();
      StringBuilder b = new StringBuilder();
      for (int t = 0; t < 25; t++) {
        a.append("alpha").append(t).append(' ').append("shared").append(t % 7).append(' ');
        b.append("bravo").append(f).append('x').append(t).append(' ').append("common").append(' ');
      }
      files[f] = new String[] {a.toString().trim(), b.toString().trim()};
    }
    Table table = LocalTableFixture.createTable(dir, files[0]);
    for (int i = 1; i < files.length; i++) {
      LocalTableFixture.appendFile(table, "f" + (i + 1) + ".parquet", files[i]);
    }
    return table;
  }

  private static Table corpus(Path dir) throws Exception {
    Files.createDirectories(dir);
    Table table = LocalTableFixture.createTable(dir, CORPUS[0]);
    for (int i = 1; i < CORPUS.length; i++) {
      LocalTableFixture.appendFile(table, "f" + (i + 1) + ".parquet", CORPUS[i]);
    }
    return table;
  }

  /**
   * A config whose reader arena is too small to hold one file, so it writes many sorted runs.
   *
   * <p>What makes a build write runs is the reader's arena filling — there is no shared vocabulary
   * map and no budget over one — so the arena size is the knob these tests turn.
   */
  private BuildConfig spilling(String name) {
    return inHeap(name, 8, 64L);
  }

  /** A config whose arena nothing reaches: one run per reader, written at the end. */
  private BuildConfig inHeap(String name) {
    return inHeap(name, 8, 8L << 20);
  }

  /** As {@link #inHeap}, with the reader count and per-reader buffer this build should use. */
  private BuildConfig inHeap(String name, int readers, long bufferBytes) {
    return LocalTableFixture.config(
        true, true, 16L * 1024 * 1024 * 1024, tmp.resolve(name).toString(),
        readers, bufferBytes);
  }

  private static long[] build(Table table, BuildConfig config, Metrics metrics) throws Exception {
    return IndexBuilder.buildColumn(
        table, LocalTableFixture.COLUMN, config, IndexBuildListener.NONE, "p", "ns", "t", metrics);
  }

  private static JsonNode termSnapshot(Table table, BuildConfig config) throws Exception {
    int fieldId = table.schema().findField(LocalTableFixture.COLUMN).fieldId();
    String path =
        TermIndexWriter.dir(IndexPaths.root(table, config.format().indexRoot()), fieldId)
            + "/index-metadata.json";
    try (var in = table.io().newInputFile(path).newStream()) {
      return TermIndexWriter.snapshotNode(new ObjectMapper().readTree(in));
    }
  }

  private static List<Agg> aggregateRows(Table table, BuildConfig config) throws Exception {
    JsonNode snapshot = termSnapshot(table, config);
    List<String> files = new ArrayList<>();
    snapshot.path("files").forEach(f -> files.add(f.asText()));
    List<String> paths = TermIndexWriter.aggregateLeaves(snapshot);
    List<Agg> rows = new ArrayList<>();
    // range order IS term order, so reading the leaves in order yields the same ascending stream a
    // single leaf did; empty ranges simply have no object
    for (String path : paths) {
      if (path.isEmpty()) {
        continue;
      }
    try (CloseableIterable<Record> records =
        Parquet.read(table.io().newInputFile(path))
            .project(TermIndexWriter.AGGREGATE_SCHEMA)
            .createReaderFunc(
                fs ->
                    GenericParquetReaders.buildReader(TermIndexWriter.AGGREGATE_SCHEMA, fs))
            .build()) {
      for (Record record : records) {
        RoaringBitmap ordinals = new RoaringBitmap();
        ordinals.deserialize(((ByteBuffer) record.getField("file_ordinals")).duplicate());
        List<String> holders = new ArrayList<>();
        ordinals.forEach((org.roaringbitmap.IntConsumer) o -> holders.add(files.get(o)));
        java.util.Collections.sort(holders);
        rows.add(
            new Agg(
                record.getField("term").toString(),
                (Integer) record.getField("file_count"),
                (Long) record.getField("total_count"),
                holders));
      }
    }
    }
    return rows;
  }

  @Test
  void spilledBuildWritesTheSameAggregateRowsAsAnInMemoryBuild() throws Exception {
    // one table built twice: file ordinals are assigned from planning order, so comparing two
    // separately created tables would compare two different numberings, not two accumulators
    Table table = denseCorpus(tmp.resolve("same"));
    Metrics heapMetrics = new Metrics();
    BuildConfig heapConfig = inHeap("heap-runs");
    assertNotNull(build(table, heapConfig, heapMetrics));
    List<Agg> expected = aggregateRows(table, heapConfig);
    List<Long> expectedTotals = totals(table, heapConfig);

    clearIndex(table, heapConfig);
    Metrics spillMetrics = new Metrics();
    BuildConfig spillConfig = spilling("spilled-runs");
    assertNotNull(build(table, spillConfig, spillMetrics));

    // The large-arena build still writes runs -- every build does -- but it writes ONE per reader
    // that saw a file, at the end.
    assertTrue(
        heapMetrics.termBuildSpills.sum() > 0
            && heapMetrics.termBuildSpills.sum() <= CORPUS.length,
        "an arena nothing fills should write one run per reader, got "
            + heapMetrics.termBuildSpills.sum());
    // The exact count is not pinned: it is a function of the arena size, which is a tuning knob.
    // What is pinned is that the small arena wrote strictly more runs than the large one -- without
    // that, the two halves of this comparison are the same build and the test is vacuous.
    assertTrue(
        spillMetrics.termBuildSpills.sum() > heapMetrics.termBuildSpills.sum(),
        "a 64-byte arena should write more runs than an 8 MiB one, got "
            + spillMetrics.termBuildSpills.sum() + " vs " + heapMetrics.termBuildSpills.sum());
    assertTrue(expected.size() > 10, "corpus should have a real vocabulary");
    assertEquals(expected, aggregateRows(table, spillConfig));
    assertEquals(expectedTotals, totals(table, spillConfig));
  }

  /**
   * Draining the reader's buffer at every opportunity must produce the same index as draining it
   * once.
   *
   * <p>The build streams terms into the accumulator from a fixed-size per-reader buffer, so where
   * the batch boundary falls depends on how many distinct terms a file happens to hold. That
   * boundary must be invisible in the output, or the index changes with the data.
   *
   * <p>A boundary that lost a term prunes away files that match, and a boundary that double-counted
   * one corrupts every count the aggregate reports.
   */
  @Test
  void theBufferBoundaryIsInvisibleInTheIndex() throws Exception {
    Table table = corpus(tmp.resolve("boundary"));

    // a buffer so small it drains after almost every row
    BuildConfig tiny = inHeap("boundary-tiny", 8, 1L);
    assertNotNull(build(table, tiny, new Metrics()));
    List<Agg> perRow = aggregateRows(table, tiny);
    List<Long> perRowTotals = totals(table, tiny);
    assertTrue(perRow.size() > 10, "corpus should have a real vocabulary");

    clearIndex(table, tiny);

    // and a buffer nothing reaches: one drain per file, at end of file
    BuildConfig large = inHeap("boundary-large", 8, 8L << 20);
    assertNotNull(build(table, large, new Metrics()));

    // The two builds must actually have differed. Without this the test is vacuous: a buffer size
    // that does not reach the build -- a static final resolved at class initialization, say --
    // leaves both halves running at whatever the first build in the JVM pinned, so this compares a
    // build against itself and which build it is depends on test ordering.
    assertNotEquals(
        tiny.termBufferBytes(), large.termBufferBytes(),
        "both halves used the same buffer size, so this compares a build against itself");

    assertEquals(perRow, aggregateRows(table, large),
        "where the reader's buffer drained changed the aggregate rows");
    assertEquals(perRowTotals, totals(table, large),
        "where the reader's buffer drained changed the reported totals");
  }

  /**
   * A term is written into the leaf for its own range, and a lookup reads only that leaf.
   *
   * <p>This is the property that makes the term tier usable at scale. The reader no longer holds
   * the vocabulary; it resolves a token to one range and reads one leaf. If a term were written to
   * one leaf and looked up in another the index would report "no file holds this term" for a term
   * that exists, and prune away every file that matches.
   */
  @Test
  void aTermIsWrittenIntoItsOwnRangeLeafAndFoundThere() throws Exception {
    Table table = corpus(tmp.resolve("ranges"));
    BuildConfig config = inHeap("range-runs");
    assertNotNull(build(table, config, new Metrics()));
    int fieldId = table.schema().findField(LocalTableFixture.COLUMN).fieldId();

    JsonNode snapshot = termSnapshot(table, config);
    List<String> leaves = TermIndexWriter.aggregateLeaves(snapshot);
    assertEquals(TermRanges.COUNT, leaves.size(), "the leaf list is not one slot per range");

    // every leaf that exists holds ONLY terms of its own range
    for (int range = 0; range < leaves.size(); range++) {
      if (leaves.get(range).isEmpty()) {
        continue;
      }
      try (CloseableIterable<Record> records =
          Parquet.read(table.io().newInputFile(leaves.get(range)))
              .project(TermIndexWriter.AGGREGATE_SCHEMA)
              .createReaderFunc(
                  fs -> GenericParquetReaders.buildReader(TermIndexWriter.AGGREGATE_SCHEMA, fs))
              .build()) {
        for (Record r : records) {
          String term = r.getField("term").toString();
          assertEquals(
              range, TermRanges.of(term),
              "term [" + term + "] was written into range " + range + ", not its own");
        }
      }
    }

    // and every term the corpus contains is found through the partitioned lookup
    TermIndex index = new TermIndex(config.format(), new Metrics());
    TermIndex.Loaded loaded = index.forField(table, fieldId);
    assertNotNull(loaded);
    java.util.Set<String> everyTerm = new java.util.TreeSet<>();
    for (String[] file : CORPUS) {
      for (String row : file) {
        everyTerm.addAll(Analyzer.tokenize(row));
      }
    }
    assertTrue(everyTerm.size() > 5, "corpus has too small a vocabulary to be a real test");
    var found = index.entriesFor(table, loaded, everyTerm);
    assertEquals(
        everyTerm, new java.util.TreeSet<>(found.keySet()),
        "a term the corpus contains was not found in its range leaf");
  }

  /**
   * Concurrent readers feeding the striped accumulator must produce what one reader would.
   *
   * <p>The accumulator is fed by every reader thread at once, striped by term range so a drain
   * only locks the range it touches. That is the whole point -- one lock over the whole
   * accumulator stalls every other reader for as long as a spill takes to write -- but it puts a
   * shared mutable structure in the path of the index's correctness. A lost update here is a term
   * with the wrong count, or a term missing from a stripe entirely, which prunes away files that
   * match.
   */
  @Test
  void concurrentReadersProduceWhatOneReaderWould() throws Exception {
    Table serial = corpus(tmp.resolve("serial"));
    // ONE reader, and it has to come from the Config the builder actually reads: a knob the build
    // ignores leaves this baseline running with the same reader count as the "concurrent" half.
    BuildConfig oneReader = inHeap("serial-runs", 1, 8L << 20);
    assertNotNull(build(serial, oneReader, new Metrics()));
    List<Agg> expected = aggregateRows(serial, oneReader);
    List<Long> expectedTotals = totals(serial, oneReader);
    assertTrue(expected.size() > 10, "corpus should have a real vocabulary");

    // the same corpus, built again with a real reader pool
    Table concurrent = corpus(tmp.resolve("concurrent"));
    BuildConfig manyReaders = inHeap("concurrent-runs", 8, 8L << 20);
    assertNotNull(build(concurrent, manyReaders, new Metrics()));

    assertNotEquals(
        oneReader.indexThreads(), manyReaders.indexThreads(),
        "both halves used the same reader count, so this compares concurrent against concurrent");

    // paths differ between the two tables, so compare the shape: term, file count, total count
    assertEquals(
        expected.stream().map(a -> a.term() + "/" + a.fileCount() + "/" + a.totalCount()).toList(),
        aggregateRows(concurrent, manyReaders).stream()
            .map(a -> a.term() + "/" + a.fileCount() + "/" + a.totalCount())
            .toList(),
        "concurrent readers produced different aggregate rows than a single reader");
    assertEquals(expectedTotals, totals(concurrent, manyReaders));
  }

  /**
   * Every term must land in the stripe its range names, and each stripe's rows must be sorted.
   *
   * <p>Draining the stripes in index order is what makes the whole output sorted, and that only
   * holds if range order is term order AND nothing crosses a stripe boundary. A term in the wrong
   * stripe is a term the reader will not find, because the reader goes straight from a token to
   * one leaf.
   */
  @Test
  void everyTermIsInItsOwnStripeAndEachStripeIsSorted() throws Exception {
    Table table = corpus(tmp.resolve("striped"));
    BuildConfig config = spilling("striped-runs"); // force spill+merge, not just the in-heap path
    assertNotNull(build(table, config, new Metrics()));

    List<String> leaves = TermIndexWriter.aggregateLeaves(termSnapshot(table, config));
    int seen = 0;
    for (int range = 0; range < leaves.size(); range++) {
      if (leaves.get(range).isEmpty()) {
        continue;
      }
      String previous = null;
      try (CloseableIterable<Record> records =
          Parquet.read(table.io().newInputFile(leaves.get(range)))
              .project(TermIndexWriter.AGGREGATE_SCHEMA)
              .createReaderFunc(
                  fs -> GenericParquetReaders.buildReader(TermIndexWriter.AGGREGATE_SCHEMA, fs))
              .build()) {
        for (Record r : records) {
          String term = r.getField("term").toString();
          assertEquals(range, TermRanges.of(term), "term [" + term + "] is in the wrong stripe");
          if (previous != null) {
            assertTrue(previous.compareTo(term) < 0,
                "stripe r" + range + " is not sorted: [" + previous + "] then [" + term + "]");
          }
          previous = term;
          seen++;
        }
      }
    }
    assertTrue(seen > 10, "no rows were checked");
  }

  /** Removes the whole sidecar index so the next build over the same table starts from nothing. */
  private static void clearIndex(Table table, BuildConfig config) throws IOException {
    Path root = Path.of(IndexPaths.root(table, config.format().indexRoot()));
    if (!Files.exists(root)) {
      return;
    }
    try (var stream = Files.walk(root)) {
      for (Path path : stream.sorted(java.util.Comparator.reverseOrder()).toList()) {
        Files.delete(path);
      }
    }
  }

  /** {rows, tokens, terms} as the metadata records them, in a stable order. */
  private static List<Long> totals(Table table, BuildConfig config) throws Exception {
    JsonNode node = termSnapshot(table, config).path("totals");
    return List.of(
        node.path("rows").asLong(), node.path("tokens").asLong(), node.path("terms").asLong());
  }

  @Test
  void spilledBuildStillServesItsTermIndex() throws Exception {
    Table table = corpus(tmp.resolve("serve"));
    BuildConfig config = spilling("serve-runs");
    assertNotNull(build(table, config, new Metrics()));

    int fieldId = table.schema().findField(LocalTableFixture.COLUMN).fieldId();
    TermIndex index = new TermIndex(config.format(), new Metrics());
    TermIndex.Loaded loaded = index.forField(table, fieldId);
    assertNotNull(loaded);
    // "alpha" is in every file, "kilo" only in the last, "zulu" in none. These come from the range
    // leaves on demand now, so this also exercises that a token lands in the leaf holding it.
    var found = index.entriesFor(table, loaded, List.of("alpha", "kilo", "zulu"));
    assertEquals(CORPUS.length, found.get("alpha").fileCount());
    assertEquals(CORPUS.length, found.get("alpha").ordinals().getCardinality());
    assertEquals(1, found.get("kilo").fileCount());
    assertTrue(found.get("kilo").ordinals().contains(ordinalOf(loaded, "f4.parquet")));
    assertNull(found.get("zulu"));
  }

  private static int ordinalOf(TermIndex.Loaded loaded, String fileName) {
    for (var entry : loaded.ordinalOf().entrySet()) {
      if (entry.getKey().endsWith("/" + fileName)) {
        return entry.getValue();
      }
    }
    throw new AssertionError("no ordinal for " + fileName);
  }

  @Test
  void incrementalAppendAfterASpilledBuildMatchesAFullRebuild() throws Exception {
    Table appended = corpus(tmp.resolve("appended"));
    BuildConfig appendedConfig = spilling("appended-runs");
    assertNotNull(build(appended, appendedConfig, new Metrics()));
    String newPath =
        LocalTableFixture.appendFile(appended, "f5.parquet", "oscar alpha", "papa bravo");

    RecordingListener listener =
        new RecordingListener();
    Metrics metrics = new Metrics();
    assertNotNull(
        IndexBuilder.buildColumn(
            appended, LocalTableFixture.COLUMN, appendedConfig, listener, "p", "ns", "t", metrics));
    // the prior aggregate joined the merge as a cursor: still incremental, still one file read
    assertEquals(IndexBuildListener.BuildKind.INCREMENTAL, listener.kind);
    assertEquals(List.of(newPath), listener.files);
    assertEquals(1, metrics.indexDataFilesRead.sum());

    List<Agg> incremental = aggregateRows(appended, appendedConfig);
    List<Long> incrementalTotals = totals(appended, appendedConfig);
    assertEquals(CORPUS.length + 1, termSnapshot(appended, appendedConfig).path("files").size());

    // a from-scratch, unspilled build over the same five files must agree row for row
    clearIndex(appended, appendedConfig);
    BuildConfig wholeConfig = inHeap("whole-runs");
    assertNotNull(build(appended, wholeConfig, new Metrics()));
    assertEquals(aggregateRows(appended, wholeConfig), incremental);
    assertEquals(totals(appended, wholeConfig), incrementalTotals);
  }

  /**
   * An incremental build whose new file contributes no indexable term must not destroy the term
   * index that already exists.
   *
   * <p>This is the shape behind a rule the writer states and nothing else enforced: the aggregate
   * is gated on the OPERATOR'S flag, never on whether this build happened to produce any sorted
   * runs. The two look identical from inside {@code finish} and are not the same thing.
   * {@link Analyzer#isIndexable} drops pure-numeric tokens over four digits, so a data file whose
   * indexed column holds only long numbers reads fine, tokenizes fine, and appends nothing to the
   * arena — no runs, and the term tier is still very much on.
   *
   * <p>Gate on the runs and the build republishes metadata that still lists every covered file
   * while naming no aggregate leaf. {@code TermIndex.load} reads that as "no term index", which
   * KEEPS every file — safe, and therefore completely silent. No error, no metric, and a column
   * that simply stops pruning from then on. That is advisory-keep exactly: the invariant absorbs
   * the failure, so the failure has to be made loud on purpose.
   *
   * <p>Note the restamp path at IndexBuilder does NOT reach this: a build with no new files at all
   * returns before finish(). It takes a new file that yields nothing.
   */
  @Test
  void anIncrementalBuildAddingNothingIndexableKeepsThePriorAggregate() throws Exception {
    Table table = corpus(tmp.resolve("nothing-indexable"));
    BuildConfig config = inHeap("nothing-indexable-runs");
    assertNotNull(build(table, config, new Metrics()));

    int fieldId = table.schema().findField(LocalTableFixture.COLUMN).fieldId();
    TermIndex before = new TermIndex(config.format(), new Metrics());
    TermIndex.Loaded loadedBefore = before.forField(table, fieldId);
    assertNotNull(loadedBefore);
    assertEquals(
        1, before.entriesFor(table, loadedBefore, List.of("alpha")).size(),
        "the first build did not index the corpus");

    // a new data file holding only tokens the analyzer refuses to index
    LocalTableFixture.appendFile(table, "numeric.parquet", "1234567 8901234", "5550001 5550002");
    table.refresh();
    assertNotNull(build(table, config, new Metrics()));

    TermIndex after = new TermIndex(config.format(), new Metrics());
    TermIndex.Loaded loadedAfter = after.forField(table, fieldId);
    assertNotNull(
        loadedAfter,
        "a build whose new file produced no runs dropped the aggregate it already had");
    assertEquals(
        CORPUS.length + 1, loadedAfter.files().size(), "the new file was not covered");
    assertEquals(
        1, after.entriesFor(table, loadedAfter, List.of("alpha")).size(),
        "a term the prior generation indexed is no longer resolvable");
  }

  @Test
  void spillRunsAreGoneAfterASuccessfulBuild() throws Exception {
    Table table = corpus(tmp.resolve("clean"));
    Path runs = tmp.resolve("clean-runs");
    assertNotNull(build(table, spilling("clean-runs"), new Metrics()));
    assertEquals(List.of(), listing(runs));
  }

  @Test
  void aSpillBudgetItCannotMeetFailsTheBuildAndLeavesNothingBehind() throws Exception {
    Table table = corpus(tmp.resolve("overrun"));
    Path runs = tmp.resolve("overrun-runs");
    // one data file's terms already exceed a 32-byte local budget, so the first run refuses
    BuildConfig config =
        LocalTableFixture.config(true, true, 32L, runs.toString(), 8, 64L);

    IOException failure =
        assertThrows(
            IOException.class,
            () -> build(table, config, new Metrics()));
    assertTrue(
        failure.getMessage().contains("KAHSHE_TERM_BUILD_MAX_SPILL_BYTES"),
        "the failure must name the budget: " + failure.getMessage());
    assertEquals(List.of(), listing(runs));
  }

  private static List<String> listing(Path dir) throws IOException {
    if (!Files.exists(dir)) {
      return List.of();
    }
    try (var stream = Files.list(dir)) {
      return stream.map(p -> p.getFileName().toString()).sorted().toList();
    }
  }

  // Peak accumulator heap is not asserted here: an estimate compared against the budget that same
  // estimator feeds cannot detect a divergence between the estimate and real heap, and real peak
  // heap at scale needs a corpus rather than a unit test.
  // RunBufferTest.capacityDoesNotDependOnTheVocabulary pins the property that IS checkable --
  // capacity is arena + offset index, both allocated, and byte-identical whatever the vocabulary.
}
