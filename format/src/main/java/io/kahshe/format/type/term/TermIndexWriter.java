package io.kahshe.format.type.term;

import com.fasterxml.jackson.databind.ObjectMapper;
import java.io.IOException;
import java.io.UncheckedIOException;
import java.nio.ByteBuffer;
import java.util.HashMap;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import org.apache.iceberg.Schema;
import org.apache.iceberg.Table;
import org.apache.iceberg.data.GenericRecord;
import org.apache.iceberg.data.Record;
import org.apache.iceberg.data.parquet.GenericParquetWriter;
import org.apache.iceberg.io.FileAppender;
import org.apache.iceberg.io.PositionOutputStream;
import org.apache.iceberg.parquet.Parquet;
import org.apache.iceberg.types.Types;
import org.roaringbitmap.RoaringBitmap;
import io.kahshe.format.Coverage;
import io.kahshe.format.IndexScope;
import io.kahshe.analysis.analyzer.Analyzer;
import io.kahshe.format.type.gram.GramIndexWriter;
import io.kahshe.format.type.gram.Grams;

/**
 * Writes the term index for one column: the range-partitioned aggregate leaves, the gram tier's
 * leaf, and the metadata that names both. Layout: {@code <index root>/term-v1-f<fieldId>/}; the
 * document's fields are specified in docs/FORMAT.md §5.
 */
public final class TermIndexWriter {
  /**
   * The Iceberg index spec's {@code format-version}, which the draft says must be 1; kahshe's own
   * tier version lives in {@code properties["kahshe.format-version"]} ({@link
   * #TIER_FORMAT_VERSION}) because the spec's field is not ours to bump. A reader refuses a
   * document newer than it on either number, keeping every file, and a builder refuses to build
   * over one.
   */
  public static final int FORMAT_VERSION = 1;

  /** Kahshe's term-tier layout version; 2 = range-partitioned aggregate with tombstoned coverage. */
  public static final int TIER_FORMAT_VERSION = 2;

  /**
   * The tier version a document was written under. A document with no
   * {@code kahshe.format-version} property is legacy: it carried the tier version in the spec's
   * {@code format-version}, so a 2 there means tier 2 and anything less means tier 1.
   */
  public static int tierVersionOf(com.fasterxml.jackson.databind.JsonNode meta) {
    com.fasterxml.jackson.databind.JsonNode tier = propertiesNode(meta).path("kahshe.format-version");
    if (!tier.isMissingNode()) {
      return tier.asInt(TIER_FORMAT_VERSION);
    }
    int spec = meta.path("format-version").asInt(1);
    return spec >= 2 ? spec : 1;
  }

  /** Whether a document is newer than this reader on the spec version or on kahshe's. */
  public static boolean newerThanThisReader(com.fasterxml.jackson.databind.JsonNode meta) {
    boolean hasTier = !propertiesNode(meta).path("kahshe.format-version").isMissingNode();
    int spec = meta.path("format-version").asInt(1);
    return (hasTier && spec > FORMAT_VERSION) || tierVersionOf(meta) > TIER_FORMAT_VERSION;
  }

  private static final org.slf4j.Logger LOG =
      org.slf4j.LoggerFactory.getLogger(TermIndexWriter.class);

  public static final Schema AGGREGATE_SCHEMA =
      new Schema(
          Types.NestedField.required(1, "term", Types.StringType.get()),
          Types.NestedField.required(2, "file_count", Types.IntegerType.get()),
          Types.NestedField.required(3, "total_count", Types.LongType.get()),
          Types.NestedField.required(4, "file_ordinals", Types.BinaryType.get()));

  /** Per-file accumulation produced by the tokenization pass. */
  public static final class FileTerms {
    final TermCounts counts = new TermCounts();
    public long rowCount;
    public long tokenTotal;
    /** The contract this file is being tokenized under; see Analyzer. Set by the build. */
    public Analyzer.Contract contract = Analyzer.contract(Analyzer.Kind.TOKENS, Analyzer.DEFAULT_MAX_TOKEN_LEN);

    /**
     * Records that a row exists without tokenizing it. Used when nothing consumes the term counts
     * -- no term dictionary and no watch rules -- where tokenizing is pure waste. The row count
     * still has to be right: it is what the metadata reports and what a later incremental build
     * reasons about.
     */
    public void rowWithoutTerms(int rowPosition) {
      rowCount = Math.max(rowCount, rowPosition + 1L);
    }

    /**
     * Records one row and its token count without keeping the terms. The tally lives here, per
     * file, because the arena it streams into is per reader and spans files.
     *
     * <p>It counts tokens, not indexable terms: the metadata's token total is what a build reports
     * as work done, and filtering it through {@code isIndexable} would under-report every file
     * holding long numerics.
     */
    public void countRow(int rowPosition, int tokens) {
      rowCount = Math.max(rowCount, rowPosition + 1L);
      tokenTotal += tokens;
    }

    public void row(int rowPosition, List<String> tokens) {
      for (String token : tokens) {
        if (!contract.isIndexable(token)) {
          continue;
        }
        counts.add(token);
      }
      tokenTotal += tokens.size();
      rowCount = Math.max(rowCount, rowPosition + 1L);
    }

    /** Exact occurrence count for one token in this file, or 0. */
    public long countOf(String token) {
      return counts.countOf(token);
    }

    /**
     * Whether this file's term counts were capped and are therefore a lower bound.
     *
     * <p>A consumer that trusts a truncated count can under-report and silently fail to fire. See
     * {@code TermCounts} for why the cap exists at all.
     */
    public boolean countsTruncated() {
      return counts.truncated();
    }

    public long rowCount() {
      return rowCount;
    }
  }

  public static String dir(String indexRoot, int fieldId) {
    return indexRoot + "/term-v1-f" + fieldId;
  }

  /** The snapshot-scoped section of a term metadata document; legacy documents are flat. */
  public static com.fasterxml.jackson.databind.JsonNode snapshotNode(
      com.fasterxml.jackson.databind.JsonNode meta) {
    return meta.has("snapshots") ? meta.path("snapshots").path(0) : meta;
  }

  /** The properties section of a term metadata document; legacy documents are flat. */
  public static com.fasterxml.jackson.databind.JsonNode propertiesNode(
      com.fasterxml.jackson.databind.JsonNode meta) {
    return meta.has("properties") ? meta.path("properties") : meta;
  }

  /**
   * Writes the aggregate leaf (and, when {@code gramMap} holds anything, the grams leaf) and then
   * the metadata, once, after all chunks — leaves always land before the metadata that points at
   * them. A null or empty {@code gramMap} writes no grams leaf and no gram-coverage: the table
   * serves from blooms.
   *
   * <p>The aggregate leaf is streamed out of the accumulator's k-way merge; an incremental build's
   * prior leaves join that merge as further sorted sources rather than being read into heap first.
   *
   * @param ordinalRemap old ordinal -> new, when this build renumbered live files; null when it
   *     did not
   * @param countsExact whether an occurrence count read off this index is exact. Separate from
   *     "has tombstones": total_count is a scalar sum, so a departed file's occurrences stay in it
   *     and compaction does not remove that inflation. Sticky until a full rebuild re-reads the
   *     data
   */
  public static void finish(
      Table table,
      org.apache.iceberg.io.FileIO io,
      String indexRoot,
      int fieldId,
      String column,
      long snapshotId,
      List<Coverage.Entry> fileCoverage,
      boolean termIndexEnabled,
      Analyzer.Contract contract,
      Grams.Contract grams,
      java.util.List<java.nio.file.Path> runs,
      RunMerger.Names runNames,
      List<String> priorAggregates,
      long rowsTotal,
      long tokenTotal,
      String priorUuid,
      Map<GramIndexWriter.ByteKey, RoaringBitmap> gramMap,
      int gramFromOrdinal,
      Map<Integer, Integer> ordinalRemap,
      boolean countsExact,
      int threads,
      List<Long> priorTermsPerRange,
      boolean partial)
      throws IOException {

    String dir = dir(indexRoot, fieldId);
    // An empty map is treated exactly like a null one: parquet creates the physical file lazily on
    // the first row, so a zero-record leaf writes no object and reading its own length back throws.
    // A column holding only nulls reaches here with zero grams, and that is legal input. Recording
    // no gram-coverage is also the safe direction: the exact layer treats an absent gram as proof
    // from from-ordinal on, so nothing may point at a leaf that holds nothing.
    GramIndexWriter.Coverage gramCoverage =
        gramMap == null || gramMap.isEmpty()
            ? null
            : GramIndexWriter.writeLeaf(io, indexRoot, fieldId, snapshotId, gramFromOrdinal, gramMap);

    // With the term tier off (KAHSHE_TERM_INDEX=false) the grams leaf and the coverage list are
    // still written, so the table serves substring pruning from the gram and bloom layers; only
    // token match pruning is unavailable, and its absence keeps files rather than skipping them.
    // The metadata records no aggregate leaf, which TermIndex reads as "no term index".
    Aggregate aggregate =
        termIndexEnabled
            ? writeAggregate(
                io, dir, snapshotId, runs, runNames, priorAggregates, ordinalRemap, threads,
                priorTermsPerRange)
            : new Aggregate(new java.util.ArrayList<>(), 0, new java.util.ArrayList<>());

    writeMetadata(
        table, io, dir, fieldId, column, snapshotId, fileCoverage, contract, grams, aggregate,
        rowsTotal, tokenTotal, priorUuid, gramCoverage, countsExact, partial);
  }

  /** The aggregate leaves this build published, with the term counts the metadata records. */
  private record Aggregate(List<String> paths, long termCount, List<Long> termsPerRange) {}

  /** What both aggregate merge paths read: where the leaves go, the runs, and the prior leaves. */
  private record AggregateInputs(
      org.apache.iceberg.io.FileIO io,
      String dir,
      long snapshotId,
      String nonce,
      java.util.List<java.nio.file.Path> runs,
      RunMerger.Names runNames,
      List<String> priorAggregates,
      Map<Integer, Integer> ordinalRemap) {}

  /** Writes the aggregate leaves and returns the paths, term count and per-range term counts. */
  private static Aggregate writeAggregate(
      org.apache.iceberg.io.FileIO io,
      String dir,
      long snapshotId,
      java.util.List<java.nio.file.Path> runs,
      RunMerger.Names runNames,
      List<String> priorAggregates,
      Map<Integer, Integer> ordinalRemap,
      int threads,
      List<Long> priorTermsPerRange)
      throws IOException {
    // The aggregate is TermRanges.COUNT leaves split by first character, so a reader opens only
    // the ranges its query tokens fall in rather than materializing the whole vocabulary.
    //
    // The drain arrives in term order and TermRanges.of is monotonic in the term, so the ranges are
    // contiguous: this walks them in order and never revisits one. Every range gets a slot, empty
    // ones included, so the leaf list is positional.
    String nonce = Long.toHexString(java.util.concurrent.ThreadLocalRandom.current().nextLong() >>> 32);
    AggregateInputs in =
        new AggregateInputs(
            io, dir, snapshotId, nonce, runs, runNames, priorAggregates, ordinalRemap);
    // Appenders are opened lazily, on the first row that lands in a range, and a range that
    // receives nothing records the empty string rather than a path: parquet writes no object for a
    // writer that never saw a row, so a named-but-unwritten leaf would be a path to nothing.
    String[] paths = new String[TermRanges.COUNT];
    long[] termsPerRange = new long[TermRanges.COUNT];
    boolean legacyLayout =
        priorAggregates != null && !priorAggregates.isEmpty()
            && priorAggregates.size() != TermRanges.COUNT;
    if (legacyLayout) {
      mergeWhole(in, paths, termsPerRange);
    } else {
      mergeRanges(in, threads, priorTermsPerRange, paths, termsPerRange);
    }
    long termCount = java.util.stream.LongStream.of(termsPerRange).sum();
    List<Long> termsPerRangeOut = new java.util.ArrayList<>();
    for (long n : termsPerRange) {
      termsPerRangeOut.add(n);
    }
    List<String> aggregatePaths = new java.util.ArrayList<>();
    for (int range = 0; range < TermRanges.COUNT; range++) {
      aggregatePaths.add(paths[range] == null ? "" : paths[range]);
    }
    long leafBytes = 0;
    for (String path : aggregatePaths) {
      if (!path.isEmpty()) {
        leafBytes += io.newInputFile(path).getLength();
      }
    }
    // Per-leaf sizes: how skewed the first-character partition is depends entirely on the corpus,
    // so the shape is logged rather than assumed.
    StringBuilder shape = new StringBuilder();
    for (int range = 0; range < TermRanges.COUNT; range++) {
      if (!aggregatePaths.get(range).isEmpty()) {
        shape.append(" r").append(String.format("%02d", range)).append('=')
            .append(io.newInputFile(aggregatePaths.get(range)).getLength() / 1024).append("KiB");
      }
    }
    LOG.info("term aggregate: {} terms in {} bytes across range leaves;{}",
        termCount, leafBytes, shape);
    return new Aggregate(aggregatePaths, termCount, termsPerRangeOut);
  }

  /** Merges a legacy whole-vocabulary prior, splitting the drain into range leaves as it goes. */
  private static void mergeWhole(AggregateInputs in, String[] paths, long[] termsPerRange)
      throws IOException {
    // The pre-partitioning single leaf spans every range and can only be merged whole; the
    // next full build republishes it partitioned, after which the per-range path applies.
    FileAppender<Record>[] outs = new FileAppender[TermRanges.COUNT];
    List<RunMerger.Source> prior = priorSources(in.io(), in.priorAggregates(), in.ordinalRemap());
    try {
      RunMerger.merge(
          in.runs(), prior, in.runNames(),
          (term, totalCount, ordinals) -> {
            if (ordinals.isEmpty()) {
              return;
            }
            int target = TermRanges.of(term);
            if (outs[target] == null) {
              paths[target] =
                  in.dir() + "/" + TermRanges.leafName(in.snapshotId(), target, in.nonce());
              outs[target] = appender(in.io(), paths[target], AGGREGATE_SCHEMA);
            }
            outs[target].add(aggregateRow(term, totalCount, ordinals));
            termsPerRange[target]++;
          });
    } finally {
      for (FileAppender<Record> out : outs) {
        if (out != null) {
          out.close();
        }
      }
      // the merge owns the prior sources and closes them
    }
  }

  /** Runs one merge per range on a bounded pool and waits for every one of them. */
  private static void mergeRanges(
      AggregateInputs in,
      int threads,
      List<Long> priorTermsPerRange,
      String[] paths,
      long[] termsPerRange)
      throws IOException {
    // One merge per range. A sorted run is range-contiguous (TermRanges.of is monotonic in the
    // term), so each range is a byte slice of every run plus that range's prior leaf, and the
    // ranges are independent. Memory is bounded by construction: at most `threads` merges, each
    // holding at most MAX_OPEN 64 KiB cursors and one writer.
    //
    // Copy-forward: a range no run has a row for, on a non-compacting incremental build, keeps its
    // prior leaf by reference. A compacting build (ordinalRemap != null) rewrites every range,
    // because compaction must translate every bitmap in the same build. A prior generation without
    // per-range term counts is rewritten too, once, so the count is never guessed.
    boolean copyForward =
        in.priorAggregates() != null && in.ordinalRemap() == null && priorTermsPerRange != null
            && priorTermsPerRange.size() == TermRanges.COUNT;
    java.util.concurrent.ExecutorService pool =
        java.util.concurrent.Executors.newFixedThreadPool(
            Math.max(1, threads),
            r -> {
              Thread t = new Thread(r, "kahshe-merge");
              t.setDaemon(true);
              return t;
            });
    List<java.util.concurrent.Future<?>> futures = new java.util.ArrayList<>();
    try {
      for (int range = 0; range < TermRanges.COUNT; range++) {
        final int r = range;
        futures.add(
            pool.submit(
                () -> {
                  mergeOneRange(in, r, copyForward, priorTermsPerRange, paths, termsPerRange);
                  return null;
                }));
      }
      for (java.util.concurrent.Future<?> future : futures) {
        try {
          future.get();
        } catch (java.util.concurrent.ExecutionException e) {
          Throwable cause = e.getCause();
          if (cause instanceof IOException io1) {
            throw io1;
          }
          throw new IOException("range merge failed", cause);
        } catch (InterruptedException e) {
          Thread.currentThread().interrupt();
          throw new IOException("interrupted while merging ranges", e);
        }
      }
    } finally {
      pool.shutdownNow();
    }
  }

  /** One range: keeps the prior leaf when no run touched it, otherwise merges and writes it. */
  private static void mergeOneRange(
      AggregateInputs in,
      int r,
      boolean copyForward,
      List<Long> priorTermsPerRange,
      String[] paths,
      long[] termsPerRange)
      throws IOException {
    boolean hasNew = false;
    for (java.nio.file.Path run : in.runs()) {
      long[] b = in.runNames().offsets(run);
      if (b == null) {
        // The same question RunMerger.mergeRange asks, and it must get the same answer. Read as
        // "holds nothing", a run with no offsets lets a non-compacting incremental build copy the
        // prior leaf forward over rows it never merged -- a covering index missing terms, which
        // prunes the files that match. RunMerger throws; so does this.
        throw new IOException("no range offsets recorded for run " + run);
      }
      if (b[r + 1] > b[r]) {
        hasNew = true;
        break;
      }
    }
    String priorPath =
        in.priorAggregates() == null || in.priorAggregates().size() != TermRanges.COUNT
            ? null
            : in.priorAggregates().get(r);
    if (copyForward && !hasNew) {
      paths[r] = priorPath == null || priorPath.isEmpty() ? null : priorPath;
      termsPerRange[r] = priorTermsPerRange.get(r);
      return;
    }
    List<RunMerger.Source> prior =
        priorPath == null || priorPath.isEmpty()
            ? List.of()
            : List.of(aggregateSource(in.io(), priorPath, in.ordinalRemap()));
    FileAppender<Record>[] out = new FileAppender[1];
    long[] written = new long[1];
    try {
      RunMerger.mergeRange(
          in.runs(), in.runNames(), r, prior, RunMerger.MAX_OPEN,
          (term, totalCount, ordinals) -> {
            if (ordinals.isEmpty()) {
              return;
            }
            if (out[0] == null) {
              paths[r] = in.dir() + "/" + TermRanges.leafName(in.snapshotId(), r, in.nonce());
              out[0] = appender(in.io(), paths[r], AGGREGATE_SCHEMA);
            }
            out[0].add(aggregateRow(term, totalCount, ordinals));
            written[0]++;
          });
    } finally {
      if (out[0] != null) {
        out[0].close();
      }
      // the merge owns the prior sources and closes them
    }
    termsPerRange[r] = written[0];
  }

  /** Assembles the index-metadata.json document and writes it, after the leaves it names. */
  private static void writeMetadata(
      Table table,
      org.apache.iceberg.io.FileIO io,
      String dir,
      int fieldId,
      String column,
      long snapshotId,
      List<Coverage.Entry> fileCoverage,
      Analyzer.Contract contract,
      Grams.Contract grams,
      Aggregate aggregate,
      long rowsTotal,
      long tokenTotal,
      String priorUuid,
      GramIndexWriter.Coverage gramCoverage,
      boolean countsExact,
      boolean partial) {
    // Document uses the draft Iceberg index spec's vocabulary (apache/iceberg#16961); kahshe keeps
    // a single index snapshot and collapses the tracking file into inline leaf lists. Every
    // divergence from the draft is listed in docs/FORMAT.md §9.
    Map<String, Object> meta = new java.util.LinkedHashMap<>();
    meta.put("format-version", FORMAT_VERSION);
    meta.put("uuid", priorUuid != null ? priorUuid : java.util.UUID.randomUUID().toString());
    meta.put("table-uuid", String.valueOf(table.uuid()));
    meta.put("location", dir);
    meta.put("type", "term-dictionary");
    // Required by the draft (format/index.md, apache/iceberg#17426). IDENTITY, not HASH: the term
    // leaf is organized by the analyzed term itself, in sorted order.
    meta.put("transform-function", "IDENTITY");
    // Required. A list because the spec allows composite keys; kahshe declares one column.
    meta.put("key-column-ids", java.util.List.of(fieldId));
    Map<String, Object> properties = new java.util.LinkedHashMap<>();
    properties.put("analyzer", contract.id());
    properties.put("grams", grams.id());
    // map<string,string> per the spec: every value a string, booleans included
    properties.put("counts-exact", String.valueOf(countsExact));
    properties.put("kahshe.format-version", String.valueOf(TIER_FORMAT_VERSION));
    if (partial) {
      // A CHECKPOINT: consistent over the files read so far, not over the table. Freshness reads
      // it as not-current (the build resumes), and _count refuses it (an exact count over part of
      // the files would be a wrong answer labelled exact). Pruning needs nothing: a file outside
      // coverage is kept.
      properties.put("partial", "true");
    }
    // Recorded so "this index covers 40 of 900 files" reads as a decision rather than a failure.
    // The reader does not consult it -- coverage is the file list, and an uncovered file is kept
    // whatever the reason -- but an operator looking at a small index needs to know which it is.
    if (IndexScope.isNarrowed(table)) {
      properties.put("index-scope", table.properties().get(IndexScope.PROPERTY));
    }
    properties.put("column", column);
    properties.put("field-id", String.valueOf(fieldId));
    meta.put("properties", properties);
    Map<String, Object> snapshot = new java.util.LinkedHashMap<>();
    snapshot.put("snapshot-id", 1);
    snapshot.put("source-table-snapshot-id", snapshotId);
    snapshot.put("timestamp-ms", System.currentTimeMillis());
    // Entries carry their own ordinal and liveness -- the list's order is not the numbering. See
    // Coverage for why a departed file is tombstoned in place rather than removed.
    snapshot.put("files", Coverage.toJson(fileCoverage));
    snapshot.put(
        "totals", Map.of("rows", rowsTotal, "tokens", tokenTotal, "terms", aggregate.termCount()));
    // per-range term counts, so a later build can carry an untouched range forward with its count
    snapshot.put("terms-per-range", aggregate.termsPerRange());
    Map<String, Object> leaves = new java.util.LinkedHashMap<>();
    leaves.put("aggregate", aggregate.paths());
    if (gramCoverage != null) {
      leaves.put("grams", gramCoverage.leafPath());
      Map<String, Object> coverage = new java.util.LinkedHashMap<>();
      coverage.put("from-ordinal", gramCoverage.fromOrdinal());
      coverage.put("grams", gramCoverage.grams());
      coverage.put("bytes", gramCoverage.bytes());
      snapshot.put("gram-coverage", coverage);
      LOG.info("gram leaf f{}: {} grams in {} bytes, from ordinal {}, rule {}",
          fieldId, gramCoverage.grams(), gramCoverage.bytes(), gramCoverage.fromOrdinal(), grams.id());
    }
    snapshot.put("leaves", leaves);
    meta.put("snapshots", List.of(snapshot));
    try (PositionOutputStream out =
        io.newOutputFile(dir + "/index-metadata.json").createOrOverwrite()) {
      out.write(new ObjectMapper().writerWithDefaultPrettyPrinter().writeValueAsBytes(meta));
    } catch (IOException e) {
      throw new UncheckedIOException(e);
    }
  }


  private static GenericRecord aggregateRow(
      String term, long totalCount, org.roaringbitmap.RoaringBitmap ordinals) {
    GenericRecord r = GenericRecord.create(AGGREGATE_SCHEMA);
    r.setField("term", term);
    r.setField("file_count", ordinals.getCardinality());
    r.setField("total_count", totalCount);
    r.setField("file_ordinals", ByteBuffer.wrap(serialize(ordinals)));
    return r;
  }

  /**
   * The prior generation's leaves, each as its own merge source. They join the single merge
   * alongside the runs rather than being concatenated, so no caller has to guarantee that range
   * order is term order across the whole list.
   */
  static List<RunMerger.Source> priorSources(
      org.apache.iceberg.io.FileIO io, List<String> paths, Map<Integer, Integer> remap)
      throws IOException {
    List<RunMerger.Source> sources = new java.util.ArrayList<>();
    if (paths == null) {
      return sources;
    }
    try {
      for (String path : paths) {
        if (path != null && !path.isEmpty()) {
          sources.add(aggregateSource(io, path, remap));
        }
      }
    } catch (IOException | RuntimeException e) {
      for (RunMerger.Source open : sources) {
        try {
          open.close();
        } catch (Exception ignored) {
          // closing a source while unwinding must not mask the failure that got us here
        }
      }
      throw e;
    }
    return sources;
  }

  /** Streams one aggregate leaf as a merge source. The leaf was written from the merge's own term
   * order, so it needs no sorting to join one. */
  static RunMerger.Source aggregateSource(
      org.apache.iceberg.io.FileIO io, String aggregatePath, Map<Integer, Integer> remap)
      throws IOException {
    org.apache.iceberg.io.CloseableIterable<Record> records =
        org.apache.iceberg.parquet.Parquet.read(io.newInputFile(aggregatePath))
            .project(AGGREGATE_SCHEMA)
            .createReaderFunc(
                fs -> org.apache.iceberg.data.parquet.GenericParquetReaders.buildReader(AGGREGATE_SCHEMA, fs))
            .build();
    try {
      return new AggregateSource(remap, records);
    } catch (IOException | RuntimeException e) {
      records.close();
      throw e;
    }
  }

  /**
   * Opens and closes a prior aggregate leaf. An incremental build consumes the leaf only at
   * finish(), so this is what keeps an unreadable one (moved, cleaned, truncated) discovered
   * before a single data file is read — while falling back to a full rebuild still costs only the
   * read it was going to do anyway.
   */
  public static void probeAggregate(org.apache.iceberg.io.FileIO io, String aggregatePath)
      throws IOException {
    try (RunMerger.Source source = aggregateSource(io, aggregatePath, null)) {
      source.term();
    }
  }

  /**
   * A prior generation's aggregate leaf, as a merge source. Its rows carry a whole ordinal bitmap
   * where a run's row carries one ordinal, which is why {@code file_count} has to be derived from
   * the union rather than summed: the two shapes cannot be added.
   */
  private static final class AggregateSource implements RunMerger.Source {
    private final org.apache.iceberg.io.CloseableIterable<Record> records;
    private final java.util.Iterator<Record> iterator;
    private byte[] term;
    private long count;
    private RoaringBitmap ordinals;

    private final Map<Integer, Integer> remap;

    AggregateSource(Map<Integer, Integer> remap, org.apache.iceberg.io.CloseableIterable<Record> records)
        throws IOException {
      this.remap = remap;
      this.records = records;
      this.iterator = records.iterator();
      next();
    }

    @Override
    public byte[] term() {
      return term;
    }

    @Override
    public long count() {
      return count;
    }

    @Override
    public void ordinalsInto(RoaringBitmap target) {
      target.or(ordinals);
    }

    @Override
    public void next() throws IOException {
      if (!iterator.hasNext()) {
        term = null;
        ordinals = null;
        return;
      }
      Record r = iterator.next();
      ByteBuffer buffer = (ByteBuffer) r.getField("file_ordinals");
      RoaringBitmap bitmap = new RoaringBitmap();
      bitmap.deserialize(buffer.duplicate());
      if (remap != null) {
        // Translate, do not reinterpret: this row's bits are numbers in the previous ordinal
        // space, and a bit left untranslated would name whichever file now holds that number.
        RoaringBitmap translated = new RoaringBitmap();
        bitmap.forEach((org.roaringbitmap.IntConsumer) old -> {
          Integer to = remap.get(old);
          if (to != null) {
            translated.add(to);
          }
        });
        bitmap = translated;
      }
      // a fresh array per row: the merge holds the smallest term while draining every source that
      // carries it, including this one, so a reused buffer would change under it
      term = r.getField("term").toString().getBytes(java.nio.charset.StandardCharsets.UTF_8);
      count = (Long) r.getField("total_count");
      ordinals = bitmap;
    }

    @Override
    public void close() throws IOException {
      records.close();
    }
  }

  private static FileAppender<Record> appender(org.apache.iceberg.io.FileIO io, String path, Schema schema)
      throws IOException {
    return Parquet.write(io.newOutputFile(path))
        .set("write.parquet.compression-codec", "zstd")
        // 1 MiB row groups, matching GramIndexWriter, against Iceberg's 128 MiB default: the
        // aggregate is written through TermRanges.COUNT appenders at once, so the default would
        // hold that many buffered row groups at finish(). Small groups also make the read cheaper,
        // since a term lookup skips row groups by their min/max term statistics.
        .set("write.parquet.row-group-size-bytes", "1048576")
        .set("write.parquet.page-size-bytes", "65536")
        .schema(schema)
        .createWriterFunc(type -> GenericParquetWriter.create(schema, type))
        .overwrite()
        .build();
  }

  public static byte[] serialize(RoaringBitmap bitmap) {
    bitmap.runOptimize();
    ByteBuffer buffer = ByteBuffer.allocate(bitmap.serializedSizeInBytes());
    bitmap.serialize(buffer);
    return buffer.array();
  }

  /** The aggregate leaf paths a term snapshot names: the range list, or the legacy single leaf. */
  public static List<String> aggregateLeaves(com.fasterxml.jackson.databind.JsonNode termSnapshot) {
    com.fasterxml.jackson.databind.JsonNode node = termSnapshot.path("leaves").path("aggregate");
    List<String> paths = new ArrayList<>();
    if (node.isArray()) {
      node.forEach(leaf -> paths.add(leaf.asText()));
    } else if (!node.asText("").isBlank()) {
      paths.add(node.asText());
    }
    return paths;
  }
}
