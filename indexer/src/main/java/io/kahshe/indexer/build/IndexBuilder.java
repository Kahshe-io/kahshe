package io.kahshe.indexer.build;

import io.kahshe.analysis.analyzer.Analyzer;
import io.kahshe.format.type.bloom.BloomIndexType;
import io.kahshe.format.type.bloom.BloomLeaf;
import io.kahshe.format.BuildReport;
import io.kahshe.format.BuildLease;
import io.kahshe.analysis.Canonical;
import io.kahshe.format.IcebergKinds;
import io.kahshe.format.Coverage;
import io.kahshe.format.type.gram.GramAccumulator;
import io.kahshe.format.type.gram.Grams;
import io.kahshe.format.type.gram.GramIndex;
import io.kahshe.format.type.gram.GramIndexWriter;
import io.kahshe.format.type.bloom.IndexMeta;
import io.kahshe.format.IndexPaths;
import io.kahshe.format.IndexScope;
import io.kahshe.format.type.IndexType;
import io.kahshe.format.type.IndexTypes;
import io.kahshe.format.type.bloom.NgramBloom;
import io.kahshe.format.type.term.RunBuffer;
import io.kahshe.format.type.term.TermIndexWriter;
import io.kahshe.format.type.term.TermRanges;
import io.kahshe.format.type.term.TermRunStore;
import com.fasterxml.jackson.databind.ObjectMapper;
import java.io.IOException;
import java.io.UncheckedIOException;
import java.util.ArrayList;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Set;
import org.apache.iceberg.FileScanTask;
import org.apache.iceberg.Schema;
import org.apache.iceberg.Table;
import org.apache.iceberg.catalog.TableIdentifier;
import org.apache.iceberg.data.Record;
import org.apache.iceberg.data.parquet.GenericParquetReaders;
import org.apache.iceberg.io.CloseableIterable;
import org.apache.iceberg.io.PositionOutputStream;
import org.apache.iceberg.parquet.Parquet;
import org.apache.iceberg.types.Types;
import io.kahshe.indexer.BuildConfig;
import io.kahshe.indexer.TableSource;

/**
 * Builds every registered index tier for one column at the table's current snapshot, in a single
 * read pass per data file: n-gram blooms and the exact gram layer for substring pruning, the term
 * index for token pruning and counts. Indexable columns are string, integer, long, uuid, binary
 * and fixed, at a path passing only through structs.
 *
 * <p>Rebuild is idempotent, and incremental where it can be: a build reads only the data files no
 * coverage entry names, and republishes the rest of the prior generation. A build holds the
 * column's {@link BuildLease} from its first read to its last publish, refuses to run at all when
 * its configured memory cannot fit the heap, and leaves the previous index untouched when it
 * fails — an unindexed or stale-but-covered table is correct, merely unpruned.
 */
public final class IndexBuilder {
  private static final org.slf4j.Logger LOG = org.slf4j.LoggerFactory.getLogger(IndexBuilder.class);




  /**
   * The dead share of coverage past which a build renumbers the survivors and drops the tombstones.
   *
   * <p>Tombstones make a departure cheap but not free forever: a table on rolling retention
   * eventually carries coverage that is almost entirely dead, and the aggregate keeps rows whose
   * only file is gone. Past this fraction the build translates every bitmap into a contiguous
   * numbering and drops what no live file holds. Not lower, because compaction is only free while
   * the aggregate is being rewritten anyway, and because each one invalidates every reader's
   * cached ordinals at once.
   */
  static final int TOMBSTONE_COMPACT_DEAD_PERCENT = 25;

  /**
   * ...and at least this many dead entries, which is not the same guard twice.
   *
   * <p>A fraction alone makes small tables compact constantly: one file leaving a three-file table
   * is 33%, so the very first removal would renumber everything — more expensive than the full
   * rebuild tombstones replaced. The floor says what is meant: compact when there is real dead
   * weight to reclaim, not when the ratio looks bad on a table too small for it to mean anything.
   */
  static final int TOMBSTONE_COMPACT_MIN_DEAD = 16;

  /**
   * Per-entry heap cost of one gram map entry, beyond the key's own bytes: a {@code HashMap.Node}
   * (~48 B), a {@code ByteKey} object (~16 B plus its array reference), and a
   * {@code RoaringBitmap} with at least one container (~64 B before it holds anything).
   *
   * <p>Deliberately rounded UP. An over-count trips the gram valve early, which degrades the
   * column to blooms — slower and correct. An under-count trips it late or never, which is an OOM.
   * The two errors are not symmetric, so the estimate should not be either.
   */
  static final int GRAM_ENTRY_OVERHEAD_BYTES = 144;

  /**
   * Heap a grams leaf costs relative to its serialized size, used to refuse one before reading it.
   * Deserializing turns a packed leaf into a {@code ByteKey}, a map node and a
   * {@code RoaringBitmap} per gram, so the in-heap figure runs well above the object's size. Keep
   * this equal to the factor {@link GramIndex} applies on the serving side, or the build and the
   * reader refuse at different points and one accepts an artifact the other will not open.
   */
  static final int GRAM_LEAF_HEAP_FACTOR = 3;

  /**
   * Heap the accumulated per-file blooms may occupy before a build refuses to continue. Every
   * file's bloom is held from the moment it is built until the single leaf write at the end, so
   * this map is O(files) for the whole build with nothing releasing it — the one structure here
   * whose size follows the table rather than a configured number.
   *
   * <p>Refusing rather than degrading, unlike the gram valve: dropping the gram layer mid-build
   * only costs pruning quality, while dropping the BLOOMS would leave the metadata's leaf list and
   * coverage disagreeing about what was written. Failing is safe — the table keeps its previous
   * index and stays correct — and it names what to change, which an OOM does not.
   */
  static final long BLOOM_BUILD_MAX_BYTES =
      Long.getLong("kahshe.bloom.build.max.bytes", 1L << 30);
  /**
   * Deployment default; {@code kahshe.index.<column>.bloom-fpp} / {@code kahshe.index.bloom-fpp}
   * override it per column or table (IndexSettings). Higher is smaller and prunes less — a pure
   * cost dial, safe in both directions because a bloom's false positives only ever KEEP files.
   */
  private static final double FPP = 0.01;
  private IndexBuilder() {}

  /**
   * The build with nothing observing it: {@link #run(TableSource, BuildConfig, String, String,
   * String, IndexBuildListener)} with {@link IndexBuildListener#NONE}.
   *
   * @throws IOException if the table cannot be read or an artifact cannot be published
   */
  public static void run(
      TableSource catalogs, BuildConfig config, String prefix, String tableName, String column)
      throws IOException {
    run(catalogs, config, prefix, tableName, column, IndexBuildListener.NONE);
  }

  /**
   * Loads {@code tableName} through {@code catalogs} and builds one column's index for its current
   * snapshot, publishing the artifacts under the configured index root. A table with no snapshots
   * is a no-op, not a failure.
   *
   * <p>This is the {@code kahshe index} CLI's entry point, so it also writes the index root and
   * the build's totals to {@code System.out} — a library caller that wants the build without the
   * printing calls {@code buildColumn} instead.
   *
   * @throws IOException if the table cannot be read or an artifact cannot be published
   */
  public static void run(
      TableSource catalogs, BuildConfig config, String prefix, String tableName, String column,
      IndexBuildListener listener)
      throws IOException {
    long start = System.nanoTime();
    TableIdentifier ident = TableIdentifier.parse(tableName);
    Table table = catalogs.load(prefix, ident);
    System.out.println("index root: " + IndexPaths.root(table, config.format().indexRoot()));
    long[] result =
        buildColumn(table, column, config, listener, prefix, ident.namespace().toString(), ident.name());
    if (result == null) {
      System.out.println("table has no snapshots; nothing to index");
      return;
    }
    double seconds = (System.nanoTime() - start) / 1e9;
    System.out.printf(
        "indexed %s.%s: %d files, snapshot %d, %.1fs%n", tableName, column, result[2], result[0], seconds);
    System.out.printf(
        "data bytes: %,d | bloom bytes: %,d (%s)%n",
        result[3],
        result[1],
        // a restamp reads no data, and a ratio against zero bytes says nothing
        result[3] > 0 ? String.format("%.2f%%", 100.0 * result[1] / result[3]) : "no data read");
  }

  /**
   * One row's tokens for a list or a map column, or null when the row contributes none.
   *
   * <p>Each element is canonicalised, grammed and tokenized ON ITS OWN, never joined into one
   * string: a gram or a token spanning an element boundary would match text that does not exist in
   * any value. A map contributes its VALUES; keys are not indexed and are not qualified onto
   * values, so a probe answers "some entry's value emitted this term" and `element_at(m,'k')='v'`
   * is served at the selectivity of "'v' appears under some key" -- an over-approximation, which is
   * the direction the invariant permits.
   *
   * <p>Under a whole-value contract a row contributes each distinct value ONCE. Without that,
   * {@code ["x","x"]} would answer 2 for `_count`, which publishes itself as exact and has meant
   * ROWS holding the value ever since a whole-value cell emitted exactly one term. A tokens
   * contract keeps counting occurrences, as it already does for scalar text.
   */
  private static List<String> containerTokens(
      Object value, IcebergKinds.Shape shape, Analyzer.Contract contract,
      GramAccumulator gramSet, boolean countTerms) {
    if (value == null) {
      return null;
    }
    Iterable<?> elements = shape.repetition() == IcebergKinds.Repetition.MAP
        ? ((java.util.Map<?, ?>) value).values()
        : (Iterable<?>) value;
    List<String> tokens = countTerms ? new ArrayList<>() : null;
    boolean any = false;
    for (Object element : elements) {
      String text = Canonical.form(shape.kind(), element);
      if (text == null) {
        continue;
      }
      any = true;
      gramSet.addAll(text);
      if (tokens != null) {
        tokens.addAll(contract.tokens(text));
      }
    }
    if (!any) {
      return null;
    }
    if (tokens == null) {
      return List.of();
    }
    if (contract.kind() == Analyzer.Kind.VALUE && tokens.size() > 1) {
      List<String> distinct = new ArrayList<>(new java.util.LinkedHashSet<>(tokens));
      return distinct;
    }
    return tokens;
  }

  /**
   * Refuses a column whose path passes through a list or map, naming the field it passes through.
   * Walks PARENT IDS up from the leaf rather than splitting the name on dots: a column name may
   * itself contain a dot, and field id is what the artifacts are keyed by.
   */
  static void requireStructPath(Schema schema, Types.NestedField leaf, String column) {
    Map<Integer, Integer> parents =
        org.apache.iceberg.types.TypeUtil.indexParents(schema.asStruct());
    for (Integer parent = parents.get(leaf.fieldId()); parent != null; parent = parents.get(parent)) {
      org.apache.iceberg.types.Type type = schema.findType(parent);
      if (!type.isStructType()) {
        throw new IllegalArgumentException(
            "repeated fields (list or map) are not supported yet: " + column
                + " passes through " + schema.findColumnName(parent) + " (" + type.typeId() + ")");
      }
    }
  }

  /** {@code kahshe.index.<column>.max-token-length}, else {@code kahshe.index.max-token-length}, else config. */
  static int maxTokenLength(Table table, String column, BuildConfig config) {
    return IndexSettings.positiveInt(table, column, "max-token-length", config.maxTokenLength());
  }

  /**
   * Whether a tier participates for this column: {@code kahshe.index.<column>.<key>}, else
   * {@code kahshe.index.<key>}, else the deployment flag — which is a CEILING, not a default. A
   * property may narrow (turn a tier off for one column) but never widen: {@link BuildBudget} and
   * the serving caches are both sized from the deployment flags, so a tier built past them would
   * be unbudgeted at build time and never opened at serve time. A widening attempt WARNs, because
   * an operator who set it must see that it did nothing.
   */
  static boolean tierEnabled(
      Table table, String column, String key, boolean deploymentEnabled) {
    Boolean requested = IndexSettings.flagOrNull(table, column, key);
    if (requested == null) {
      return deploymentEnabled;
    }
    if (requested && !deploymentEnabled) {
      LOG.warn(
          "{} for column {} requests a tier the deployment has off; the deployment flag is a "
              + "ceiling — the build is budgeted and the serving caches are sized by it, so the "
              + "tier stays off",
          key, column);
      return false;
    }
    return requested;
  }

  /**
   * Builds both indexes for one column at the table's current snapshot. Returns
   * {@code {snapshotId, bloomBytes, filesCovered, dataBytes}}, or null when the table has no
   * snapshot to index.
   */
  public static long[] buildColumn(Table table, String column, BuildConfig config)
      throws IOException {
    return buildColumn(
        table, column, config, IndexBuildListener.NONE, "", "", "", new io.kahshe.common.Metrics());
  }

  /**
   * As above, with a build listener. The table identity strings are only handed to the listener;
   * pass decoded (human-readable) values.
   */
  public static long[] buildColumn(
      Table table, String column, BuildConfig config, IndexBuildListener listener,
      String prefix, String namespace, String tableName)
      throws IOException {
    return buildColumn(
        table, column, config, listener, prefix, namespace, tableName, new io.kahshe.common.Metrics());
  }

  /**
   * As above, counting gram-layer build events (valve skips) on the given metrics — and holding
   * the column's {@link BuildLease} for the whole build, so two builders of one column cannot
   * interleave their two metadata publishes. A snapshotless table or an unknown column takes no
   * lease: the inner method answers those before anything is written.
   */
  public static long[] buildColumn(
      Table table, String column, BuildConfig config, IndexBuildListener listener,
      String prefix, String namespace, String tableName, io.kahshe.common.Metrics metrics)
      throws IOException {
    // Wrapped ONCE, here: every hook below -- readsTermCounts, beforePublish, start and the
    // context it returns -- is then safe to call bare, and a listener failure never fails a build.
    listener = IndexBuildListener.safe(listener);
    // BY IDENTITY, not by the name the property was written with. The artifacts are keyed by field
    // id, and an operator who renamed the column has a kahshe.index entry that still says the old
    // name; resolving by name alone would find no field and stop maintaining the column in silence.
    String configured = column;
    column = IndexSettings.currentName(table, column);
    if (!column.equals(configured)) {
      LOG.warn(
          "column {} was renamed to {}; maintaining its index under field id {} -- update "
              + "kahshe.index and any kahshe.index.{}.* properties to the new name",
          configured, column, table.schema().findField(column).fieldId(), configured);
    }
    var field = table.currentSnapshot() == null ? null : table.schema().findField(column);
    if (field == null) {
      return buildColumnHeld(
          table, column, config, listener, prefix, namespace, tableName, metrics, null, 0);
    }
    BuildLease lease =
        BuildLease.acquire(
            IndexPaths.io(table, config.format()),
            BuildLease.leasePath(IndexPaths.root(table, config.format().indexRoot()), field.fieldId()),
            BuildLease.ownerId(),
            System.currentTimeMillis());
    // CHECKPOINTS are ordinary publishes in a loop: at most `checkpointFiles` new files per pass,
    // each pass consumed by the next through the incremental path unchanged, all under one lease.
    // A pass that is not the last is stamped partial (see TermIndexWriter.finish). Default off.
    int checkpointFiles = IndexSettings.positiveInt(table, column, "checkpoint-files", 0);
    try {
      long[] total = null;
      while (true) {
        long[] pass =
            buildColumnHeld(
                table, column, config, listener, prefix, namespace, tableName, metrics, lease,
                checkpointFiles);
        if (pass == null) {
          return null;
        }
        long left = pass.length > 4 ? pass[4] : 0; // the restamp path answers four elements
        if (total == null) {
          total = pass;
        } else {
          total = new long[] {pass[0], total[1] + pass[1], total[2] + pass[2], total[3] + pass[3], left};
        }
        if (left == 0) {
          return total;
        }
        LOG.info("checkpoint published; {} new file(s) remain for the next pass", pass[4]);
      }
    } finally {
      lease.release();
    }
  }

  /** One column build's fixed identity: what is indexed, where it is written, under which contracts. */
  private record BuildTarget(
      Table table, org.apache.iceberg.io.FileIO indexIo, String indexRoot, String column,
      int fieldId, long snapshotId, String prefix, String namespace, String tableName,
      Analyzer.Contract contract, Grams.Contract gramRule, boolean termEnabled, boolean gramEnabled,
      long startedMs) {}

  /** The prior generation as this build reads it, before it decides anything. */
  private record PriorIndex(
      com.fasterxml.jackson.databind.JsonNode termMeta, List<Coverage.Entry> coverage,
      List<Long> termsPerRange, List<String> covered, java.util.Set<String> priorCovered) {}

  /** Coverage after tombstoning, reviving, and -- past the threshold -- renumbering. */
  private record Reconciled(
      List<Coverage.Entry> coverage, List<String> departedPaths,
      java.util.Map<Integer, Integer> ordinalRemap) {}

  /** The files this pass reads, and how many a checkpoint left for the next one. */
  private record NewFiles(List<Coverage.Entry> coverage, List<String> newPaths, int remaining) {}

  /** The gram tier's build state: the map being accumulated and where its coverage starts. */
  private static final class GramState {
    Map<GramIndexWriter.ByteKey, org.roaringbitmap.RoaringBitmap> map;
    long keyBytes;
    int fromOrdinal;
  }

  /** What survived the prior aggregate's probe, and the totals it carries forward. */
  private record PriorAggregates(
      boolean incremental, List<String> leaves, long rowsTotal, long tokenTotal) {}

  /** Whether anything on this build reads term counts. */
  private record Counting(boolean countTerms, boolean listenerReadsCounts) {}

  /** What the read pass produced. */
  private record ReadPassResult(long rowsTotal, long tokenTotal, long dataBytes, int saturated) {}

  private static long[] buildColumnHeld(
      Table table, String column, BuildConfig config, IndexBuildListener listener,
      String prefix, String namespace, String tableName, io.kahshe.common.Metrics metrics,
      BuildLease lease, int maxNewFiles)
      throws IOException {
    // BEFORE anything is opened: a configuration that cannot fit the heap is arithmetic, and
    // checking it costs microseconds against an hours-long build that dies part-way. See
    // BuildBudget.
    long startedMs = System.currentTimeMillis();
    BuildBudget.check(config, Runtime.getRuntime().maxMemory());

    String indexRoot = IndexPaths.root(table, config.format().indexRoot());
    org.apache.iceberg.io.FileIO indexIo = IndexPaths.io(table, config.format());
    if (table.currentSnapshot() == null) {
      return null;
    }
    long snapshotId = table.currentSnapshot().snapshotId();
    BuildTarget target =
        resolveTarget(table, column, config, indexIo, indexRoot, snapshotId, prefix, namespace,
            tableName, startedMs);

    PriorIndex prior = readPriorIndex(target);
    Map<String, Long> sizes = scanCurrentFiles(table);
    List<String> currentPaths = new ArrayList<>(sizes.keySet());
    java.util.Set<String> currentSet = new java.util.HashSet<>(currentPaths);
    IndexMeta priorBloomMeta = BloomLeaf.readMeta(indexIo, indexRoot, target.fieldId());
    boolean incremental = decideIncremental(target, prior, priorBloomMeta);
    Reconciled reconciled = reconcileCoverage(prior.coverage(), currentSet, incremental);
    java.util.Map<Integer, Integer> ordinalRemap = reconciled.ordinalRemap();
    NewFiles files = selectNewFiles(currentPaths, reconciled.coverage(), incremental, maxNewFiles);
    List<Coverage.Entry> coverage = files.coverage();
    List<String> newPaths = files.newPaths();
    final boolean partial = files.remaining() > 0;

    // ordinalRemap == null is load-bearing, not defensive. This shortcut rewrites the METADATA and
    // nothing else, which is right only when the snapshot pointer is all that changed. A compacting
    // build has RENUMBERED coverage, and the bitmaps that coverage indexes are translated only by
    // the merge below -- so taking this path after compacting would publish renumbered coverage
    // against untranslated bitmaps, and a live path would resolve to whichever file used to hold
    // its number. A compacting build therefore does the full merge even with nothing new to read.
    if (incremental && newPaths.isEmpty() && ordinalRemap == null) {
      return restampOnly(target, listener, lease, prior.termMeta(), priorBloomMeta, coverage,
          reconciled.departedPaths(), currentPaths, metrics);
    }

    com.fasterxml.jackson.databind.JsonNode termSnapshot =
        prior.termMeta() == null ? null : TermIndexWriter.snapshotNode(prior.termMeta());
    GramState gramState =
        loadPriorGrams(target, config, metrics, incremental, termSnapshot, prior.covered());
    PriorAggregates probed =
        probePriorAggregates(target, incremental, termSnapshot, gramState, prior.covered(),
            newPaths, currentPaths);
    incremental = probed.incremental();
    Counting counting = decideCounting(listener, target.termEnabled());
    // The registry, not the three by name: every type gets its per-build collector here, and only
    // the ones that want rows survive into the read below.
    IndexType.Collector[] collectors =
        IndexTypes.collectors(
            new IndexType.BuildContext(
                table, column, target.fieldId(), snapshotId, target.contract(), target.gramRule(),
                target.gramEnabled(), target.termEnabled(), incremental));

    // the incremental-vs-full decision is final here; only now may the listener learn buildKind
    IndexBuildListener.BuildContext watch =
        listener
            .start(prefix, namespace, tableName, column, snapshotId,
                incremental ? IndexBuildListener.BuildKind.INCREMENTAL : IndexBuildListener.BuildKind.FULL,
                prior.priorCovered(), target.gramRule());

    // O(FILES) FOR THE WHOLE BUILD: every bloom stays here until the leaf is written at the end.
    // Bounded explicitly by BLOOM_BUILD_MAX_BYTES rather than left to find the heap.
    Map<String, NgramBloom> blooms = new LinkedHashMap<>();
    // ALLOCATED, not positional: one past the highest ordinal ever issued, counting dead slots.
    // Reusing a dead ordinal would hand a new file the bitmaps of the file that used to hold it.
    int startOrdinal = Coverage.nextOrdinal(coverage);
    Schema projection = table.schema().select(column);
    // From BuildConfig rather than straight from the environment, so a caller can vary it.
    int threads = Math.max(1, config.indexThreads());
    java.util.concurrent.ExecutorService pool = java.util.concurrent.Executors.newFixedThreadPool(threads);
    translateGramOrdinals(gramState, ordinalRemap);
    gramState.map = gramValve(gramState.map, gramState.keyBytes, config, metrics);
    int arenaCount = planArenas(config, threads, newPaths.size());
    long buildStart = System.nanoTime();
    try (TermRunStore store = new TermRunStore(config.termBuildDir(), config.termBuildMaxSpillBytes(), metrics)) {
      RunBuffer[] arenas = newArenas(config, store, arenaCount, target.termEnabled());
      List<java.nio.file.Path> runs = new ArrayList<>();
      ReadPassResult read;
      try {
        read =
            readPass(target, config, metrics, watch, counting, newPaths, sizes, blooms, gramState,
                probed.rowsTotal(), probed.tokenTotal(), startOrdinal, projection, arenas, pool,
                buildStart, collectors);
      } finally {
        stopReaders(pool);
        drainArenas(arenas, runs);
      }
      return publish(target, config, listener, lease, watch, prior, priorBloomMeta, incremental,
          coverage, newPaths, reconciled.departedPaths(), probed.leaves(), ordinalRemap, gramState,
          blooms, read, runs, store, startOrdinal, partial, files.remaining(), metrics);
    }
  }

  /**
   * Resolves what this build indexes: the field and its id, the contracts it is cut under, and the
   * tier flags every later step reads instead of the deployment defaults.
   */
  private static BuildTarget resolveTarget(
      Table table, String column, BuildConfig config, org.apache.iceberg.io.FileIO indexIo,
      String indexRoot, long snapshotId, String prefix, String namespace, String tableName,
      long startedMs) {
    var field = table.schema().findField(column);
    // IcebergKinds.shapeOf(type).kind(), not IcebergKinds.of(type): a list or a map is admitted
    // on the kind of what it HOLDS, so list<string> resolves exactly where string does and
    // list<struct> is refused exactly where struct is.
    if (field == null || !Canonical.indexable(IcebergKinds.shapeOf(field.type()).kind())) {
      throw new IllegalArgumentException(
          "column must exist and be a string, integer, long, uuid, binary or fixed -- or a list or "
              + "map of one: " + column);
    }
    // Every ANCESTOR must be a struct. The addressed node itself may now be a list or a map --
    // that is the repeated-field support -- but a path THROUGH one stays refused: tags.element is
    // an ordinary STRING NestedField, and indexing it would key an artifact at a field id the
    // pruner cannot reason about (IcebergKinds.repeatedPath). Artifacts are keyed by the addressed
    // field's id, which Iceberg assigns to nested fields too, so nothing downstream tells attrs.msg
    // from a top-level msg except the id.
    requireStructPath(table.schema(), field, column);
    int fieldId = field.fieldId();
    // The resolved analyzer goes into the analyzer id, so an index always records the contract it
    // was built under and a reader never guesses. Changing it for a column means rebuilding that
    // column: the old index is refused, not reinterpreted.
    Analyzer.Contract contract = IndexSettings.contract(table, column, config);
    Grams.Contract gramRule = IndexSettings.grams(table, column, config);
    // Tier participation is resolved ONCE, here, and every step below reads these flags rather
    // than the deployment ones, so no two sites can disagree about whether a tier is on.
    boolean termEnabled = tierEnabled(table, column, "term-index", config.format().termIndexEnabled());
    boolean gramEnabled = tierEnabled(table, column, "gram-index", config.format().gramIndexEnabled());
    return new BuildTarget(table, indexIo, indexRoot, column, fieldId, snapshotId, prefix,
        namespace, tableName, contract, gramRule, termEnabled, gramEnabled, startedMs);
  }

  /**
   * Reads the prior generation's term metadata and the coverage, per-range counts and live paths it
   * carries; a null metadata node means there is no prior generation.
   */
  private static PriorIndex readPriorIndex(BuildTarget target) {
    // prior coverage: the term metadata's file list is the source of truth. Entries carry their
    // own ordinal, so this list's ORDER no longer means anything -- see Coverage.
    com.fasterxml.jackson.databind.JsonNode termMeta =
        readTermMeta(target.indexIo(), target.indexRoot(), target.fieldId());
    if (termMeta != null && TermIndexWriter.newerThanThisReader(termMeta)) {
      // Never build over a newer index: the incremental path would republish it in this kahshe's
      // layout, downgrading it in place under a reader that cannot tell.
      throw new IllegalStateException(
          "term index format-version " + termMeta.path("format-version").asInt(1)
              + " / kahshe.format-version " + TermIndexWriter.tierVersionOf(termMeta)
              + " is newer than this kahshe's " + TermIndexWriter.FORMAT_VERSION + " / "
              + TermIndexWriter.TIER_FORMAT_VERSION + "; refusing to build over it");
    }
    List<Coverage.Entry> coverage =
        termMeta == null
            ? new ArrayList<>()
            : new ArrayList<>(Coverage.parse(TermIndexWriter.snapshotNode(termMeta).path("files")));
    // The prior generation's per-range term counts, which let an untouched range be carried
    // forward with its count; absent (older generations) means every range is rewritten once.
    List<Long> priorTermsPerRange = null;
    if (termMeta != null) {
      com.fasterxml.jackson.databind.JsonNode node =
          TermIndexWriter.snapshotNode(termMeta).path("terms-per-range");
      if (node.isArray() && node.size() == TermRanges.COUNT) {
        List<Long> parsed = new ArrayList<>();
        node.forEach(n -> parsed.add(n.asLong()));
        priorTermsPerRange = parsed;
      }
    }
    List<String> covered = Coverage.livePaths(coverage);

    // captured before the fallback below may clear coverage; listeners need the prior view
    java.util.Set<String> priorCovered = java.util.Set.copyOf(covered);
    return new PriorIndex(termMeta, coverage, priorTermsPerRange, covered, priorCovered);
  }

  /** Plans the table's in-scope data files with their sizes, keyed by location. */
  private static Map<String, Long> scanCurrentFiles(Table table) throws IOException {
    // SORTED BY LOCATION, and that is correctness, not tidiness. An ordinal is allocated and
    // recorded per entry rather than being a position in this list (see Coverage), but sorting
    // still governs the ORDER NEW FILES ARE NUMBERED IN. Iceberg plans manifests concurrently, so
    // planFiles() varies its order between runs, and without sorting two full builds of one table
    // would assign its files different ordinals -- after which a bitmap names the wrong files, and
    // naming the wrong files means pruning away files that match.
    //
    // Sorting keeps a FULL build's numbering a pure function of the file set; allocation keeps an
    // INCREMENTAL build's numbering stable against removals. Neither substitutes for the other.
    //
    // The scan is FILTERED by the table's declared index scope, so Iceberg prunes partitions
    // before a single data file is read. Partial coverage is safe: an unindexed file is kept,
    // never skipped.
    org.apache.iceberg.expressions.Expression scope = IndexScope.of(table);
    Map<String, Long> sizes = new java.util.TreeMap<>();
    try (CloseableIterable<FileScanTask> tasks = table.newScan().filter(scope).planFiles()) {
      for (FileScanTask task : tasks) {
        sizes.put(task.file().location(), task.file().fileSizeInBytes());
      }
    }
    return sizes;
  }

  /** Whether this build may patch the prior generation rather than re-read every file. */
  private static boolean decideIncremental(
      BuildTarget target, PriorIndex prior, IndexMeta priorBloomMeta) {
    com.fasterxml.jackson.databind.JsonNode termMeta = prior.termMeta();
    List<Coverage.Entry> coverage = prior.coverage();
    boolean termEnabled = target.termEnabled();
    Analyzer.Contract contract = target.contract();
    Grams.Contract gramRule = target.gramRule();
    // A TIER THAT IS ENABLED BUT ABSENT FORCES A FULL REBUILD. Coverage says which FILES were
    // read, not which tiers were written from them. An index built with the term tier off covers
    // every file, so turning the tier back on would otherwise take the incremental path, find
    // nothing new to do, restamp the metadata and publish no term index at all -- silently, and
    // reported as a success.
    boolean termTierMissing =
        termEnabled
            && termMeta != null
            && TermIndexWriter.aggregateLeaves(TermIndexWriter.snapshotNode(termMeta)).stream()
                .allMatch(leaf -> leaf == null || leaf.isEmpty());
    if (termTierMissing) {
      LOG.info(
          "term index is enabled but the existing index has no aggregate; rebuilding in full");
    }
    // A DEPARTED FILE DOES NOT force a full rebuild: ordinals are allocated, not positional, so a
    // file that left is tombstoned in place and every surviving bitmap still names what it named
    // before. Rolling retention stays an ordinary incremental build.
    //
    // A CHANGED ANALYZER DOES force one. Every publish stamps the CURRENT analyzer id, so an
    // incremental build over an index cut under another contract would relabel it while the old
    // files' tokens under the new contract were never written. The reader takes the contract from
    // the index, so it would probe for such a token, find it absent, and prune exactly the files
    // that hold it. The prior aggregate cannot be patched: the tokens it lacks were never
    // tokenized.
    String priorAnalyzer =
        termMeta == null
            ? null
            : TermIndexWriter.propertiesNode(termMeta).path("analyzer").asText(null);
    boolean analyzerChanged =
        priorAnalyzer != null && !priorAnalyzer.equals(contract.id());
    // The gram rule too: grams cut under another rule or size cannot be patched any more than
    // tokens can. A document carrying no gram property was cut under the earliest rule.
    String priorGrams =
        termMeta == null
            ? null
            : Grams.Contract.of(TermIndexWriter.propertiesNode(termMeta).path("grams").asText(null)).id();
    boolean gramsChanged = priorGrams != null && !priorGrams.equals(gramRule.id());
    if (gramsChanged) {
      LOG.info("gram rule changed ({} -> {}); rebuilding in full so every file is cut under the new rule",
          priorGrams, gramRule.id());
    }
    if (analyzerChanged) {
      LOG.info(
          "term index analyzer changed ({} -> {}); rebuilding in full so every file is tokenized "
              + "under the new contract",
          priorAnalyzer, contract.id());
    }
    boolean incremental =
        !coverage.isEmpty() && priorBloomMeta != null && !termTierMissing && !analyzerChanged
            && !gramsChanged;
    // The leaves are probed HERE, before anything is derived from the answer. Probed later --
    // after coverage and the new-file list have been computed under incremental=true -- an
    // unreadable leaf would have to unwind that state, and a partial unwind leaves two live
    // ordinals for one path, persisting across every later build. Incrementality is decided once;
    // a surprise after this point is an error, not a rewrite.
    if (incremental && termEnabled && termMeta != null) {
      try {
        for (String leaf : TermIndexWriter.aggregateLeaves(TermIndexWriter.snapshotNode(termMeta))) {
          if (leaf != null && !leaf.isEmpty()) {
            TermIndexWriter.probeAggregate(target.indexIo(), leaf);
          }
        }
      } catch (IOException | RuntimeException e) {
        // prior leaves unreadable (moved/cleaned): a full rebuild self-heals, a retry never would
        LOG.warn("prior index leaves unreadable ({}); falling back to full rebuild", e.toString());
        incremental = false;
      }
    }
    return incremental;
  }

  /**
   * Tombstones what left, revives what came back, and past the threshold renumbers the survivors
   * into a contiguous ordinal space.
   */
  private static Reconciled reconcileCoverage(
      List<Coverage.Entry> coverage, java.util.Set<String> currentSet, boolean incremental) {
    // Tombstone what left, revive what came back. A revived path is NOT re-read: Iceberg data
    // files are immutable, so the bitmaps naming its ordinal still describe it exactly.
    int departed = 0;
    int revived = 0;
    List<String> departedPaths = new ArrayList<>();
    if (incremental) {
      for (Coverage.Entry entry : coverage) {
        boolean nowLive = currentSet.contains(entry.path());
        if (nowLive != entry.live()) {
          if (nowLive) {
            revived++;
          } else {
            departed++;
            departedPaths.add(entry.path());
          }
        }
      }
      coverage = Coverage.reconcile(coverage, currentSet);
      if (departed > 0 || revived > 0) {
        // Logged because a tombstone changes what the index covers without moving any counter,
        // and "the index covers less than you think" is otherwise invisible.
        LOG.info(
            "coverage reconciled against the current snapshot: {} file(s) departed and were "
                + "tombstoned, {} revived; ordinals are unchanged and no bitmap was rewritten",
            departed, revived);
      }
    }

    // TOMBSTONE COMPACTION. Past the threshold the survivors are renumbered contiguously and the
    // dead entries dropped. Every bitmap in the gram and term tiers must be translated through the
    // same remap in THIS build: coverage and bitmaps that disagree about what a number means is a
    // false negative. Blooms are unaffected -- they are keyed by path, not by ordinal.
    java.util.Map<Integer, Integer> ordinalRemap = null;
    int deadEntries = coverage.size() - Coverage.livePaths(coverage).size();
    if (incremental
        && deadEntries >= TOMBSTONE_COMPACT_MIN_DEAD
        && Coverage.deadPercent(coverage) >= TOMBSTONE_COMPACT_DEAD_PERCENT) {
      Coverage.Renumbered renumbered = Coverage.renumberLive(coverage);
      LOG.info(
          "compacting tombstoned coverage: {} of {} entries were dead; renumbering {} survivors "
              + "and translating every bitmap",
          coverage.size() - renumbered.entries().size(), coverage.size(),
          renumbered.entries().size());
      ordinalRemap = renumbered.remap();
      coverage = new ArrayList<>(renumbered.entries());
    }
    return new Reconciled(coverage, departedPaths, ordinalRemap);
  }

  /** The in-scope files no coverage entry names, truncated to a checkpoint's file budget. */
  private static NewFiles selectNewFiles(
      List<String> currentPaths, List<Coverage.Entry> coverage, boolean incremental,
      int maxNewFiles) {
    List<String> newPaths = new ArrayList<>();
    java.util.Set<String> coveredSet =
        incremental
            ? coverage.stream().map(Coverage.Entry::path).collect(java.util.stream.Collectors.toSet())
            : java.util.Set.of();
    for (String path : currentPaths) {
      if (!coveredSet.contains(path)) {
        newPaths.add(path);
      }
    }
    if (!incremental) {
      // a full build renumbers from zero, so nothing prior survives to be tombstoned
      coverage = new ArrayList<>();
    }
    // A checkpointed pass reads only the first maxNewFiles of the sorted new files and reports
    // how many remain; the wrapper loops. Coverage stays a prefix of the sorted file set.
    int remaining = 0;
    if (maxNewFiles > 0 && newPaths.size() > maxNewFiles) {
      remaining = newPaths.size() - maxNewFiles;
      newPaths = new ArrayList<>(newPaths.subList(0, maxNewFiles));
    }
    return new NewFiles(coverage, newPaths, remaining);
  }

  /**
   * The nothing-to-read path: republishes the prior generation's metadata against the current
   * snapshot, carrying any tombstones with it.
   */
  private static long[] restampOnly(
      BuildTarget target, IndexBuildListener listener, BuildLease lease,
      com.fasterxml.jackson.databind.JsonNode termMeta, IndexMeta priorBloomMeta,
      List<Coverage.Entry> coverage, List<String> departedPaths, List<String> currentPaths,
      io.kahshe.common.Metrics metrics)
      throws IOException {
    // SAY SO: a build with nothing to do and a build that indexed everything otherwise report
    // themselves identically, and an index scope widened to a threshold that happens to add no
    // files reads as a failure rather than as the no-op it is.
    int liveCount = Coverage.livePaths(coverage).size();
    if (IndexScope.isNarrowed(target.table())) {
      LOG.info(
          "nothing to do: the index scope selects {} data file(s), all {} of which are already "
          + "covered. Widen kahshe.index.scope to index more.",
          currentPaths.size(), liveCount);
    } else {
      LOG.info(
          "nothing to do: all {} data files in the table are already covered; restamping the "
          + "snapshot pointer only.",
          liveCount);
    }
    // coverage already complete: just restamp the snapshot, carrying any tombstones with it.
    // A restamp publishes too, so it makes the same last check before its first write.
    listener.beforePublish();
    if (lease != null) {
      lease.assertStillHeld();
    }
    priorBloomMeta.snapshotId = target.snapshotId();
    priorBloomMeta.timestampMs = System.currentTimeMillis();
    BloomLeaf.writeMeta(
        target.indexIo(), IndexMeta.metaPath(target.indexRoot(), target.fieldId()),
        priorBloomMeta);
    restampTermMeta(target.indexIo(), target.indexRoot(), target.fieldId(), termMeta,
        target.snapshotId(), coverage);
    writeReport(target.indexIo(), target.indexRoot(), target.fieldId(), target.prefix(),
        target.namespace(), target.tableName(), target.column(), target.snapshotId(),
        BuildReport.Kind.RESTAMP, target.contract(), target.gramRule(),
        target.startedMs(), List.of(), departedPaths, priorBloomMeta.indexBytes, 0,
        false, List.of(), metrics);
    return new long[] {target.snapshotId(), priorBloomMeta.indexBytes, liveCount, 0};
  }

  /**
   * Loads the prior gram layer into the map this build extends, or restarts its coverage when the
   * prior leaf is absent, too large to hold, or unreadable.
   */
  private static GramState loadPriorGrams(
      BuildTarget target, BuildConfig config, io.kahshe.common.Metrics metrics, boolean incremental,
      com.fasterxml.jackson.databind.JsonNode termSnapshot, List<String> covered) {
    Map<GramIndexWriter.ByteKey, org.roaringbitmap.RoaringBitmap> gramMap =
        target.gramEnabled() ? new java.util.HashMap<>() : null;
    long gramKeyBytes = 0;
    int gramFromOrdinal = 0;
    if (incremental && gramMap != null) {
      String priorGramsLeaf = termSnapshot.path("leaves").path("grams").asText();
      // The writer records the leaf's size, so its heap cost is REFUSED BEFORE IT IS PAID. The
      // valve further down cannot stand in for this check: it runs after readLeaf has already
      // deserialized the whole leaf into the map, by which time the spike has happened.
      long priorGramBytes = termSnapshot.path("gram-coverage").path("bytes").asLong();
      boolean priorGramsTooLarge =
          !priorGramsLeaf.isEmpty()
              && GRAM_LEAF_HEAP_FACTOR * priorGramBytes > config.gramBuildMaxBytes();
      if (priorGramsLeaf.isEmpty()) {
        gramFromOrdinal = covered.size(); // prior build carried no gram layer: cover from here on
      } else if (priorGramsTooLarge) {
        // Coverage RESTARTS here rather than the layer being dropped outright: the new files still
        // get an exact gram layer, and the files before this ordinal fall back to blooms, which is
        // what from-ordinal means. Dropping it entirely would give up pruning the build could
        // still provide. Same degradation the unreadable-leaf path takes, for a different reason.
        metrics.gramTooLarge.increment();
        LOG.warn(
            "prior grams leaf is {} MiB, and loading it would cost about {} MiB against "
                + "KAHSHE_GRAM_BUILD_MAX_BYTES={} MiB; NOT reading it. Gram coverage restarts at "
                + "ordinal {} and earlier files serve from blooms. Raise the cap to keep the "
                + "existing coverage.",
            priorGramBytes >> 20, (GRAM_LEAF_HEAP_FACTOR * priorGramBytes) >> 20,
            config.gramBuildMaxBytes() >> 20, covered.size());
        gramFromOrdinal = covered.size();
      } else {
        try {
          GramIndexWriter.readLeaf(target.indexIo(), priorGramsLeaf, gramMap);
          gramFromOrdinal = termSnapshot.path("gram-coverage").path("from-ordinal").asInt();
          for (GramIndexWriter.ByteKey gramKey : gramMap.keySet()) {
            gramKeyBytes += gramKey.bytes.length + GRAM_ENTRY_OVERHEAD_BYTES;
          }
        } catch (IOException | RuntimeException e) {
          // the gram layer degrades alone — never force a re-read of already-covered data files
          LOG.warn("prior grams leaf unreadable; gram coverage restarts at ordinal {}",
              covered.size(), e);
          gramMap.clear();
          gramKeyBytes = 0;
          gramFromOrdinal = covered.size();
        }
      }
    }
    GramState state = new GramState();
    state.map = gramMap;
    state.keyBytes = gramKeyBytes;
    state.fromOrdinal = gramFromOrdinal;
    return state;
  }

  /**
   * Probes the prior aggregate leaves and carries their totals forward; an unreadable leaf falls
   * the build back to reading every file again.
   */
  private static PriorAggregates probePriorAggregates(
      BuildTarget target, boolean incremental,
      com.fasterxml.jackson.databind.JsonNode termSnapshot, GramState gramState,
      List<String> covered, List<String> newPaths, List<String> currentPaths)
      throws IOException {
    long rowsTotal = 0;
    long tokenTotal = 0;
    Map<GramIndexWriter.ByteKey, org.roaringbitmap.RoaringBitmap> gramMap = gramState.map;
    long gramKeyBytes = gramState.keyBytes;
    int gramFromOrdinal = gramState.fromOrdinal;
    // an incremental build's prior aggregate is not read into heap — it joins the accumulator's
    // merge at finish() as one more sorted source — but it is PROBED here, so an unreadable one
    // still costs a full rebuild rather than a failure after every data file has been read
    List<String> priorAggregates = null;
    if (incremental) {
      try {
        priorAggregates = TermIndexWriter.aggregateLeaves(termSnapshot);
        for (String leaf : priorAggregates) {
          if (leaf != null && !leaf.isEmpty()) {
            // empty ranges have no object to probe
            TermIndexWriter.probeAggregate(target.indexIo(), leaf);
          }
        }
        rowsTotal = termSnapshot.path("totals").path("rows").asLong();
        tokenTotal = termSnapshot.path("totals").path("tokens").asLong();
      } catch (IOException | RuntimeException e) {
        // decideIncremental probed these same leaves and found them readable; losing them between
        // that probe and this one is a race with a cleaner, and the honest answer is to fail this
        // build -- the next observation decides afresh. Rewriting the build's state here is what
        // used to publish two live ordinals for one path.
        throw new IOException("prior index leaves became unreadable after the build decided to be "
            + "incremental; not rewriting coverage mid-build", e);
      }
    }
    gramState.map = gramMap;
    gramState.keyBytes = gramKeyBytes;
    gramState.fromOrdinal = gramFromOrdinal;
    return new PriorAggregates(incremental, priorAggregates, rowsTotal, tokenTotal);
  }

  /** Whether the build tokenizes at all, and whether a listener wants whole-file counts. */
  private static Counting decideCounting(IndexBuildListener listener, boolean termEnabled) {
    // Tokenizing is only worth doing if something reads the counts: the term dictionary, or a
    // watch rule that matches on tokens. With neither it is the most expensive thing in the build
    // and the result is discarded.
    //
    // NOT "listener != null": IndexBuildListener.NONE is a non-null no-op, so identity is what
    // separates a build with a live listener from one without.
    boolean listenerReadsCounts =
        listener != IndexBuildListener.NONE && listener.readsTermCounts();
    boolean countTerms =
        termEnabled
            || listenerReadsCounts;
    LOG.info(
        "term counts: {} (term index {}, listener {})",
        countTerms ? "computed" : "skipped -- nothing reads them",
        termEnabled ? "on" : "off",
        listener == IndexBuildListener.NONE ? "none" : "present");
    return new Counting(countTerms, listenerReadsCounts);
  }

  /** Translates the gram tier's bitmaps and its from-ordinal into the renumbered ordinal space. */
  private static void translateGramOrdinals(
      GramState gramState, java.util.Map<Integer, Integer> ordinalRemap) {
    Map<GramIndexWriter.ByteKey, org.roaringbitmap.RoaringBitmap> gramMap = gramState.map;
    int gramFromOrdinal = gramState.fromOrdinal;
    // The gram tier's bitmaps are ordinals too, so they are translated in the same build that
    // renumbers coverage -- including from-ordinal, which is a threshold in that same space. A
    // gram bitmap left in the old numbering would answer "this file has no such gram" about a file
    // that does, and the gram layer's answer is FINAL inside its coverage: it prunes outright.
    if (ordinalRemap != null) {
      if (gramMap != null) {
        for (Map.Entry<GramIndexWriter.ByteKey, org.roaringbitmap.RoaringBitmap> e : gramMap.entrySet()) {
          org.roaringbitmap.RoaringBitmap translated = new org.roaringbitmap.RoaringBitmap();
          final java.util.Map<Integer, Integer> remap = ordinalRemap;
          e.getValue().forEach((org.roaringbitmap.IntConsumer) old -> {
            Integer to = remap.get(old);
            if (to != null) {
              translated.add(to);
            }
          });
          e.setValue(translated);
        }
        gramMap.values().removeIf(org.roaringbitmap.RoaringBitmap::isEmpty);
      }
      // Coverage started somewhere in the old space; after renumbering the survivors are 0..n-1,
      // and every one of them is still covered by whatever the gram layer covered before.
      Integer movedFrom = ordinalRemap.get(gramFromOrdinal);
      gramFromOrdinal = movedFrom != null ? movedFrom : 0;
    }
    gramState.fromOrdinal = gramFromOrdinal;
  }

  /** Sizes the reader arenas and says what the build committed, before a file is opened. */
  private static int planArenas(BuildConfig config, int threads, int fileCount) {
    // Never more arenas than there are files to read. The arena is ALLOCATED, not grown, so a
    // three-file table with eight readers would otherwise commit eight arenas to hold three files.
    int arenaCount = Math.max(1, Math.min(threads, fileCount));
    LOG.info(
        "term arena: {} MiB x {} arena(s) = {} MiB allocated for {} reader(s), {} file(s) "
            + "(heap {} MiB)",
        config.termBufferBytes() / (1024 * 1024), arenaCount,
        config.termBufferBytes() * arenaCount / (1024 * 1024), threads, fileCount,
        Runtime.getRuntime().maxMemory() / (1024 * 1024));
    return arenaCount;
  }

  /** Probes the run store and allocates one arena per reader. */
  private static RunBuffer[] newArenas(
      BuildConfig config, TermRunStore store, int arenaCount, boolean termEnabled)
      throws IOException {
    // ONE ARENA PER READER, reused across data files, which is why every record carries its own
    // ordinal. Peak heap for the term tier is readers x (arena + offset index): a count of
    // fixed-size things, allocated here, before a single file is opened. A growable structure
    // bounded by a guess about its per-entry cost cannot be bounded at all, so there is none.
    //
    // Driven by the TIER, never by whether a listener wants counts. Those are independent needs --
    // a watch rule wants one file's counts in a map, the index wants every file's terms in a run,
    // and a build with both does both. Gating the runs on a listener would publish an EMPTY term
    // aggregate for any table with a watch rule on the indexed column: no error, no metric, and
    // every match query then prunes the whole table.
    boolean writeRuns = termEnabled;
    if (writeRuns) {
      // Every build that writes runs touches local disk, so an unwritable scratch directory is
      // found here rather than at the first flush, which would waste every file read so far.
      store.probe();
    }
    RunBuffer[] arenas = new RunBuffer[arenaCount];
    if (writeRuns) {
      for (int slot = 0; slot < arenaCount; slot++) {
        arenas[slot] = new RunBuffer(config.termBufferBytes(), store, slot);
      }
    }
    return arenas;
  }

  /**
   * The read pass, one wave of concurrent readers at a time — the read is latency-bound on object
   * storage — with ordinal order restored as each wave is collected.
   *
   * <p>Each wave's {@code FileTerms} are consumed and released, which bounds the TERM path's build
   * memory at any file count. It does NOT bound the bloom map, which is retained for the whole
   * build and guarded separately by {@link #BLOOM_BUILD_MAX_BYTES}.
   */
  private static ReadPassResult readPass(
      BuildTarget target, BuildConfig config, io.kahshe.common.Metrics metrics,
      IndexBuildListener.BuildContext watch, Counting counting, List<String> newPaths,
      Map<String, Long> sizes, Map<String, NgramBloom> blooms, GramState gramState,
      long rowsTotal, long tokenTotal, int startOrdinal, Schema projection, RunBuffer[] arenas,
      java.util.concurrent.ExecutorService pool, long buildStart,
      IndexType.Collector[] collectors)
      throws IOException {
    Map<GramIndexWriter.ByteKey, org.roaringbitmap.RoaringBitmap> gramMap = gramState.map;
    long gramKeyBytes = gramState.keyBytes;
    long bloomHeapBytes = 0;
    long dataBytes = 0;
    int saturated = 0;
    // NO SHARED STATE AND NO LOCK. A reader appends into its own arena and writes its own run
    // files; two readers never touch the same object, so there is nothing to serialize on. A
    // shared accumulator between them -- even one striped and locked per stripe -- leaves the
    // drain around it serial, and readers park while most of the cores idle.
    int filesDone = 0;
    int saturatedFiles = 0;
    String saturatedExample = null;
    for (int at = 0; at < newPaths.size(); ) {
      int width = Math.min(arenas.length, newPaths.size() - at);
      List<String> wave = newPaths.subList(at, at + width);
      List<java.util.concurrent.Future<Object[]>> futures = new ArrayList<>(width);
      for (int i = 0; i < width; i++) {
        final String path = wave.get(i);
        final int ordinal = startOrdinal + at + i;
        // slot i of the wave, so a reader's arena is reused across the files it handles rather
        // than allocated per file. A wave never has two tasks at the same slot, and the next
        // wave starts only after this one is collected, so no two threads share an arena.
        final RunBuffer arena = arenas[i];
        futures.add(
            pool.submit(
                () ->
                    readFile(
                        target.table(), projection, target.column(), path, config,
                        counting.countTerms(), counting.listenerReadsCounts(), ordinal,
                        arena, collectors)));
      }
      for (int i = 0; i < width; i++) {
        Object[] result = collect(futures.get(i), wave.get(i));
        TermIndexWriter.FileTerms terms = (TermIndexWriter.FileTerms) result[0];
        rowsTotal += terms.rowCount;
        tokenTotal += terms.tokenTotal;
        NgramBloom builtBloom = (NgramBloom) result[1];
        blooms.put(wave.get(i), builtBloom);
        // path chars + map node + the bloom's own long[] and header
        bloomHeapBytes += builtBloom.sizeBytes() + 2L * wave.get(i).length() + 64;
        if (bloomHeapBytes > BLOOM_BUILD_MAX_BYTES) {
          throw new IllegalStateException(String.format(
              "the per-file bloom map has reached %d MiB after %d files, past the %d MiB cap "
                  + "(kahshe.bloom.build.max.bytes). Every file's bloom is held until the leaf "
                  + "is written, so this grows with the table's file count. Raise the cap if "
                  + "the heap can take it, narrow kahshe.index.scope, or compact the table's "
                  + "data files. The existing index is untouched and still correct.",
              bloomHeapBytes >> 20, blooms.size(), BLOOM_BUILD_MAX_BYTES >> 20));
        }
        dataBytes += sizes.getOrDefault(wave.get(i), 0L);
        @SuppressWarnings("unchecked")
        Set<String> grams = (Set<String>) result[2];
        watch.file(wave.get(i), terms, grams);
        if (gramSpaceSaturated(grams, target.gramRule())) {
          saturated++;
          saturatedFiles++;
          if (saturatedExample == null) {
            saturatedExample = wave.get(i);
          }
          metrics.gramSaturatedFiles.increment();
        }

        if (gramMap != null) {
          int ordinal = startOrdinal + at + i;
          for (String gram : grams) {
            GramIndexWriter.ByteKey gramKey = GramIndexWriter.ByteKey.of(gram);
            org.roaringbitmap.RoaringBitmap bitmap = gramMap.get(gramKey);
            if (bitmap == null) {
              bitmap = new org.roaringbitmap.RoaringBitmap();
              gramMap.put(gramKey, bitmap);
              gramKeyBytes += gramKey.bytes.length + GRAM_ENTRY_OVERHEAD_BYTES;
            }
            bitmap.add(ordinal);
          }
        }
        metrics.indexDataFilesRead.increment();
        filesDone++;
        LOG.info(
            "index build {} f{}: {}/{} data files, {} rows, {} MiB of source read, {}s elapsed",
            target.column(), target.fieldId(), filesDone, newPaths.size(), rowsTotal,
            dataBytes / (1024 * 1024),
            String.format("%.1f", (System.nanoTime() - buildStart) / 1e9));
      }
      gramMap = gramValve(gramMap, gramKeyBytes, config, metrics);
      at += width;
    }
    warnSaturated(target.column(), saturatedFiles, newPaths.size(), saturatedExample);
    gramState.map = gramMap;
    gramState.keyBytes = gramKeyBytes;
    return new ReadPassResult(rowsTotal, tokenTotal, dataBytes, saturated);
  }

  /** Says how many of this pass's files saturate their alphabet's trigram space. */
  private static void warnSaturated(
      String column, int saturatedFiles, int fileCount, String saturatedExample) {
    // Said HERE because it cannot be seen anywhere else: a saturated file is served correctly --
    // every probe keeps it -- so the query-side symptom is indistinguishable from data that is
    // simply not selective, and only the build has the gram sets in hand.
    if (saturatedFiles > 0) {
      LOG.warn(
          "column {}: {} of {} new files' gram sets saturate their alphabet's trigram space "
              + "(e.g. {}); the gram and bloom tiers will prune little on this column, though "
              + "the term dictionary still answers match exactly. Consider "
              + "kahshe.index.{}.gram-index=false and a higher kahshe.index.{}.bloom-fpp to "
              + "stop paying for selectivity that is not there.",
          column, saturatedFiles, fileCount, saturatedExample, column, column);
    }
  }

  /** One reader's result, with a build failure raised as itself rather than as a read failure. */
  private static Object[] collect(java.util.concurrent.Future<Object[]> future, String path)
      throws IOException {
    try {
      return future.get();
    } catch (Exception e) {
      // Unwrap: a failure raised by the accumulator inside the sink is a BUILD failure --
      // a budget exceeded, a spill that could not be written -- and wrapping it as "failed
      // reading <file>" hides the only sentence that says what to change.
      Throwable cause = e instanceof java.util.concurrent.ExecutionException ? e.getCause() : e;
      if (cause instanceof IOException io && !(cause instanceof java.io.UncheckedIOException)) {
        throw io;
      }
      throw new IOException("failed reading " + path, cause);
    }
  }

  /** Waits for the readers to stop, rather than merely signalling them. */
  private static void stopReaders(java.util.concurrent.ExecutorService pool) {
    // WAIT for the readers, do not merely signal them. A reader still running past this point
    // would write into an arena the merge is about to consume, and would recreate the scratch
    // directory the store's close has just removed.
    pool.shutdownNow();
    try {
      if (!pool.awaitTermination(30, java.util.concurrent.TimeUnit.SECONDS)) {
        LOG.warn("index readers did not stop within 30s; draining their arenas anyway");
      }
    } catch (InterruptedException e) {
      Thread.currentThread().interrupt();
    }
  }

  /** Closes every arena and collects the runs it flushed, residue included. */
  private static void drainArenas(RunBuffer[] arenas, List<java.nio.file.Path> runs)
      throws IOException {
    // THE RESIDUE. An arena is flushed when it fills, and whatever it still holds when the corpus
    // runs out has nowhere else to go -- lose it and the last file each reader touched contributes
    // nothing to the index, which is that file pruned away for every term only it holds. This
    // happens after the readers are joined and before the merge, never in the reader task: the
    // arena outlives the task by design.
    for (RunBuffer arena : arenas) {
      if (arena != null) {
        arena.close();
        runs.addAll(arena.runs());
      }
    }
  }

  /** The publish: the bloom leaf, the term-index finish, and the build report, in that order. */
  private static long[] publish(
      BuildTarget target, BuildConfig config, IndexBuildListener listener, BuildLease lease,
      IndexBuildListener.BuildContext watch, PriorIndex prior, IndexMeta priorBloomMeta,
      boolean incremental, List<Coverage.Entry> coverage, List<String> newPaths,
      List<String> departedPaths, List<String> priorAggregates,
      java.util.Map<Integer, Integer> ordinalRemap, GramState gramState,
      Map<String, NgramBloom> blooms, ReadPassResult read, List<java.nio.file.Path> runs,
      TermRunStore store, int startOrdinal, boolean partial, int remaining,
      io.kahshe.common.Metrics metrics)
      throws IOException {
    String priorBloomUuid = priorBloomMeta == null ? null : priorBloomMeta.uuid;
    // THE LAST CHECK BEFORE THE FIRST PUBLISH. Two builders that both believed they took the
    // lease resolve here: whichever wrote it last owns it, the other aborts with nothing written.
    listener.beforePublish();
    if (lease != null) {
      lease.assertStillHeld();
    }
    // Prior entries keep their ordinals verbatim -- that is the whole point -- and the files
    // just read are appended with the ordinals they were assigned above.
    List<Coverage.Entry> allCoverage = new ArrayList<>(coverage);
    for (int i = 0; i < newPaths.size(); i++) {
      allCoverage.add(new Coverage.Entry(newPaths.get(i), startOrdinal + i, true));
    }
    com.fasterxml.jackson.databind.JsonNode termMeta = prior.termMeta();
    String priorUuid = termMeta == null ? null : termMeta.path("uuid").asText(null);
    // The aggregate is gated on the OPERATOR'S FLAG, never on whether any run exists. "No runs"
    // and "the term tier is off" are different things: an incremental build that read no new
    // files produces no runs and must still republish the prior generation's rows, and a build
    // whose new files happen to hold no indexable token must not be read as a tier switched off.
    IndexType.PublishContext publish =
        new IndexType.PublishContext(
            target.table(), target.indexIo(), target.indexRoot(), target.column(),
            target.fieldId(), target.snapshotId(), target.contract(), target.gramRule(),
            incremental, partial, allCoverage, blooms, priorBloomMeta, priorBloomUuid,
            IndexSettings.probability(target.table(), target.column(), "bloom-fpp", FPP),
            target.termEnabled(), runs, store, priorAggregates, read.rowsTotal(),
            read.tokenTotal(), priorUuid, gramState.map, gramState.fromOrdinal, ordinalRemap,
            // Sticky: inexact from the first tombstone until a full rebuild re-reads the data.
            // Compaction removes the tombstones but not the departed files' occurrences from
            // total_count, so a flag keyed on tombstones would wrongly flip back to exact.
            !incremental || (priorCountsExact(termMeta) && !Coverage.hasTombstones(coverage)
                && ordinalRemap == null),
            config.indexThreads(), prior.termsPerRange());
    // In cost order, which is also write order: the bloom leaf, then the term finish, which
    // publishes the gram leaf and the metadata both tiers share.
    Map<String, IndexType.Leaves> published = new LinkedHashMap<>();
    for (IndexType type : IndexTypes.inCostOrder()) {
      published.put(type.key(), type.write(publish));
    }
    IndexType.Leaves bloomLeaves = published.get(BloomIndexType.KEY);
    long bloomBytes = bloomLeaves == null ? 0 : bloomLeaves.bytes();
    if (store.runsWritten() > 0) {
      LOG.info(
          "term aggregate merged from {} sorted run(s) holding {} bytes of local scratch "
              + "(KAHSHE_TERM_BUILD_MAX_SPILL_BYTES={})",
          store.runsWritten(), store.bytesWritten(), config.termBuildMaxSpillBytes());
    }
    watch.done();
    writeReport(target.indexIo(), target.indexRoot(), target.fieldId(), target.prefix(),
        target.namespace(), target.tableName(), target.column(), target.snapshotId(),
        incremental ? BuildReport.Kind.INCREMENTAL : BuildReport.Kind.FULL, target.contract(),
        target.gramRule(), target.startedMs(), newPaths, departedPaths, bloomBytes,
        read.saturated(), partial, watch.alerts(), metrics);
    return new long[] {
        target.snapshotId(), bloomBytes, blooms.size(), read.dataBytes(), remaining};
  }

  /**
   * The gram layer's build-memory valve, checked per chunk so it can never trip only after the
   * heap is already spent: past KAHSHE_GRAM_BUILD_MAX_BYTES the layer is dropped for this whole
   * build (no grams leaf, no gram-coverage — the table degrades to blooms), never resumed
   * mid-build. Estimate: key bytes + map slot per gram, plus each bitmap's serialized size.
   */
  private static Map<GramIndexWriter.ByteKey, org.roaringbitmap.RoaringBitmap> gramValve(
      Map<GramIndexWriter.ByteKey, org.roaringbitmap.RoaringBitmap> gramMap, long gramKeyBytes,
      BuildConfig config, io.kahshe.common.Metrics metrics) {
    if (gramMap == null) {
      return null;
    }
    // serializedSizeInBytes is the PAYLOAD, not what the object costs in heap: a RoaringBitmap
    // holds its containers in arrays with their own headers and slack, and measures well above its
    // serialized form. 1.5x is the same factor GramIndex already uses for its load-time refusal,
    // and erring high is the safe direction here -- see GRAM_ENTRY_OVERHEAD_BYTES.
    long estimate = gramKeyBytes;
    for (org.roaringbitmap.RoaringBitmap bitmap : gramMap.values()) {
      estimate += (long) (1.5 * bitmap.serializedSizeInBytes());
    }
    if (estimate <= config.gramBuildMaxBytes()) {
      return gramMap;
    }
    metrics.gramBuildsSkipped.increment();
    LOG.warn("gram map estimate {} bytes exceeds KAHSHE_GRAM_BUILD_MAX_BYTES={}; "
        + "skipping the gram layer for this build (blooms still serve)",
        estimate, config.gramBuildMaxBytes());
    return null;
  }


  /**
   * One file's read pass, on a reader thread. Returns {@code {FileTerms, NgramBloom, gram set}}.
   *
   * <p>When {@code arena} is non-null the file's tokens are appended straight into it, one record
   * per occurrence, rather than accumulated into a per-file map. That is what makes peak heap
   * independent of how many distinct terms a data file holds: a file of prose and a file of two
   * billion unique trace ids cost the same here.
   *
   * <p>{@code arena} is null only when the term tier is off; a build that still tokenizes then has
   * a listener reading counts, and {@code needsFileMap} serves that through the per-file map
   * instead. A build with both writes both.
   */
  private static Object[] readFile(Table table, Schema projection, String column, String path,
      BuildConfig config, boolean countTerms, boolean needsFileMap, int ordinal,
      RunBuffer arena, IndexType.Collector[] collectors)
      throws IOException {
    // Resolved here rather than threaded: this method already has the table, the column and the
    // config, and the table's properties are a fixed snapshot for the life of the build.
    Analyzer.Contract contract = IndexSettings.contract(table, column, config);
    Grams.Contract gramRule = IndexSettings.grams(table, column, config);
    double fpp = IndexSettings.probability(table, column, "bloom-fpp", FPP);
    // Primitive gram accumulation, not a Set<String>: see GramAccumulator. Collecting grams as
    // strings costs a set per ROW and a substring per gram position, which dominates the read.
    GramAccumulator gramSet = new GramAccumulator(gramRule);
    TermIndexWriter.FileTerms terms = new TermIndexWriter.FileTerms();
    // The SAME contract the arena path below uses. Left at its default, these two would disagree
    // about which tokens are indexable and the per-file tally would describe a dictionary nobody
    // built.
    terms.contract = contract;
    // One accessor for the column, built from the PROJECTION, whose positions are the ones the
    // reader's records actually have. It walks nested structs and answers null when any
    // intermediate struct is null. Not Record.getField: that is single-level and answers null for
    // a path it does not know, so a nested column would read as all-null and publish a covering,
    // token-free index -- an index that prunes away every file that matches.
    org.apache.iceberg.Accessor<org.apache.iceberg.StructLike> accessor =
        projection.accessorForField(table.schema().findField(column).fieldId());
    IcebergKinds.Shape shape =
        IcebergKinds.shapeOf(table.schema().findField(column).type());
    // Resolve BEFORE any collector is told this file exists, and refuse here rather than below.
    // Without the table's name mapping, a file that carries no field ids is resolved by column
    // POSITION, which indexes one column's values under another's id whenever a schema has ever
    // dropped or reordered a column. See DataFileIds.
    org.apache.iceberg.mapping.NameMapping mapping = io.kahshe.format.DataFileIds.mappingOf(table);
    org.apache.iceberg.io.InputFile input =
        IndexPaths.dataIo(table, config.format()).newInputFile(path);
    io.kahshe.format.DataFileIds.requireResolvable(input, mapping);
    // ORDER MATTERS, and it is invisible while the refusal above throws. A collector told about
    // (path, ordinal) has taken an ordinal for this file, and the publish below turns every new
    // path into a LIVE coverage entry at that ordinal. Refuse first and the file was never
    // announced; refuse after and any future per-file tolerance would leave a covered entry
    // holding nothing -- the file pruned for every term it actually contains. Today the build
    // dies either way, so nothing observes this; it is ordered correctly so that the day
    // something does, it is already right.
    for (IndexType.Collector collector : collectors) {
      collector.file(path, ordinal);
    }
    int row = 0;
    try (CloseableIterable<Record> records =
        io.kahshe.format.DataFileIds.withMapping(Parquet.read(input), mapping)
            .project(projection)
            .createReaderFunc(fs -> GenericParquetReaders.buildReader(projection, fs))
            .build()) {
      for (Record record : records) {
        // Canonical form, not toString(): a long is its decimal text, a UUID its undashed hex, a
        // binary its lowercase hex -- and the pruner canonicalises literals the same way, which
        // is the only reason a probe for a non-string id can find what the build wrote.
        // One value per row for a scalar, many for a list or a map. A null row and an empty
        // container are the same thing here: no terms, exactly as a null scalar contributes none.
        List<String> rowTokens;
        if (shape.repetition() == IcebergKinds.Repetition.SCALAR) {
          String text = Canonical.form(shape.kind(), accessor.get(record));
          rowTokens = null;
          if (text != null) {
            gramSet.addAll(text);
            rowTokens = countTerms ? contract.tokens(text) : List.of();
          }
        } else {
          rowTokens = containerTokens(accessor.get(record), shape, contract, gramSet, countTerms);
        }
        if (rowTokens != null) {
          if (countTerms) {
            List<String> tokens = rowTokens;
            if (arena != null) {
              // Straight into the arena, one record per token occurrence. Nothing is coalesced on
              // the way in and nothing is held per file: a term repeated a thousand times appends
              // a thousand records and they collapse at the flush, where the data is already
              // sorted.
              for (String token : tokens) {
                if (contract.isIndexable(token)) {
                  arena.append(token, 1L, ordinal);
                }
              }
            }
            if (needsFileMap) {
              // A watch rule needs this file's counts as a map it can probe by token. That is a
              // separate need from the index's, and a build that has both does both -- the map is
              // per file and released with it, while the arena is per reader.
              terms.row(row, tokens);
            } else {
              // Row and token totals are tallied HERE, not by the structure holding the terms:
              // they are per-file, while the arena is per-reader and spans files.
              terms.countRow(row, tokens.size());
            }
            for (IndexType.Collector collector : collectors) {
              collector.row(row, tokens);
            }
          } else {
            // nothing consumes the counts: skip tokenizing entirely rather than computing and
            // discarding them. Grams and blooms are unaffected -- they read the raw text.
            terms.rowWithoutTerms(row);
            // no tokens exist on this build; a row tier still sees the row
            for (IndexType.Collector collector : collectors) {
              collector.row(row, List.of());
            }
          }
        }
        row++;
      }
    }
    for (IndexType.Collector collector : collectors) {
      collector.fileDone();
    }
    // NO tail flush here. The arena belongs to the reader, not to this file: it carries records
    // for every file this reader has handled and is drained once, after every reader is joined.
    // Flushing per file would allocate a run per data file and defeat the reuse that the ordinal
    // in each record exists to allow.

    // materialized once per file, over the distinct grams rather than the positions that
    // produced them
    Set<String> grams = gramSet.toStrings();
    return new Object[] {terms, NgramBloom.build(grams, gramRule, fpp), grams};
  }


  /**
   * Whether the prior generation still claimed exact counts. Absent means an index written before
   * the flag existed, and those were exact by construction: nothing could tombstone yet.
   */
  private static boolean priorCountsExact(com.fasterxml.jackson.databind.JsonNode termMeta) {
    return termMeta == null
        || TermIndexWriter.propertiesNode(termMeta).path("counts-exact").asBoolean(true);
  }

  /**
   * A file below this many distinct grams is never reported saturated: a small gram set is
   * selective whatever its alphabet, and a file that small costs nothing to scan anyway.
   */
  static final int GRAM_SATURATION_MIN = 256;

  /**
   * Whether one file's gram set has saturated its own alphabet's gram space, measured where the
   * evidence exists.
   *
   * <p>Deliberately NOT the bloom's fill fraction: {@link NgramBloom#build} sizes the filter from
   * the gram count it is given, so a well-formed bloom sits near half-full whatever the column
   * holds — fill measures sizing, not selectivity. What saturates on a dense-identifier column is
   * the gram SET itself: its distinct grams approach every gram its alphabet can form (the
   * alphabet raised to the gram size — 1000 for decimal trigrams, 4096 for hex), at which point a
   * probe drawn from that alphabet finds all its grams present and neither the blooms nor the
   * exact gram tier can prune the file. The alphabet is measured from the grams themselves, so
   * prose — a large alphabet using a sparse corner of its space — is not flagged. Saturated means
   * at least half the space is present, past the size floor above.
   */
  static boolean gramSpaceSaturated(Set<String> grams, Grams.Contract rule) {
    if (grams.size() < GRAM_SATURATION_MIN) {
      return false;
    }
    java.util.Set<Integer> alphabet = new java.util.HashSet<>();
    for (String gram : grams) {
      gram.codePoints().forEach(alphabet::add);
    }
    double space = Math.pow(alphabet.size(), rule.size());
    return grams.size() * 2.0 >= space;
  }


  private static com.fasterxml.jackson.databind.JsonNode readTermMeta(
      org.apache.iceberg.io.FileIO io, String indexRoot, int fieldId) {
    try {
      var metaFile = io.newInputFile(TermIndexWriter.dir(indexRoot, fieldId) + "/index-metadata.json");
      if (!metaFile.exists()) {
        return null;
      }
      try (var in = metaFile.newStream()) {
        return new ObjectMapper().readTree(in);
      }
    } catch (Exception e) {
      // SAY SO. Returning null here makes `covered` empty, which makes the build FULL -- hours on
      // a large table, and otherwise indistinguishable from a first build in every log line that
      // follows.
      LOG.warn(
          "term index metadata for f{} exists but could not be read; treating the table as "
              + "UNINDEXED, which forces a full rebuild. If this is transient, the rebuild is "
              + "wasted work rather than a wrong answer.",
          fieldId, e);
      return null;
    }
  }


  /**
   * Re-points an existing term index at a new snapshot without rebuilding it, for a commit that
   * added no data files.
   *
   * <p>This is also the ONLY migration point for an index written by an older binary, which is why
   * it does more than restamp: a table that is merely restamped is exactly the table nobody
   * rebuilds, so a document rewritten verbatim here would advertise retired tiers forever and
   * their orphaned objects would be deleted on no code path at all. The restamp therefore strips
   * those leaf lists, deletes the objects they named, and fills in the spec fields a legacy
   * document predates. Deletion is best effort and logged: an object that cannot be removed is
   * wasted space, never a correctness problem, and must not fail an otherwise complete build.
   */
  private static void restampTermMeta(
      org.apache.iceberg.io.FileIO io, String indexRoot, int fieldId,
      com.fasterxml.jackson.databind.JsonNode termMeta, long snapshotId,
      List<Coverage.Entry> coverage) {
    try (PositionOutputStream out = io
        .newOutputFile(TermIndexWriter.dir(indexRoot, fieldId) + "/index-metadata.json")
        .createOrOverwrite()) {
      var snapshot =
          (com.fasterxml.jackson.databind.node.ObjectNode) TermIndexWriter.snapshotNode(termMeta);
      // A restamp is the ONLY path a departure can take when nothing new arrived, so the
      // reconciled liveness has to be written here or a file that left stays recorded as covered
      // until some unrelated build happens to notice.
      if (coverage != null) {
        snapshot.set(
            "files", new ObjectMapper().valueToTree(Coverage.toJson(coverage)));
      }
      if (snapshot.has("source-table-snapshot-id")) {
        snapshot.put("source-table-snapshot-id", snapshotId);
        snapshot.put("timestamp-ms", System.currentTimeMillis());
      } else {
        snapshot.put("snapshotId", snapshotId);
      }
      dropDeadLeaves(io, snapshot);
      if (termMeta instanceof com.fasterxml.jackson.databind.node.ObjectNode root) {
        // a document written before these were required carries neither; both are derivable here
        if (!root.has("transform-function")) {
          root.put("transform-function", "IDENTITY");
        }
        if (!root.has("key-column-ids")) {
          root.putArray("key-column-ids").add(fieldId);
        }
      }
      out.write(new ObjectMapper().writerWithDefaultPrettyPrinter().writeValueAsBytes(termMeta));
    } catch (IOException e) {
      throw new UncheckedIOException(e);
    }
  }

  /**
   * Removes the retired postings and norms leaf lists from a snapshot node and deletes the objects
   * they name. Neither tier is read at plan time, and together they cost several times the bytes
   * of the index that does answer queries.
   */
  private static void dropDeadLeaves(
      org.apache.iceberg.io.FileIO io, com.fasterxml.jackson.databind.node.ObjectNode snapshot) {
    var leaves = snapshot.path("leaves");
    if (!(leaves instanceof com.fasterxml.jackson.databind.node.ObjectNode node)) {
      return;
    }
    for (String dead : java.util.List.of("postings", "norms")) {
      var removed = node.remove(dead);
      if (removed == null || !removed.isArray()) {
        continue;
      }
      int gone = 0;
      for (var leaf : removed) {
        String path = leaf.asText("");
        if (path.isBlank()) {
          continue;
        }
        try {
          io.deleteFile(path);
          gone++;
        } catch (RuntimeException e) {
          // wasted space, never a correctness problem: a build that is otherwise complete must
          // not fail because an object it no longer needs could not be removed
          LOG.warn("could not delete the orphaned {} leaf {}", dead, path, e);
        }
      }
      LOG.info("dropped {} orphaned {} leaf/leaves from a legacy term index", gone, dead);
    }
  }



  /**
   * The build report (docs/FORMAT.md section 12), written after the tiers' metadata so a reader
   * that sees the report sees the build. Its counters are read back from the term document just
   * published rather than carried through the build, so the report says what the artifact says.
   * A prior report that cannot be read chains to nothing, loudly; a report that cannot be written
   * fails the build's caller, not the build: the artifact is already published.
   */
  private static void writeReport(
      org.apache.iceberg.io.FileIO io, String indexRoot, int fieldId, String prefix, String namespace,
      String tableName, String column, long snapshotId, BuildReport.Kind kind, Analyzer.Contract contract,
      Grams.Contract gramRule, long startedMs, List<String> added, List<String> departed, long bloomBytes,
      int saturatedFiles, boolean partial, List<Map<String, Object>> alerts,
      io.kahshe.common.Metrics metrics) {
    String previous = null;
    try {
      BuildReport prior = BuildReport.read(io, indexRoot, fieldId);
      previous = prior == null ? null : prior.buildId();
    } catch (RuntimeException e) {
      LOG.warn("prior build report for f{} unreadable; this report chains to nothing", fieldId, e);
    }
    Map<String, Long> counters = new java.util.LinkedHashMap<>();
    List<String> leaves = new ArrayList<>();
    com.fasterxml.jackson.databind.JsonNode termMeta = readTermMeta(io, indexRoot, fieldId);
    long covered = 0;
    if (termMeta != null) {
      com.fasterxml.jackson.databind.JsonNode snapshot = TermIndexWriter.snapshotNode(termMeta);
      covered = Coverage.livePaths(Coverage.parse(snapshot.path("files"))).size();
      snapshot.path("totals").properties().forEach(e -> {
        if (e.getValue().isNumber()) {
          counters.put(e.getKey(), e.getValue().asLong());
        }
      });
      counters.put("grams", snapshot.path("gram-coverage").path("grams").asLong(0));
      counters.put("gram-bytes", snapshot.path("gram-coverage").path("bytes").asLong(0));
      for (String leaf : TermIndexWriter.aggregateLeaves(snapshot)) {
        if (!leaf.isEmpty()) {
          leaves.add(leaf);
        }
      }
      String gramsLeaf = snapshot.path("leaves").path("grams").asText("");
      if (!gramsLeaf.isEmpty()) {
        leaves.add(gramsLeaf);
      }
    }
    counters.put("files-covered", covered);
    counters.put("files-added", (long) added.size());
    counters.put("files-departed", (long) departed.size());
    counters.put("bloom-bytes", bloomBytes);
    List<String> warnings = new ArrayList<>();
    if (saturatedFiles > 0) {
      warnings.add("gram space saturated in " + saturatedFiles + " file(s)");
    }
    if (partial) {
      warnings.add("partial: a checkpoint pass, not the whole snapshot");
    }
    long publishedMs = System.currentTimeMillis();
    new BuildReport(
            Long.toHexString(java.util.concurrent.ThreadLocalRandom.current().nextLong()),
            previous, prefix, namespace, tableName, column, fieldId, snapshotId, kind, contract.id(),
            gramRule.id(), startedMs, publishedMs, counters, added, departed, leaves,
            warnings, alerts)
        .write(io, indexRoot);
    // After the write, so this agrees with kahshe_index_builds_total on what counts as a build: a
    // report that could not be written fails the caller, and the caller counts a failure.
    metrics.indexPublished(
        namespace.isEmpty() ? tableName : namespace + "." + tableName, column,
        kind.name().toLowerCase(java.util.Locale.ROOT), publishedMs - startedMs);
  }
}
