package io.kahshe.format.type.term;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import io.kahshe.common.SingleFlight;
import io.kahshe.common.WeighedCache;
import java.io.IOException;
import java.io.InputStream;
import java.nio.ByteBuffer;
import java.util.Collection;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import org.apache.iceberg.Table;
import org.apache.iceberg.data.Record;
import org.apache.iceberg.data.parquet.GenericParquetReaders;
import org.apache.iceberg.io.CloseableIterable;
import org.apache.iceberg.io.InputFile;
import org.apache.iceberg.parquet.Parquet;
import org.roaringbitmap.RoaringBitmap;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import io.kahshe.format.Coverage;
import io.kahshe.format.FormatConfig;
import io.kahshe.format.IndexPaths;
import io.kahshe.analysis.analyzer.Analyzer;
import io.kahshe.analysis.analyzer.Analyzers;

/**
 * Loads and caches the term index's aggregate layer (term -> files bitmap + counts). What is
 * loaded per field is the metadata and the coverage list, never the vocabulary: a query's tokens
 * are resolved on demand out of the range leaves they fall in.
 */
public final class TermIndex {
  private static final Logger LOG = LoggerFactory.getLogger(TermIndex.class);
  private static final long TTL_MS = 30_000;
  private static final ObjectMapper MAPPER = new ObjectMapper();

  public record Entry(int fileCount, long totalCount, RoaringBitmap ordinals) {}

  /**
   * One field's term index as a reader holds it: the metadata document, the coverage list it needs
   * to interpret any bitmap, and the contract every probe against it must obey.
   *
   * @param contract the analyzer contract this index was built under, read off its analyzer id:
   *     the version's admission rule and the token-length cap. Readers take it from here and never
   *     from their own configuration, so a proxy configured differently from the build cannot probe
   *     for tokens the build never wrote — absence prunes
   * @param countsExact whether an occurrence count read off this index is exact. False from the
   *     moment any covered file has left the table, and sticky until a full rebuild re-reads the
   *     data; pruning is unaffected. Not "has tombstones": compaction removes the tombstones and
   *     cannot remove the inflation total_count carries
   * @param fingerprint the revalidation token this index was loaded under, and the entry cache's
   *     key prefix. It moves whenever any leaf's content moves, so a cached entry cannot outlive
   *     the bytes it was read from; see {@code fingerprintOf}
   * @param partial a checkpoint over part of the table's files: not current, and _count refuses it
   */
  public record Loaded(
      long snapshotId,
      String analyzer,
      Analyzer.Contract contract,
      List<String> files,
      Map<String, Integer> ordinalOf,
      List<String> aggregateLeaves,
      long rowsTotal,
      boolean countsExact,
      long weightBytes,
      String fingerprint,
      boolean partial) {
    /** The cap the contract carries; {@code Integer.MAX_VALUE} for v1, which had none. */
    public int maxTokenLen() {
      return contract.maxTokenLen();
    }
  }

  private record CacheEntry(
      Optional<Loaded> index, String fingerprint, long loadedAt, long weightBytes) {}

  private final WeighedCache<String, CacheEntry> cache;

  /**
   * Resolved {@code (fingerprint, token) -> entry} lookups, so a repeated question does not reopen
   * a Parquet reader and re-parse the leaf footer. It caches the answer rather than the footer:
   * Iceberg's {@code Parquet.ReadBuilder} accepts no pre-parsed {@code ParquetMetadata}.
   *
   * <p>Absence is cached too, and a token with no entry prunes -- so a stale negative is a file
   * dropped from someone's answer. That is safe only because the key is the fingerprint, which
   * moves whenever any leaf's bytes move. Do not re-key this on anything weaker.
   */
  private final WeighedCache<String, Optional<Entry>> entryCache;
  private final SingleFlight<String> flight = new SingleFlight<>();

  /**
   * Dead-ordinal percentage per (table, field), for {@code kahshe_index_dead_ordinal_percent}. A
   * tombstoned entry costs nothing to serve, so its accumulation is invisible from every other
   * signal while it inflates the metadata and makes {@code _count} refuse. Bounded by the number of
   * (table, field) pairs this process has loaded.
   */
  private final java.util.concurrent.ConcurrentHashMap<String, Integer> deadPercent =
      new java.util.concurrent.ConcurrentHashMap<>();
  private final FormatConfig config;

  // injectable for TTL tests
  java.util.function.LongSupplier nowMs = System::currentTimeMillis;

  private final io.kahshe.common.Metrics metrics;

  public TermIndex(FormatConfig config, io.kahshe.common.Metrics metrics) {
    this.metrics = metrics;
    this.config = config;
    this.cache =
        new WeighedCache<>(
            config.termCacheBytes(), CacheEntry::weightBytes, metrics.termCacheEvictions);
    metrics.termCacheWeightBytes = cache::estimatedWeightBytes;
    metrics.indexDeadOrdinalPercent =
        () -> deadPercent.values().stream().mapToLong(Integer::longValue).max().orElse(0L);
    // A quarter of the term budget for resolved lookups. An entry is a term's bitmap plus two
    // counts; on the corpus this matters for, a term names one file and serializes to ~20 bytes.
    this.entryCache =
        new WeighedCache<>(
            Math.max(1L << 20, config.termCacheBytes() / 4),
            e -> e.map(x -> 64L + x.ordinals().serializedSizeInBytes()).orElse(48L),
            metrics.termCacheEvictions);
  }

  /**
   * Returns the term index for (table, fieldId), or null when none exists. TTL expiry re-reads
   * only the metadata JSON; the aggregate is reloaded only when the index changed. Concurrent
   * cold loads of the same key share one flight; different keys still load concurrently.
   */
  public Loaded forField(Table table, int fieldId) {
    String key = table.location() + "#f" + fieldId;
    CacheEntry entry = fresh(key);
    if (entry == null) {
      entry = flight.load(key, () -> fresh(key), () -> refresh(table, fieldId, key));
    }
    return entry.index().orElse(null);
  }

  private CacheEntry fresh(String key) {
    CacheEntry entry = cache.get(key);
    return entry != null && nowMs.getAsLong() - entry.loadedAt() <= TTL_MS ? entry : null;
  }

  private CacheEntry refresh(Table table, int fieldId, String key) {
    CacheEntry entry = cache.get(key);
    long now = nowMs.getAsLong();
    String fingerprint = fingerprint(table, fieldId, config);
    if (entry != null && entry.fingerprint().equals(fingerprint)) {
      CacheEntry renewed = new CacheEntry(entry.index(), fingerprint, now, entry.weightBytes());
      cache.put(key, renewed);
      return renewed;
    }
    Optional<Loaded> loaded = load(table, fieldId, key);
    // The stored token is the one load derived, not the one compared above: those two reads can
    // straddle a rebuild, and the payload's own token is the only one guaranteed to describe the
    // ordinals this entry actually holds. A failed load must not stick, so poison it uniquely and
    // the next expiry retries.
    String storeFingerprint =
        loaded.map(Loaded::fingerprint)
            .orElseGet(
                () ->
                    "absent".equals(fingerprint)
                        ? "absent"
                        : "load-failed-" + UNIQUE.incrementAndGet());
    CacheEntry stored =
        new CacheEntry(loaded, storeFingerprint, now, entryWeight(loaded, key, storeFingerprint));
    cache.put(key, stored);
    return stored;
  }

  /** Negative entries are weighed too, so probe storms on absent indexes stay budget-bounded. */
  private static long entryWeight(Optional<Loaded> index, String key, String fingerprint) {
    return index
        .map(Loaded::weightBytes)
        .orElse(2L * key.length() + 2L * fingerprint.length() + 464);
  }

  /**
   * Distinguishes one unreadable/failed observation from the next. A shared sentinel would be a
   * false negative: it keys the resolved-entry cache, which records token absence, so one table's
   * "this token is in no file" would be served for another's. It would also make {@link #refresh}
   * renew a stale index forever, since it renews an entry whose fingerprint compares equal.
   */
  private static final java.util.concurrent.atomic.AtomicLong UNIQUE = new java.util.concurrent.atomic.AtomicLong();

  /** The revalidation token for a field's index, or {@code "absent"} when it has none. */
  public static String fingerprint(Table table, int fieldId, FormatConfig config) {
    try {
      org.apache.iceberg.io.FileIO io = IndexPaths.io(table, config);
      InputFile metaFile = io.newInputFile(
          TermIndexWriter.dir(IndexPaths.root(table, config.indexRoot()), fieldId) + "/index-metadata.json");
      if (!metaFile.exists()) {
        return "absent";
      }
      try (InputStream in = metaFile.newStream()) {
        return fingerprintOf(io, MAPPER.readTree(in));
      }
    } catch (IOException | RuntimeException e) {
      return "unreadable-" + UNIQUE.incrementAndGet();
    }
  }

  /**
   * The token for an already-parsed metadata document. Taking the document as an argument is what
   * makes the token and the payload describe the same generation: reading the metadata once to
   * fingerprint it and again to load it is a race, and a bitmap's ordinals are positions in one
   * generation's file list, so a payload stamped with the wrong token prunes files that match.
   */
  private static String fingerprintOf(org.apache.iceberg.io.FileIO io, JsonNode meta) {
    JsonNode snapshot = TermIndexWriter.snapshotNode(meta);
    long snapshotId = snapshot.path("source-table-snapshot-id").asLong(snapshot.path("snapshotId").asLong());
    List<String> leaves = TermIndexWriter.aggregateLeaves(snapshot);
    if (leaves.isEmpty()) {
      return "absent";
    }
    // The name is what makes this token move when the content does: every range leaf is written
    // under a per-publish nonce (TermRanges.leafName), so a rewrite -- even a full rebuild at the
    // same snapshot -- produces a different path, and the metadata's leaf list identifies the
    // generation by itself. Only a leaf whose name lacks the nonce shape (the pre-partitioning
    // single-leaf layout) has to be sized with a HEAD instead.
    StringBuilder token = new StringBuilder().append(snapshotId);
    for (String leaf : leaves) {
      if (leaf == null || leaf.isEmpty()) {
        token.append("|-"); // an empty range still occupies its slot in the token
        continue;
      }
      token.append('|').append(leaf);
      if (!NONCED_LEAF.matcher(leaf).find()) {
        token.append('|').append(io.newInputFile(leaf).getLength());
      }
    }
    return token.toString();
  }

  /** A range leaf named by {@code TermRanges.leafName}: snapshot, range, per-publish nonce. */
  private static final java.util.regex.Pattern NONCED_LEAF =
      java.util.regex.Pattern.compile("/aggregate-\\d+-r\\d{2}-[0-9a-f]+\\.parquet$");

  /**
   * The entries for {@code tokens}, read from only the range leaves those tokens fall in. The
   * aggregate is partitioned by first character and written in sorted term order, so a query
   * touches at most one leaf per distinct first character and, within a leaf, only the row groups
   * whose statistics could hold the token.
   *
   * <p>An absent token yields no entry, which the caller reads as "no file holds this term" and
   * prunes on. A failure to read therefore throws rather than returning empty, and every caller
   * must let the exception propagate to the keep-everything path.
   */
  public Map<String, Entry> entriesFor(Table table, Loaded loaded, Collection<String> tokens)
      throws IOException {
    List<String> leaves = loaded.aggregateLeaves();
    if (leaves.isEmpty() || tokens.isEmpty()) {
      return Map.of();
    }
    Map<String, Entry> found = new HashMap<>();
    // Resolved lookups first. A miss is a token we have never answered under this fingerprint.
    java.util.Set<String> unresolved = new java.util.LinkedHashSet<>();
    for (String token : tokens) {
      Optional<Entry> cached = entryCache.get(loaded.fingerprint() + "|" + token);
      if (cached == null) {
        unresolved.add(token);
      } else {
        cached.ifPresent(entry -> found.put(token, entry));
        metrics.termEntryCacheHits.increment();
      }
    }
    if (unresolved.isEmpty()) {
      metrics.termLookups.increment();
      return found;
    }

    // An index written before range partitioning is one leaf holding every term, so every token
    // resolves to it. This is what lets an existing index keep serving until it is rebuilt.
    boolean partitioned = leaves.size() > 1;
    Map<Integer, java.util.Set<String>> byLeaf = new java.util.LinkedHashMap<>();
    for (String token : unresolved) {
      int at = partitioned ? TermRanges.of(token) : 0;
      if (at >= 0 && at < leaves.size()) {
        byLeaf.computeIfAbsent(at, ignored -> new java.util.HashSet<>()).add(token);
      }
    }
    org.apache.iceberg.io.FileIO io = IndexPaths.io(table, config);
    for (Map.Entry<Integer, java.util.Set<String>> group : byLeaf.entrySet()) {
      String leaf = leaves.get(group.getKey());
      if (leaf == null || leaf.isEmpty()) {
        continue;
      }
      java.util.Set<String> wanted = group.getValue();
      try (CloseableIterable<Record> records =
          Parquet.read(io.newInputFile(leaf))
              .project(TermIndexWriter.AGGREGATE_SCHEMA)
              .filter(org.apache.iceberg.expressions.Expressions.in("term", wanted))
              .createReaderFunc(
                  fs -> GenericParquetReaders.buildReader(TermIndexWriter.AGGREGATE_SCHEMA, fs))
              .build()) {
        for (Record r : records) {
          String term = r.getField("term").toString();
          if (!wanted.contains(term)) {
            continue; // row-group filtering is coarse; the exact match is still ours to make
          }
          ByteBuffer buffer = (ByteBuffer) r.getField("file_ordinals");
          RoaringBitmap ordinals = new RoaringBitmap();
          ordinals.deserialize(buffer.duplicate());
          found.put(
              term,
              new Entry(
                  (Integer) r.getField("file_count"), (Long) r.getField("total_count"), ordinals));
        }
      }
    }
    // Populate only after every leaf read succeeded: a missing entry prunes, so a partial answer
    // cached is a file dropped from a future query with nothing to indicate it happened.
    for (String token : unresolved) {
      entryCache.put(
          loaded.fingerprint() + "|" + token, Optional.ofNullable(found.get(token)));
    }
    metrics.termLookups.increment();
    return found;
  }

  /** The union over a run of the dictionary: what a prefix or range query prunes and counts with. */
  public record PrefixEntries(int terms, long totalCount, RoaringBitmap ordinals) {}

  /**
   * Every term that starts with {@code prefix}, joined. A prefix is the range from the prefix to
   * its successor (the prefix with its last char bumped), so this is {@link #entriesForRange}
   * with each row also checked with {@code startsWith}; a surrogate last char has no successor
   * and the range is open above. Null when the run is longer than {@code maxTerms}.
   */
  public PrefixEntries entriesForPrefix(Table table, Loaded loaded, String prefix, int maxTerms)
      throws IOException {
    if (prefix.isEmpty()) {
      return null; // everything starts with nothing; that is not a prefix query
    }
    char last = prefix.charAt(prefix.length() - 1);
    String upper =
        last < Character.MIN_SURROGATE
            ? prefix.substring(0, prefix.length() - 1) + (char) (last + 1)
            : null;
    return entriesForRange(table, loaded, prefix, true, upper, false, maxTerms, prefix);
  }

  /**
   * Every term in a range of the dictionary, joined: because rows ascend by term, a range is a
   * contiguous run across the leaves its bounds' first characters name -- the row groups between
   * the bounds -- with each row compared exactly in Iceberg's string order (the order the engine's
   * own evaluator uses), and the file bitmaps ORed. A null bound is open. Null when the run is
   * longer than {@code maxTerms}: the caller keeps every file and says so, which bounds what one
   * loose range can cost. No entry cache: a run's answer can be large, and the read is bounded.
   */
  public PrefixEntries entriesForRange(
      Table table, Loaded loaded, String lower, boolean lowerInclusive, String upper,
      boolean upperInclusive, int maxTerms)
      throws IOException {
    return entriesForRange(table, loaded, lower, lowerInclusive, upper, upperInclusive, maxTerms, null);
  }

  private PrefixEntries entriesForRange(
      Table table, Loaded loaded, String lower, boolean lowerInclusive, String upper,
      boolean upperInclusive, int maxTerms, String prefix)
      throws IOException {
    List<String> leaves = loaded.aggregateLeaves();
    if (leaves.isEmpty()) {
      return new PrefixEntries(0, 0L, new RoaringBitmap());
    }
    java.util.Comparator<CharSequence> order = org.apache.iceberg.types.Comparators.charSequences();
    if (lower != null && upper != null && order.compare(lower, upper) > 0) {
      return new PrefixEntries(0, 0L, new RoaringBitmap()); // an empty range holds nothing
    }
    int first = lower == null || leaves.size() == 1 ? 0 : TermRanges.of(lower);
    int lastLeaf = upper == null || leaves.size() == 1 ? leaves.size() - 1 : TermRanges.of(upper);
    org.apache.iceberg.expressions.Expression filter = org.apache.iceberg.expressions.Expressions.alwaysTrue();
    if (lower != null) {
      filter =
          lowerInclusive
              ? org.apache.iceberg.expressions.Expressions.greaterThanOrEqual("term", lower)
              : org.apache.iceberg.expressions.Expressions.greaterThan("term", lower);
    }
    if (upper != null) {
      filter =
          org.apache.iceberg.expressions.Expressions.and(
              filter,
              upperInclusive
                  ? org.apache.iceberg.expressions.Expressions.lessThanOrEqual("term", upper)
                  : org.apache.iceberg.expressions.Expressions.lessThan("term", upper));
    }
    RoaringBitmap union = new RoaringBitmap();
    long total = 0;
    int terms = 0;
    org.apache.iceberg.io.FileIO io = IndexPaths.io(table, config);
    for (int at = first; at <= lastLeaf; at++) {
      String leaf = leaves.get(at);
      if (leaf == null || leaf.isEmpty()) {
        continue;
      }
      try (CloseableIterable<Record> records =
          Parquet.read(io.newInputFile(leaf))
              .project(TermIndexWriter.AGGREGATE_SCHEMA)
              .filter(filter)
              .createReaderFunc(
                  fs -> GenericParquetReaders.buildReader(TermIndexWriter.AGGREGATE_SCHEMA, fs))
              .build()) {
        for (Record r : records) {
          String term = r.getField("term").toString();
          // row-group filtering is coarse; the range's edges are ours to enforce
          if (lower != null) {
            int c = order.compare(term, lower);
            if (c < 0 || (c == 0 && !lowerInclusive)) {
              continue;
            }
          }
          if (upper != null) {
            int c = order.compare(term, upper);
            if (c > 0 || (c == 0 && !upperInclusive)) {
              continue;
            }
          }
          if (prefix != null && !term.startsWith(prefix)) {
            continue;
          }
          if (++terms > maxTerms) {
            metrics.termPrefixCapped.increment();
            return null;
          }
          ByteBuffer buffer = (ByteBuffer) r.getField("file_ordinals");
          RoaringBitmap ordinals = new RoaringBitmap();
          ordinals.deserialize(buffer.duplicate());
          union.or(ordinals);
          total += (Long) r.getField("total_count");
        }
      }
    }
    metrics.termPrefixLookups.increment();
    return new PrefixEntries(terms, total, union);
  }

  private Optional<Loaded> load(Table table, int fieldId, String key) {
    try {
      org.apache.iceberg.io.FileIO io = IndexPaths.io(table, config);
      InputFile metaFile = io.newInputFile(
          TermIndexWriter.dir(IndexPaths.root(table, config.indexRoot()), fieldId) + "/index-metadata.json");
      if (!metaFile.exists()) {
        return Optional.empty();
      }
      JsonNode meta;
      try (InputStream in = metaFile.newStream()) {
        meta = MAPPER.readTree(in);
      }
      // Derived from the document just parsed -- see fingerprintOf.
      String fingerprint = fingerprintOf(io, meta);
      if (TermIndexWriter.newerThanThisReader(meta)) {
        // A newer writer's layout, on the spec's version or on kahshe's. Refusing keeps every
        // file; guessing at it would prune on a document whose meaning this reader does not know.
        LOG.warn(
            "term index format-version {} / kahshe.format-version {} is newer than this kahshe's "
                + "{} / {}; refusing to use it, so this column serves unpruned until kahshe is upgraded",
            meta.path("format-version").asInt(1), TermIndexWriter.tierVersionOf(meta),
            TermIndexWriter.FORMAT_VERSION, TermIndexWriter.TIER_FORMAT_VERSION);
        return Optional.empty();
      }
      String analyzer = TermIndexWriter.propertiesNode(meta).path("analyzer").asText();
      Analyzer.Contract contract = Analyzer.contractOf(analyzer);
      if (contract == null) {
        // An id no loaded family owns -- not the built-in ascii family, whose v1 rule is kept so
        // its indexes stay readable during a rebuild, nor value, nor anything the service loader
        // found. Refusing keeps every file: correct, unpruned, and it says so, which is what
        // makes changing the contract survivable at all.
        LOG.warn(
            "no analyzer family loaded here owns the term index analyzer {} (loaded: {}); "
                + "refusing to use it, so this column serves unpruned until it is rebuilt",
            analyzer, Analyzers.names());
        return Optional.empty();
      }
      JsonNode snapshot = TermIndexWriter.snapshotNode(meta);
      // Live entries only, each at its recorded ordinal rather than its position. A tombstoned
      // file is absent from ordinalOf, so any pruner keeps it -- see Coverage.
      List<Coverage.Entry> coverage = Coverage.parse(snapshot.path("files"));
      List<String> files = Coverage.livePaths(coverage);
      Map<String, Integer> ordinalOf = Coverage.ordinalOfLive(coverage);
      boolean countsExact =
          TermIndexWriter.propertiesNode(meta).path("counts-exact").asBoolean(true);
      boolean partial =
          "true".equals(TermIndexWriter.propertiesNode(meta).path("partial").asText(null));
      deadPercent.put(
          key, coverage.isEmpty() ? 0 : (int) (100L * (coverage.size() - files.size()) / coverage.size()));
      // calibrated per-entry heap estimate: term chars, bitmap payload, map/record overhead
      long weightBytes = 1024L + 2L * key.length() + 2L * fingerprint.length() + 400;
      for (String path : files) {
        weightBytes += 2L * path.length() + 200;
      }
      // The vocabulary is not loaded here: a query names a handful of tokens, so only those are
      // read, from only the range leaves they fall in (see entriesFor). What is loaded is the file
      // list, because every pruning decision needs it to interpret any bitmap at all.
      List<String> aggregateLeaves = TermIndexWriter.aggregateLeaves(snapshot);
      if (aggregateLeaves.stream().allMatch(leaf -> leaf == null || leaf.isEmpty())) {
        // no aggregate: either the term tier was off for this build, or it wrote nothing. Either
        // way there is no term index to serve, which keeps files rather than pruning them.
        return Optional.empty();
      }
      // The leaf list is positional against TermRanges.of, and nothing in the artifact records
      // which routing function produced it. A reader whose TermRanges disagrees with the writer's
      // would route tokens to the wrong slot or past the end of the list, and every covered file
      // would be pruned for those queries with no log to find it by. Refusing the artifact keeps
      // files instead. A single leaf is the pre-partitioning layout and is exempt.
      if (aggregateLeaves.size() > 1 && aggregateLeaves.size() != TermRanges.COUNT) {
        LOG.warn(
            "term index f{} has {} aggregate range leaves but this reader routes into {}; "
                + "refusing to use it (files will be kept, not pruned) -- the artifact was written "
                + "by a different range scheme and must be rebuilt",
            fieldId, aggregateLeaves.size(), TermRanges.COUNT);
        return Optional.empty();
      }
      long snapshotId = snapshot.path("source-table-snapshot-id").asLong(snapshot.path("snapshotId").asLong());
      LOG.info(
          "loaded term index f{}: {} files, {} aggregate range leaf/leaves, snapshot {}",
          fieldId, files.size(), aggregateLeaves.size(), snapshotId);
      return Optional.of(
          new Loaded(
              snapshotId,
              analyzer,
              contract,
              files,
              ordinalOf,
              aggregateLeaves,
              snapshot.path("totals").path("rows").asLong(),
              countsExact,
              weightBytes,
              fingerprint,
              partial));
    } catch (IOException | RuntimeException e) {
      LOG.warn("failed to load term index f{}; ignoring", fieldId, e);
      return Optional.empty();
    }
  }
}
