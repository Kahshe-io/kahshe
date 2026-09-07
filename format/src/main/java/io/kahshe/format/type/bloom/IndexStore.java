package io.kahshe.format.type.bloom;

import com.fasterxml.jackson.databind.ObjectMapper;
import io.kahshe.common.SingleFlight;
import io.kahshe.common.WeighedCache;
import java.io.IOException;
import java.io.InputStream;
import java.nio.ByteBuffer;
import java.util.HashMap;
import java.util.Map;
import java.util.Optional;
import org.apache.iceberg.Table;
import org.apache.iceberg.data.Record;
import org.apache.iceberg.data.parquet.GenericParquetReaders;
import org.apache.iceberg.io.CloseableIterable;
import org.apache.iceberg.io.InputFile;
import org.apache.iceberg.parquet.Parquet;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import io.kahshe.format.FormatConfig;
import io.kahshe.format.IndexPaths;
import io.kahshe.format.type.gram.GramIndex;
import io.kahshe.format.type.gram.Grams;

/** Loads and caches per-file bloom indexes. A missing index is cached too (negative caching). */
public final class IndexStore {
  private static final Logger LOG = LoggerFactory.getLogger(IndexStore.class);
  private static final long TTL_MS = 30_000;
  private static final ObjectMapper MAPPER = new ObjectMapper();

  public record LoadedIndex(IndexMeta meta, Map<String, NgramBloom> blooms, long weightBytes) {}

  public static java.util.List<String> leaves(IndexMeta meta) {
    return meta.leafFiles;
  }

  private record CacheEntry(
      Optional<LoadedIndex> index, String fingerprint, long loadedAt, long weightBytes) {}

  private final WeighedCache<String, CacheEntry> cache;
  private final SingleFlight<String> flight = new SingleFlight<>();

  /**
   * Bloom leaf-list length per (table, column), for {@code kahshe_index_bloom_leaves}.
   *
   * <p>A write appends one leaf and {@link #load} opens every leaf in the list on a cold read, so
   * an uncompacted tier costs one round trip per leaf on every content change, and no other signal
   * moves when it grows.
   */
  private final java.util.concurrent.ConcurrentHashMap<String, Integer> bloomLeaves =
      new java.util.concurrent.ConcurrentHashMap<>();

  /**
   * Refuse a bloom tier heavier than the cache that is supposed to hold it.
   *
   * <p>{@link io.kahshe.common.WeighedCache} clamp-retains an entry heavier than its whole budget:
   * it cannot evict the only thing present, so an over-budget artifact is loaded in full and then
   * pinned. The estimate is the writer's own recorded {@code index-bytes} scaled by
   * {@link #HEAP_PER_BLOOM_BYTE}, checked against the cache budget before any leaf is opened --
   * refusing after deserializing would have already paid the cost being avoided.
   */
  // package-private and non-final so a test can shrink it: the cache budget has an 8 MiB floor, so
  // no configuration can express a budget small enough to trip this on a fixture-sized tier
  long maxLoadBytes;

  private final io.kahshe.common.Metrics metrics;

  /**
   * Heap cost per byte of serialized bloom. Each bloom becomes a {@code long[]} plus an object
   * header, and each entry a map node keyed by a data-file path, so the in-heap figure runs well
   * above the payload. Erring high refuses early and keeps serving from the gram tier, which is
   * correct and merely slower; erring low is the pin this exists to prevent.
   */
  private static final double HEAP_PER_BLOOM_BYTE = 2.0;
  private final FormatConfig config;

  // injectable for TTL tests
  java.util.function.LongSupplier nowMs = System::currentTimeMillis;

  public IndexStore(FormatConfig config, io.kahshe.common.Metrics metrics) {
    this.config = config;
    this.metrics = metrics;
    this.maxLoadBytes = config.indexCacheBytes();
    this.cache =
        new WeighedCache<>(
            config.indexCacheBytes(), CacheEntry::weightBytes, metrics.indexCacheEvictions);
    metrics.indexCacheWeightBytes = cache::estimatedWeightBytes;
    metrics.indexBloomLeaves =
        () -> bloomLeaves.values().stream().mapToLong(Integer::longValue).max().orElse(0L);
  }

  /**
   * Returns the index for (table, column), or null when none exists. On TTL expiry only the small
   * metadata JSON is re-read; leaves are reloaded only when the index actually changed. Concurrent
   * cold loads of the same key share one flight; different keys still load concurrently.
   *
   * <p>The column name is not pattern-validated: it reaches no path this class builds, only a
   * cache key and a schema lookup, and Iceberg permits spaces and unicode in column names.
   */
  public LoadedIndex forColumn(Table table, String column) {
    String key = table.location() + "#" + column;
    CacheEntry entry = fresh(key);
    if (entry == null) {
      entry = flight.load(key, () -> fresh(key), () -> refresh(table, column, key));
    }
    return entry.index().orElse(null);
  }

  private CacheEntry fresh(String key) {
    CacheEntry entry = cache.get(key);
    return entry != null && nowMs.getAsLong() - entry.loadedAt() <= TTL_MS ? entry : null;
  }

  private CacheEntry refresh(Table table, String column, String key) {
    CacheEntry entry = cache.get(key);
    long now = nowMs.getAsLong();
    String fingerprint = fingerprint(table, column, config);
    if (entry != null && entry.fingerprint().equals(fingerprint)) {
      CacheEntry renewed = new CacheEntry(entry.index(), fingerprint, now, entry.weightBytes());
      cache.put(key, renewed);
      return renewed;
    }
    Optional<LoadedIndex> loaded = load(table, column, key, fingerprint);
    // a failed load must not stick: poison the fingerprint so the next expiry retries
    String storeFingerprint =
        loaded.isEmpty() && !"absent".equals(fingerprint) ? "load-failed-" + now : fingerprint;
    CacheEntry stored =
        new CacheEntry(loaded, storeFingerprint, now, entryWeight(loaded, key, storeFingerprint));
    cache.put(key, stored);
    return stored;
  }

  /** Negative entries are weighed too, so probe storms on absent indexes stay budget-bounded. */
  private static long entryWeight(Optional<LoadedIndex> index, String key, String fingerprint) {
    return index
        .map(LoadedIndex::weightBytes)
        .orElse(2L * key.length() + 2L * fingerprint.length() + 464);
  }

  /**
   * The declared column's Iceberg field id, or -1 when the current schema has no such column —
   * which is what a renamed or dropped column looks like from here. Callers treat -1 as "no index",
   * which keeps every file: the safe answer, never a wrong one.
   */
  private static int fieldId(Table table, String column) {
    var field = table.schema().findField(column);
    return field == null ? -1 : field.fieldId();
  }

  private static String fingerprint(Table table, String column, FormatConfig config) {
    try {
      int fieldId = fieldId(table, column);
      if (fieldId < 0) {
        return "absent";
      }
      InputFile metaFile = IndexPaths.io(table, config)
          .newInputFile(IndexMeta.metaPath(IndexPaths.root(table, config.indexRoot()), fieldId));
      if (!metaFile.exists()) {
        return "absent";
      }
      try (InputStream in = metaFile.newStream()) {
        IndexMeta meta = IndexMeta.parse(MAPPER.readTree(in));
        return meta.snapshotId + "|" + String.join(",", leaves(meta));
      }
    } catch (IOException | RuntimeException e) {
      return "unreadable";
    }
  }

  private Optional<LoadedIndex> load(Table table, String column, String key, String fingerprint) {
    try {
      int fieldId = fieldId(table, column);
      if (fieldId < 0) {
        return Optional.empty();
      }
      org.apache.iceberg.io.FileIO io = IndexPaths.io(table, config);
      InputFile metaFile =
          io.newInputFile(IndexMeta.metaPath(IndexPaths.root(table, config.indexRoot()), fieldId));
      if (!metaFile.exists()) {
        return Optional.empty();
      }
      IndexMeta meta;
      try (InputStream in = metaFile.newStream()) {
        meta = IndexMeta.parse(MAPPER.readTree(in));
      }
      long estimatedHeap = (long) (HEAP_PER_BLOOM_BYTE * meta.indexBytes);
      if (estimatedHeap > maxLoadBytes) {
        // before opening a leaf: loading it would pin more than the whole cache budget, and an
        // absent bloom keeps files rather than skipping them, so the cost is a scan, not an answer
        metrics.indexTooLarge.increment();
        LOG.warn(
            "bloom tier for column {} is ~{} MiB of heap against a {} MiB cache budget; refusing "
                + "to load it. contains pruning falls back to the gram tier for covered files and "
                + "keeps everything else. Raise KAHSHE_CACHE_BYTES or rebuild with fewer files.",
            column, estimatedHeap >> 20, maxLoadBytes >> 20);
        return Optional.empty();
      }
      Map<String, NgramBloom> blooms = new HashMap<>();
      // calibrated per-entry heap estimate: bloom payload + path chars + map/record overhead
      long weightBytes = 1024L + 2L * key.length() + 2L * fingerprint.length() + 400;
      for (String leaf : leaves(meta)) {
        try (CloseableIterable<Record> records =
            Parquet.read(io.newInputFile(leaf))
                .project(BloomLeaf.LEAF_SCHEMA)
                .createReaderFunc(
                    fileSchema -> GenericParquetReaders.buildReader(BloomLeaf.LEAF_SCHEMA, fileSchema))
                .build()) {
          for (Record record : records) {
            ByteBuffer buffer = (ByteBuffer) record.getField("bloom");
            byte[] bytes = new byte[buffer.remaining()];
            buffer.duplicate().get(bytes);
            String path = record.getField("data_file_path").toString();
            NgramBloom bloom = NgramBloom.deserialize(bytes, Grams.Contract.of(meta.grams).rule());
            blooms.put(path, bloom);
            weightBytes += bloom.sizeBytes() + 2L * path.length() + 152;
          }
        }
      }
      bloomLeaves.put(key, leaves(meta).size());
      LOG.info("loaded index {} ({} files, {} leaf/leaves) for {}",
          meta.indexType, blooms.size(), leaves(meta).size(), column);
      return Optional.of(new LoadedIndex(meta, blooms, weightBytes));
    } catch (IOException | RuntimeException e) {
      LOG.warn("failed to load index for column {}; ignoring index", column, e);
      return Optional.empty();
    }
  }
}
