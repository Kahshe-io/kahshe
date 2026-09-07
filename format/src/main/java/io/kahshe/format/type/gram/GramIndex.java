package io.kahshe.format.type.gram;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import io.kahshe.common.SingleFlight;
import io.kahshe.common.WeighedCache;
import java.io.IOException;
import java.io.InputStream;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import org.apache.iceberg.Table;
import org.apache.iceberg.io.InputFile;
import org.roaringbitmap.RoaringBitmap;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import io.kahshe.format.Coverage;
import io.kahshe.format.FormatConfig;
import io.kahshe.format.IndexPaths;
import io.kahshe.format.type.bloom.NgramBloom;
import io.kahshe.format.type.term.TermIndex;
import io.kahshe.format.type.term.TermIndexWriter;

/**
 * Loads and caches the term index's exact gram layer (gram bytes -> covered-file ordinal bitmap).
 * Freshness mirrors {@link TermIndex}: 30s TTL with metadata revalidation, single-flight cold
 * loads, poisoned failures, weighed negative entries. The revalidation token includes the grams
 * leaf's length so a rewrite at the same path is still observed.
 */
public final class GramIndex {
  private static final Logger LOG = LoggerFactory.getLogger(GramIndex.class);
  private static final long TTL_MS = 30_000;
  private static final ObjectMapper MAPPER = new ObjectMapper();

  public record Loaded(
      long snapshotId,
      int fromOrdinal,
      Map<String, Integer> ordinalOf,
      Map<GramIndexWriter.ByteKey, RoaringBitmap> grams,
      long weightBytes,
      Grams.Contract contract) {

    /**
     * Match bitmap over this layer's file ordinals, mirroring {@link NgramBloom#mightContain}
     * exactly: literals shorter than the gram size probe the whole-value key under EQ and cannot
     * prune otherwise (null); longer literals and their sliding grams in every mode. An absent
     * gram is an empty bitmap — the layer is exact, so no covered file contains it. Every
     * lowercase form from {@link NgramBloom#variants} is probed and the results ORed, so a
     * context-sensitive lowercasing (Greek final sigma) can never prune a truly-matching file.
     * Answers are meaningful only for ordinals at or past {@link #fromOrdinal}.
     */
    public RoaringBitmap matches(String literal, NgramBloom.Mode mode) {
      RoaringBitmap union = null;
      for (String form : NgramBloom.variants(literal)) {
        RoaringBitmap match = matchesForm(form, mode);
        if (match == null) {
          return null;
        }
        union = union == null ? match : RoaringBitmap.or(union, match);
      }
      return union;
    }

    private RoaringBitmap matchesForm(String form, NgramBloom.Mode mode) {
      if (contract.shorterThanWindow(form)) {
        return mode == NgramBloom.Mode.EQ ? bitmapOf(form) : null;
      }
      RoaringBitmap[] all = {null};
      contract.forEachWindow(form, gram -> {
        if (all[0] != null && all[0].isEmpty()) {
          return;
        }
        RoaringBitmap one = bitmapOf(gram);
        all[0] = all[0] == null ? one : RoaringBitmap.and(all[0], one);
      });
      return all[0];
    }

    private RoaringBitmap bitmapOf(String gram) {
      RoaringBitmap bitmap = grams.get(GramIndexWriter.ByteKey.of(gram));
      return bitmap == null ? new RoaringBitmap() : bitmap;
    }
  }

  private record CacheEntry(Optional<Loaded> index, String token, long loadedAt, long weightBytes) {}

  private final WeighedCache<String, CacheEntry> cache;
  private final SingleFlight<String> flight = new SingleFlight<>();
  private final FormatConfig config;
  private final io.kahshe.common.Metrics metrics;

  // injectable for TTL tests
  java.util.function.LongSupplier nowMs = System::currentTimeMillis;
  // injectable for over-budget tests: deserialized bitmaps cost roughly 3x their leaf bytes, and
  // WeighedCache clamp-retains an entry heavier than its whole budget — so refuse before loading
  public long maxLoadBytes;

  public GramIndex(FormatConfig config, io.kahshe.common.Metrics metrics) {
    this.config = config;
    this.metrics = metrics;
    this.maxLoadBytes = config.gramCacheBytes();
    this.cache =
        new WeighedCache<>(
            config.gramCacheBytes(), CacheEntry::weightBytes, metrics.gramCacheEvictions);
    metrics.gramCacheWeightBytes = cache::estimatedWeightBytes;
  }

  /**
   * Returns the gram layer for (table, fieldId), or null when none exists, none fits the budget,
   * or the metadata carries no grams leaf. TTL expiry re-reads only the metadata JSON plus the
   * leaf's length; the leaf is reloaded only when the layer changed.
   */
  public Loaded forField(Table table, int fieldId) {
    String key = table.location() + "#g" + fieldId;
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
    String token = token(table, fieldId);
    if (entry != null && entry.token().equals(token)) {
      CacheEntry renewed = new CacheEntry(entry.index(), token, now, entry.weightBytes());
      cache.put(key, renewed);
      return renewed;
    }
    if (token.startsWith("too-large|")) {
      // refused before loading; the negative entry holds until the artifact changes
      metrics.gramTooLarge.increment();
      LOG.warn("gram layer f{} exceeds the gram cache budget ({}); serving from blooms", fieldId, token);
      CacheEntry refused = new CacheEntry(Optional.empty(), token, now, negativeWeight(key, token));
      cache.put(key, refused);
      return refused;
    }
    Optional<Loaded> loaded = load(table, fieldId, key, token);
    // a failed load must not stick: poison the token so the next expiry retries
    String storeToken =
        loaded.isEmpty() && !"absent".equals(token) ? "load-failed-" + now : token;
    CacheEntry stored =
        new CacheEntry(
            loaded,
            storeToken,
            now,
            loaded.map(Loaded::weightBytes).orElse(negativeWeight(key, storeToken)));
    cache.put(key, stored);
    return stored;
  }

  /** Negative entries are weighed too, so probe storms on absent layers stay budget-bounded. */
  private static long negativeWeight(String key, String token) {
    return 2L * key.length() + 2L * token.length() + 464;
  }

  private String token(Table table, int fieldId) {
    try {
      org.apache.iceberg.io.FileIO io = IndexPaths.io(table, config);
      InputFile metaFile = io.newInputFile(
          TermIndexWriter.dir(IndexPaths.root(table, config.indexRoot()), fieldId) + "/index-metadata.json");
      if (!metaFile.exists()) {
        return "absent";
      }
      JsonNode document;
      try (InputStream in = metaFile.newStream()) {
        document = MAPPER.readTree(in);
      }
      if (TermIndexWriter.newerThanThisReader(document)) {
        return "absent"; // a newer term document: what its grams leaf means is not known here
      }
      JsonNode snapshot = TermIndexWriter.snapshotNode(document);
      String gramsPath = snapshot.path("leaves").path("grams").asText();
      if (gramsPath.isEmpty()) {
        return "absent";
      }
      long snapshotId =
          snapshot.path("source-table-snapshot-id").asLong(snapshot.path("snapshotId").asLong());
      String base = snapshotId + "|" + gramsPath + "|" + io.newInputFile(gramsPath).getLength();
      long leafBytes = snapshot.path("gram-coverage").path("bytes").asLong();
      return 3 * leafBytes > maxLoadBytes ? "too-large|" + base : base;
    } catch (IOException | RuntimeException e) {
      return "unreadable";
    }
  }

  private Optional<Loaded> load(Table table, int fieldId, String key, String token) {
    try {
      org.apache.iceberg.io.FileIO io = IndexPaths.io(table, config);
      InputFile metaFile = io.newInputFile(
          TermIndexWriter.dir(IndexPaths.root(table, config.indexRoot()), fieldId) + "/index-metadata.json");
      if (!metaFile.exists()) {
        return Optional.empty();
      }
      JsonNode document;
      try (InputStream in = metaFile.newStream()) {
        document = MAPPER.readTree(in);
      }
      if (TermIndexWriter.newerThanThisReader(document)) {
        // The same refusal TermIndex.load makes, for the same reason: the leaf this document
        // names may not mean what this reader thinks; keeping every file is the safe answer.
        LOG.warn("gram layer f{} names a term document newer than this reader; ignoring it", fieldId);
        return Optional.empty();
      }
      JsonNode snapshot = TermIndexWriter.snapshotNode(document);
      String gramsPath = snapshot.path("leaves").path("grams").asText();
      if (gramsPath.isEmpty()) {
        return Optional.empty();
      }
      // live entries only, and each at the ordinal the metadata records rather than its position.
      // A tombstoned file is absent from this map, so the pruner keeps it -- which is the safe
      // direction and, since the file is gone from the table, never actually reached.
      List<Coverage.Entry> coverage = Coverage.parse(snapshot.path("files"));
      Map<String, Integer> ordinalOf = Coverage.ordinalOfLive(coverage);
      // calibrated per-entry heap estimate: gram bytes, bitmap payload, map/record overhead
      long weightBytes = 1024L + 2L * key.length() + 2L * token.length() + 400;
      for (String path : ordinalOf.keySet()) {
        weightBytes += 2L * path.length() + 200;
      }
      Map<GramIndexWriter.ByteKey, RoaringBitmap> grams = new HashMap<>();
      GramIndexWriter.readLeaf(io, gramsPath, grams);
      for (Map.Entry<GramIndexWriter.ByteKey, RoaringBitmap> e : grams.entrySet()) {
        weightBytes += e.getKey().bytes.length + 112 + (long) (1.5 * e.getValue().serializedSizeInBytes());
      }
      int fromOrdinal = snapshot.path("gram-coverage").path("from-ordinal").asInt();
      long snapshotId =
          snapshot.path("source-table-snapshot-id").asLong(snapshot.path("snapshotId").asLong());
      LOG.info("loaded gram layer f{}: {} grams, from ordinal {}, {} live file(s) of {} covered, snapshot {}",
          fieldId, grams.size(), fromOrdinal, ordinalOf.size(), coverage.size(), snapshotId);
      // The rule this leaf was cut under: absent means v1, the rule before it had an id.
      Grams.Contract contract =
          Grams.Contract.of(TermIndexWriter.propertiesNode(document).path("grams").asText(null));
      return Optional.of(new Loaded(snapshotId, fromOrdinal, ordinalOf, grams, weightBytes, contract));
    } catch (IOException | RuntimeException e) {
      LOG.warn("failed to load gram layer f{}; ignoring", fieldId, e);
      return Optional.empty();
    }
  }
}
