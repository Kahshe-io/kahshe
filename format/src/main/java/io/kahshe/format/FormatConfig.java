package io.kahshe.format;

/**
 * What a reader or writer of the index artifact needs and nothing a build needs: where the index
 * lives and how to reach it, the tiers this deployment serves at most (a ceiling, never a
 * default; see {@code IndexSettings}), the shared cache budget and its fixed shares, and the
 * prefix cap. The environment names and defaults behind these fields are in
 * {@code Kahshe.Config.fromEnv}; nothing in this module reads the environment itself.
 *
 * @param indexIoImpl the {@code FileIO} implementation class for the index root, resolved the way
 *     Iceberg resolves one: by class name plus dotted properties. Empty means the table's own IO.
 *     The {@code KAHSHE_INDEX_S3_*} settings are a convenience that maps onto the same dotted keys;
 *     see the app's {@code IndexIo}
 * @param indexIoProperties Iceberg-dotted properties for that implementation, e.g.
 *     {@code s3.endpoint}
 * @param dataIo "table" reads data files through the table's own IO; "index" through the index
 *     client
 * @param cacheBytes the process-wide cache budget the four shares below are cut from
 * @param prefixMaxTerms the most terms a prefix query may union before the proxy keeps every file
 *     instead
 */
public record FormatConfig(
    String indexRoot,
    String indexS3Endpoint,
    String indexS3AccessKey,
    String indexS3SecretKey,
    String indexS3Region,
    String indexIoImpl,
    java.util.Map<String, String> indexIoProperties,
    String dataIo,
    long cacheBytes,
    boolean gramIndexEnabled,
    boolean termIndexEnabled,
    int prefixMaxTerms) {

  /** Neither IO field is optional to read: an absent impl is "", absent properties are empty. */
  public FormatConfig {
    indexIoImpl = indexIoImpl == null ? "" : indexIoImpl;
    indexIoProperties =
        indexIoProperties == null ? java.util.Map.of() : java.util.Map.copyOf(indexIoProperties);
  }

  private static final long CACHE_FLOOR_BYTES = 8L * 1024 * 1024;

  // divide first: multiply-first wraps for budgets near Long.MAX_VALUE (maxMemory() can be
  // Long.MAX_VALUE under some JVM configs)
  public long indexCacheBytes() {
    return Math.max(CACHE_FLOOR_BYTES, cacheBytes / 100 * 40);
  }

  public long termCacheBytes() {
    return Math.max(CACHE_FLOOR_BYTES, cacheBytes / 100 * 25);
  }

  /**
   * The plan cache's share. It is cut here, beside the three the readers use, because the budget
   * is one number for the whole process and a build's heap check ({@code BuildBudget}) reserves
   * this share whether or not a proxy shares the heap.
   */
  public long planCacheBytes() {
    return Math.max(CACHE_FLOOR_BYTES, cacheBytes / 100 * 20);
  }

  public long gramCacheBytes() {
    return Math.max(CACHE_FLOOR_BYTES, cacheBytes / 100 * 15);
  }
}
