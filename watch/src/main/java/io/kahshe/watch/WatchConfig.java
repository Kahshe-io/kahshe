package io.kahshe.watch;

/**
 * What alerting needs: the rules file, which sink delivers, the webhook and its delivery bounds,
 * discovery's poll, and the row scan's switch and width. Every sink reads this same record —
 * {@code sink} picks one, and the fields a sink needs are its own to read, which is why the
 * webhook's three live here rather than behind an accessor only it implements.
 *
 * @param watchScan whether the row scan runs at all; false leaves only the index-riding path
 * @param watchScanThreads how many data files the scan reads at once
 * @param watchWindowMaxKeys the most live keys a window rule may track before the oldest are
 *     evicted; an eviction loses a partial window, which is a MISS, so it is counted
 * @param watchReplayMaxFiles the most files a first-sight replay may read per table before it
 *     stops; stopping leaves state unrebuilt, which is also a miss, so it is loud
 */
public record WatchConfig(
    String watchRulesPath,
    String sink,
    String watchWebhook,
    String watchWebhookAuth,
    long watchWebhookTimeoutMs,
    long watchPollMs,
    String watchSqlCatalog,
    boolean watchRealertOnRebuild,
    boolean watchScan,
    int watchScanThreads,
    int watchWindowMaxKeys,
    int watchReplayMaxFiles) {

  /** With the default window-key and replay budgets. */
  public WatchConfig(
      String watchRulesPath,
      String sink,
      String watchWebhook,
      String watchWebhookAuth,
      long watchWebhookTimeoutMs,
      long watchPollMs,
      String watchSqlCatalog,
      boolean watchRealertOnRebuild,
      boolean watchScan,
      int watchScanThreads) {
    this(watchRulesPath, sink, watchWebhook, watchWebhookAuth, watchWebhookTimeoutMs, watchPollMs,
        watchSqlCatalog, watchRealertOnRebuild, watchScan, watchScanThreads, 200_000, 2_000);
  }
}
