package io.kahshe.watch.scan;

import io.kahshe.watch.Alerts;
import io.kahshe.common.Metrics;
import io.kahshe.watch.rules.WatchRules;

/**
 * The process state a scanner is configured with, once, before any table is scanned.
 *
 * <p>{@link WatchRules} is handed over rather than a snapshot of the rules, because rules hot
 * reload on the file's mtime: a scanner that cached the list would keep firing yesterday's
 * detections. {@link Alerts} is the shared payload builder and the shared per-(rule, file)
 * suppression, so alerts a scanner raises and alerts the index-riding engine raises are one
 * shape and one claim.
 *
 * @param rules the loaded rule set, re-read on every call
 * @param alerts the shared payload builder and suppression state
 * @param metrics the process metrics, for a scanner whose costs need counting
 * @param windowMaxKeys the live-key budget a window rule may hold (KAHSHE_WATCH_WINDOW_MAX_KEYS)
 * @param replayMaxFiles the file budget one table's first-sight replay may spend
 */
public record ScanContext(
    WatchRules rules, Alerts alerts, Metrics metrics, int windowMaxKeys, int replayMaxFiles) {

  /** For a caller with no stateful scanner to configure: fresh metrics and the default budgets. */
  public ScanContext(WatchRules rules, Alerts alerts) {
    this(rules, alerts, new Metrics(), 200_000, 2_000);
  }

  /** As above with explicit metrics and key budget; the replay budget takes its default. */
  public ScanContext(WatchRules rules, Alerts alerts, Metrics metrics, int windowMaxKeys) {
    this(rules, alerts, metrics, windowMaxKeys, 2_000);
  }
}
