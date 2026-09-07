package io.kahshe.indexer.maintain;

import io.kahshe.common.Metrics;
import io.kahshe.common.BoundedCache;
import java.util.List;
import java.util.Map;
import java.util.TreeMap;
import java.util.function.Consumer;
import java.util.function.LongSupplier;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Per-table index staleness, so a scrape can tell "up to date" apart from "quietly stopped
 * indexing" — the failure that costs correctness while producing no error, no log and no counter
 * movement. For every (prefix, namespace, table) observed declaring an index column it keeps the
 * snapshot last seen, the snapshot last covered by a completed maintenance pass, and when each
 * happened; the derived gauges are registered on {@link Metrics}, and {@link #entries()} hands
 * the same set to the admin port's per-table status.
 *
 * <p>Per-process and not durable: a restart legitimately resets it to empty, and a replica that
 * never sees a table's loadTable traffic never tracks that table. Bounded to {@link #MAX_TRACKED}
 * entries (LRU), so a large catalog cannot grow it without limit.
 *
 * <p>A process whose indexer is off still tracks what it observes ({@code tables_tracked} stays
 * honest) but reports nothing behind and warns of nothing: it cannot close the gap, so the gap is
 * not its to raise. The module README says why that is the right reading.
 *
 * <p>Updates are best effort under concurrent observations of one table: a lost read-modify-write
 * is corrected by the next observation, matching the benign racing of the indexer's dedup map.
 */
public final class IndexFreshness {
  private static final Logger LOG = LoggerFactory.getLogger(IndexFreshness.class);

  /** Entry cap; tables past it fall out LRU-first and are re-tracked on their next observation. */
  static final int MAX_TRACKED = 4096;

  private static final long UNSET = Long.MIN_VALUE;

  /**
   * One table's freshness. Times are wall-clock ms, 0 = never. A table is behind while its
   * observation is uncovered. Carries its own key so {@link #entries()} can hand the tracked set
   * out from the cache's value copy, which is the only view the cache gives.
   */
  public record Entry(
      String key,
      long observedSnapshot,
      long observedAtMs,
      long builtSnapshot,
      long builtAtMs,
      long behindSinceMs,
      long lastWarnAtMs) {
    public boolean behind() {
      return observedSnapshot != builtSnapshot;
    }

    /** False until a maintenance pass has covered this table: {@link #builtSnapshot} is then noise. */
    public boolean hasBuild() {
      return builtSnapshot != UNSET;
    }
  }

  private final BoundedCache<String, Entry> tracked = new BoundedCache<>(MAX_TRACKED);
  private final long staleWarnMs;
  /** Whether this process builds: false reads nothing behind, whatever was observed. */
  private final boolean builds;

  // injectable for staleness tests
  LongSupplier nowMs = System::currentTimeMillis;
  Consumer<String> warnSink = LOG::warn;

  public IndexFreshness(Metrics metrics, long staleWarnMs) {
    this(metrics, staleWarnMs, true);
  }

  public IndexFreshness(Metrics metrics, long staleWarnMs, boolean builds) {
    this.staleWarnMs = staleWarnMs;
    this.builds = builds;
    metrics.indexTablesTracked = tracked::size;
    metrics.indexTablesBehind = this::tablesBehind;
    metrics.indexMaxBehindSeconds = this::maxBehindSeconds;
    metrics.indexLastBuildAgeSeconds = this::lastBuildAgeSeconds;
    metrics.indexBehindByTable = this::behindByTable;
  }

  /**
   * Records a loadTable observation. Called for every observation, including the ones the indexer
   * dedups away — a table that is genuinely up to date must read as tracked-and-current, not as
   * untracked. Re-observing a snapshot already covered by a build is current, not behind; a
   * repeated observation of a still-uncovered snapshot keeps the moment it was first seen, so the
   * behind age measures the real gap rather than restarting on every request.
   */
  public void observed(String key, long snapshotId) {
    long now = nowMs.getAsLong();
    Entry existing = tracked.get(key);
    Entry updated;
    if (existing == null) {
      updated = new Entry(key, snapshotId, now, UNSET, 0, now, 0);
    } else if (existing.observedSnapshot() == snapshotId) {
      updated =
          new Entry(
              key,
              snapshotId,
              now,
              existing.builtSnapshot(),
              existing.builtAtMs(),
              existing.behindSinceMs(),
              existing.lastWarnAtMs());
    } else if (snapshotId == existing.builtSnapshot()) {
      updated =
          new Entry(key, snapshotId, now, existing.builtSnapshot(), existing.builtAtMs(), 0, 0);
    } else {
      updated =
          new Entry(
              key,
              snapshotId,
              now,
              existing.builtSnapshot(),
              existing.builtAtMs(),
              now,
              existing.lastWarnAtMs());
    }
    tracked.put(key, updated);
    maybeWarn(key, updated, now);
  }

  /**
   * Records a completed maintenance pass: {@code observedSnapshot} is the snapshot the queued job
   * carried, {@code builtSnapshot} the snapshot actually covered (the worker re-reads the live
   * table, so an out-of-band commit makes the two differ). A rebuild and a verification that the
   * existing artifacts already cover the snapshot both count — both mean maintenance ran to
   * completion for this table, which is what the age gauges are asked about.
   *
   * <p>An observation that arrived while the build ran is not satisfied by it: the table stays
   * behind, dated from that newer observation, until a pass covers it.
   */
  public void built(String key, long observedSnapshot, long builtSnapshot) {
    long now = nowMs.getAsLong();
    Entry existing = tracked.get(key);
    if (existing == null) {
      tracked.put(key, new Entry(key, builtSnapshot, now, builtSnapshot, now, 0, 0));
      return;
    }
    boolean satisfied =
        existing.observedSnapshot() == observedSnapshot
            || existing.observedSnapshot() == builtSnapshot;
    tracked.put(
        key,
        satisfied
            ? new Entry(key, builtSnapshot, now, builtSnapshot, now, 0, 0)
            : new Entry(
                key,
                existing.observedSnapshot(),
                existing.observedAtMs(),
                builtSnapshot,
                now,
                existing.behindSinceMs(),
                existing.lastWarnAtMs()));
  }

  /**
   * The worker gave up on this table and will retry on the next observation. Mirrors the indexer
   * clearing its dedup entry: an entry that flips back to unhandled must not keep reading as
   * current, or a failing build loop would look exactly like a healthy one.
   */
  public void failed(String key) {
    Entry existing = tracked.get(key);
    if (existing == null || existing.behind()) {
      return; // already behind: keep the older behind-since, which is the honest one
    }
    tracked.put(
        key,
        new Entry(
            key,
            existing.observedSnapshot(),
            existing.observedAtMs(),
            UNSET,
            existing.builtAtMs(),
            nowMs.getAsLong(),
            existing.lastWarnAtMs()));
  }

  long tablesBehind() {
    if (!builds) {
      return 0;
    }
    long behind = 0;
    for (Entry entry : tracked.values()) {
      if (entry.behind()) {
        behind++;
      }
    }
    return behind;
  }

  /**
   * The largest gap between now and the moment a still-uncovered snapshot was first observed,
   * rounded up so any table that is behind reports at least 1: zero means nothing is behind, with
   * no ambiguity for an alert rule that fires on {@code > 0}.
   */
  long maxBehindSeconds() {
    if (!builds) {
      return 0;
    }
    long now = nowMs.getAsLong();
    long max = 0;
    for (Entry entry : tracked.values()) {
      if (entry.behind()) {
        max = Math.max(max, behindSeconds(now, entry.behindSinceMs()));
      }
    }
    return max;
  }

  /**
   * The tables currently behind, as {@code namespace.table} to their {@link #behindSeconds}, for
   * the per-table gauge. A current table has no entry, so the family lists only what needs
   * attention; the prefix is dropped because it is a REST tenancy segment, not part of the name a
   * query uses. Empty on a process that does not build, as the fleet gauges are.
   */
  Map<String, Long> behindByTable() {
    if (!builds) {
      return Map.of();
    }
    Map<String, Long> behind = new TreeMap<>();
    for (Entry entry : tracked.values()) {
      if (entry.behind()) {
        behind.put(tableLabel(entry.key()), behindSeconds(entry));
      }
    }
    return behind;
  }

  /**
   * {@code namespace.table}, decoded, from a {@code prefix|namespace|table} key of raw path
   * segments. A segment that does not decode keeps its raw form: a scrape must not fail over one
   * odd table name.
   */
  static String tableLabel(String key) {
    String[] parts = key.split("\\|", 3);
    if (parts.length < 3) {
      return key;
    }
    try {
      return IndexerService.decodeIdent(parts[1], parts[2]).toString();
    } catch (IllegalArgumentException e) {
      return parts[1] + "." + parts[2];
    }
  }

  /**
   * Age of the most recent completed maintenance pass across tracked tables. 0 when nothing has
   * been built yet: a never-built process and a just-built one both read 0, so this is a
   * supporting signal only — {@link #maxBehindSeconds()} is the one to alert on.
   */
  long lastBuildAgeSeconds() {
    long newest = 0;
    for (Entry entry : tracked.values()) {
      newest = Math.max(newest, entry.builtAtMs());
    }
    return newest == 0 ? 0 : Math.max(0, nowMs.getAsLong() - newest) / 1000;
  }

  /**
   * The tracked set as it stands: an immutable copy of immutable entries, so a reader on the admin
   * port can walk it while observations and builds keep landing, and can change nothing.
   */
  public List<Entry> entries() {
    return tracked.values();
  }

  /**
   * How long {@code entry} has been behind, by the rule {@link #maxBehindSeconds()} applies to
   * the fleet gauge: 0 when current, at least 1 when not, and 0 on a process that does not
   * build, which is not the one to raise the gap.
   */
  public long behindSeconds(Entry entry) {
    if (!builds || !entry.behind()) {
      return 0;
    }
    return behindSeconds(nowMs.getAsLong(), entry.behindSinceMs());
  }

  private static long behindSeconds(long now, long behindSinceMs) {
    return Math.max(1, (Math.max(0, now - behindSinceMs) + 999) / 1000);
  }

  /** At most one warning per table per warn interval, and only once the threshold is crossed. */
  private void maybeWarn(String key, Entry entry, long now) {
    if (!builds || !entry.behind() || now - entry.behindSinceMs() < staleWarnMs) {
      return;
    }
    if (entry.lastWarnAtMs() != 0 && now - entry.lastWarnAtMs() < staleWarnMs) {
      return;
    }
    warnSink.accept(
        String.format(
            "index for %s behind for %ds: observed snapshot %d, last built %s"
                + " — no loadTable traffic reaching the proxy, indexer disabled, or builds failing",
            key,
            (now - entry.behindSinceMs()) / 1000,
            entry.observedSnapshot(),
            entry.builtSnapshot() == UNSET ? "none" : String.valueOf(entry.builtSnapshot())));
    tracked.put(
        key,
        new Entry(
            key,
            entry.observedSnapshot(),
            entry.observedAtMs(),
            entry.builtSnapshot(),
            entry.builtAtMs(),
            entry.behindSinceMs(),
            now));
  }
}
