package io.kahshe.watch.scan;

import io.kahshe.common.Metrics;
import java.util.LinkedHashMap;
import java.util.Map;

/**
 * Exact "N within T" per key, in bounded memory.
 *
 * <p>Per key, the {@code N} most recent event times, kept ascending in a {@code long[]} of exactly
 * {@code N} slots — enough to decide the threshold EXACTLY, since a qualifying window ends at some
 * event and the array holds those N or a tighter set when that event is offered. Memory per key is
 * therefore N longs whatever the traffic; what it grows with is KEYS, hence the cap and the LRU
 * within each shard.
 *
 * <p>An event older than everything retained cannot be placed without evicting something newer, so
 * it is dropped; a key evicted under the cap loses its partial window. Both are MISSES rather than
 * delays and both are counted ({@code kahshe_watch_window_late_drops_total},
 * {@code kahshe_watch_window_key_evictions_total}). After a trip the key's slots are cleared, so a
 * rule fires once per N matching events rather than once per event thereafter.
 *
 * <p>Sharded by key hash with a lock per shard rather than one lock per rule: several scan threads
 * offer to one rule's counters, the row path is a few tens of nanoseconds, and a single lock would
 * serialise every one of them through it.
 */
final class WindowCounters {

  /** Enough that scan threads rarely meet; a power of two so the mask is a mask. */
  private static final int SHARDS = 16;

  private final int count;
  private final long timeframeMs;
  private final Metrics metrics;
  private final Shard[] shards = new Shard[SHARDS];

  WindowCounters(int count, long timeframeMs, int maxKeys, Metrics metrics) {
    this.count = count;
    this.timeframeMs = timeframeMs;
    this.metrics = metrics;
    int perShard = Math.max(1, maxKeys / SHARDS);
    for (int i = 0; i < SHARDS; i++) {
      shards[i] = new Shard(perShard);
    }
  }

  /**
   * Offers one matching event.
   *
   * @param key the group-by key, or a constant when the rule groups by nothing
   * @param tsMs the event's time
   * @return the window that just tripped, or null
   */
  Trip offer(String key, long tsMs) {
    Shard shard = shards[(key.hashCode() >>> 16 ^ key.hashCode()) & (SHARDS - 1)];
    synchronized (shard) {
      long[] slots = shard.keys.get(key);
      if (slots == null) {
        slots = new long[count];
        java.util.Arrays.fill(slots, Long.MIN_VALUE);
        shard.keys.put(key, slots);
      }
      boolean full = slots[0] != Long.MIN_VALUE;
      if (full && tsMs < slots[0]) {
        metrics.watchWindowLateDrops.increment();
        return null;
      }
      // shift-left insert, ascending, dropping the oldest retained
      int i = 0;
      while (i + 1 < count && slots[i + 1] < tsMs) {
        slots[i] = slots[i + 1];
        i++;
      }
      slots[i] = tsMs;
      if (slots[0] == Long.MIN_VALUE || slots[count - 1] - slots[0] > timeframeMs) {
        return null;
      }
      Trip trip = new Trip(slots[0], slots[count - 1]);
      java.util.Arrays.fill(slots, Long.MIN_VALUE); // fire once per N, not once per event after
      metrics.watchWindowTrips.increment();
      return trip;
    }
  }

  /** Live keys across the shards; the gauge behind {@code kahshe_watch_window_keys}. */
  long keys() {
    long total = 0;
    for (Shard shard : shards) {
      synchronized (shard) {
        total += shard.keys.size();
      }
    }
    return total;
  }

  /** The window that tripped: the span its N events actually covered. */
  record Trip(long startMs, long endMs) {}

  /** One lock's worth of keys, LRU-capped. */
  private final class Shard {
    private final Map<String, long[]> keys;

    Shard(int cap) {
      this.keys =
          new LinkedHashMap<>(16, 0.75f, true) {
            @Override
            protected boolean removeEldestEntry(Map.Entry<String, long[]> eldest) {
              if (size() <= cap) {
                return false;
              }
              // A partial window thrown away: a miss, and counted as one.
              metrics.watchWindowKeyEvictions.increment();
              return true;
            }
          };
    }
  }
}
