package io.kahshe.common;

import com.github.benmanes.caffeine.cache.Cache;
import com.github.benmanes.caffeine.cache.Caffeine;
import java.util.concurrent.atomic.AtomicLong;
import java.util.concurrent.atomic.LongAdder;
import java.util.function.ToLongFunction;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Byte-weighted cache over Caffeine: each entry carries a precomputed byte weight and the cache
 * holds a total byte budget, so one huge index cannot blow the heap the way an entry-count bound
 * allows. Weights are kept in KiB and {@link #estimatedWeightBytes} converts back, so gauges
 * report KiB-rounded bytes. An entry heavier than the whole budget is clamped to the budget and
 * stays resident, evicting everything else; a caller that would rather not hold such an entry must
 * refuse it before putting it in. Nothing expires here — freshness stays with the callers.
 */
public final class WeighedCache<K, V> {
  private static final Logger LOG = LoggerFactory.getLogger(WeighedCache.class);
  private static final long OVERSIZED_WARN_INTERVAL_MS = 60_000;

  private final Cache<K, V> cache;
  private final AtomicLong lastOversizedWarnMs = new AtomicLong();

  /**
   * @param weightBytes reads the per-value stored weight; results are clamped to [1 KiB, budget]
   * @param evictions incremented only for capacity evictions, not explicit removals
   */
  public WeighedCache(long budgetBytes, ToLongFunction<V> weightBytes, LongAdder evictions) {
    long budgetKib = Math.max(1, budgetBytes / 1024);
    this.cache =
        Caffeine.newBuilder()
            .maximumWeight(budgetKib)
            .weigher(
                (K key, V value) -> {
                  long bytes = weightBytes.applyAsLong(value);
                  long kib = Math.max(1, Math.min((bytes + 1023) / 1024, Integer.MAX_VALUE));
                  if (kib > budgetKib) {
                    warnOversized(bytes, budgetBytes);
                    kib = budgetKib;
                  }
                  return (int) kib;
                })
            // same-thread maintenance: evictions (and their counts) are visible as soon as the
            // triggering write returns
            .executor(Runnable::run)
            .removalListener(
                (K key, V value, com.github.benmanes.caffeine.cache.RemovalCause cause) -> {
                  if (cause.wasEvicted()) {
                    evictions.increment();
                  }
                })
            .build();
  }

  public V get(K key) {
    return cache.getIfPresent(key);
  }

  public void put(K key, V value) {
    cache.put(key, value);
  }

  public void remove(K key) {
    cache.invalidate(key);
  }

  public long estimatedWeightBytes() {
    return cache.policy().eviction().map(e -> e.weightedSize().orElse(0L)).orElse(0L) * 1024;
  }

  void cleanUp() {
    cache.cleanUp();
  }

  private void warnOversized(long weightBytes, long budgetBytes) {
    long now = System.currentTimeMillis();
    long last = lastOversizedWarnMs.get();
    if (now - last >= OVERSIZED_WARN_INTERVAL_MS && lastOversizedWarnMs.compareAndSet(last, now)) {
      LOG.warn(
          "cache entry weighs {} bytes, more than the whole {}-byte budget; retaining it anyway",
          weightBytes,
          budgetBytes);
    }
  }
}
