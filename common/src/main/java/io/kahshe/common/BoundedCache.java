package io.kahshe.common;

import java.util.LinkedHashMap;
import java.util.Map;
import java.util.function.Function;

/**
 * Tiny synchronized LRU — every kahshe cache is bounded. Access-ordered, so a {@link #get} counts
 * as a use, and {@code get} returns null for a key the cache does not hold. Every method takes the
 * one monitor, so a slow load inside {@link #computeIfAbsent} blocks every other caller: use
 * {@link SingleFlight} instead when loads are slow and keys are independent.
 */
public final class BoundedCache<K, V> {
  private final Map<K, V> map;

  public BoundedCache(int maxEntries) {
    this(maxEntries, value -> {});
  }

  /**
   * As {@link #BoundedCache(int)}, but calls {@code onEvict} for each entry the LRU drops. Needed
   * when a value owns something the garbage collector will not reclaim, such as a catalog client's
   * token-refresh executor and pooled HTTP client. The callback runs while this cache's monitor is
   * held, so it must not block.
   */
  public BoundedCache(int maxEntries, java.util.function.Consumer<V> onEvict) {
    this.map =
        new LinkedHashMap<>(64, 0.75f, true) {
          @Override
          protected boolean removeEldestEntry(Map.Entry<K, V> eldest) {
            if (size() > maxEntries) {
              onEvict.accept(eldest.getValue());
              return true;
            }
            return false;
          }
        };
  }

  public synchronized V get(K key) {
    return map.get(key);
  }

  public synchronized void put(K key, V value) {
    map.put(key, value);
  }

  public synchronized V computeIfAbsent(K key, Function<K, V> fn) {
    return map.computeIfAbsent(key, fn);
  }

  public synchronized void remove(K key) {
    map.remove(key);
  }

  public synchronized int size() {
    return map.size();
  }

  /** A copy of the current values, for gauges that summarize the whole cache. */
  public synchronized java.util.List<V> values() {
    return java.util.List.copyOf(map.values());
  }
}
