package io.kahshe.common;

import java.util.concurrent.ConcurrentHashMap;
import java.util.function.Supplier;

/**
 * Collapses concurrent cold loads of the same key into one: callers who raced a load re-check
 * under the per-key lock and reuse what the winner produced instead of loading again. Distinct
 * keys lock independently and load concurrently.
 *
 * <p>A key's flight is refcounted and pinned while any thread is inside {@link #load}, so a lock
 * in use is never discarded — that would let two loads of the same key run at once — and the
 * map holds only keys with a load actually in flight.
 */
public final class SingleFlight<K> {
  private static final class Flight {
    int refs;
  }

  private final ConcurrentHashMap<K, Flight> flights = new ConcurrentHashMap<>();

  /**
   * Runs {@code check} then, only if it returned null, {@code load} — both under this key's lock.
   * Callers do their own lock-free fast-path check first; {@code check} must return null exactly
   * when a load is still needed. A load that throws propagates and is not remembered: the lock is
   * released and the next caller runs the loader again.
   */
  public <V> V load(K key, Supplier<V> check, Supplier<V> load) {
    Flight flight =
        flights.compute(
            key,
            (k, f) -> {
              Flight acquired = f == null ? new Flight() : f;
              acquired.refs++;
              return acquired;
            });
    try {
      synchronized (flight) {
        V fresh = check.get();
        return fresh != null ? fresh : load.get();
      }
    } finally {
      flights.compute(key, (k, f) -> f == null || --f.refs == 0 ? null : f);
    }
  }
}
