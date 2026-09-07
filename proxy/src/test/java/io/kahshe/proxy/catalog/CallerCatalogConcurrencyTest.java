package io.kahshe.proxy.catalog;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertTrue;

import io.kahshe.common.SingleFlight;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;
import org.junit.jupiter.api.Test;

/**
 * A cold caller-identity catalog must not block every other caller while it initializes.
 *
 * <p>{@code BackendCatalogs.forCaller} loads through a cache keyed by caller. Loading it under
 * {@code BoundedCache.computeIfAbsent} — {@code synchronized} on the whole cache — with a mapping
 * function that performs a network-bound {@code RESTCatalog.initialize()} makes one cold init
 * against a slow backend hold a cache-wide monitor for the length of that round trip, and every
 * other caller-identity request in the process queues behind it doing nothing. With a 5 s backend
 * timeout and 32 workers that is the entire data plane stalled on one unlucky token.
 *
 * <p>These test the locking discipline directly rather than through {@code BackendCatalogs},
 * because reproducing it there needs a real slow REST backend and the property in question belongs
 * to the primitive. What they pin is the two things it has to get right at once: distinct keys
 * proceed in parallel, and one key is still built only once.
 */
class CallerCatalogConcurrencyTest {

  /**
   * Two different tokens must initialize concurrently.
   *
   * <p>Each task blocks inside its loader until the other has started. Under a cache-wide monitor
   * the second task can never enter its loader, so the latch never reaches zero and this times out
   * — which is exactly the stall this forbids.
   */
  @Test
  void twoDifferentKeysInitializeAtTheSameTime() throws Exception {
    SingleFlight<String> flight = new SingleFlight<>();
    CountDownLatch bothInside = new CountDownLatch(2);
    ExecutorService pool = Executors.newFixedThreadPool(2);
    try {
      for (String key : new String[] {"token-a", "token-b"}) {
        pool.submit(
            () ->
                flight.load(
                    key,
                    () -> null,
                    () -> {
                      bothInside.countDown();
                      try {
                        // only returns once the OTHER key's loader has also started
                        assertTrue(bothInside.await(5, TimeUnit.SECONDS));
                      } catch (InterruptedException e) {
                        Thread.currentThread().interrupt();
                      }
                      return key;
                    }));
      }
      assertTrue(
          bothInside.await(5, TimeUnit.SECONDS),
          "two distinct keys could not initialize concurrently; the load is serialized across "
              + "keys, so one slow backend round trip stalls every other caller");
    } finally {
      pool.shutdownNow();
    }
  }

  /**
   * One key must still be built once, however many callers race for it.
   *
   * <p>This is the half a naive "just drop the lock" breaks: two threads bearing the same fresh
   * token would each construct a `RESTCatalog`, one would win the cache, and the loser's HTTP
   * client and token-refresh thread would leak with nothing holding a reference to close them.
   */
  @Test
  void oneKeyIsBuiltOnceEvenWhenCallersRaceForIt() throws Exception {
    SingleFlight<String> flight = new SingleFlight<>();
    AtomicInteger built = new AtomicInteger();
    java.util.Map<String, String> cache = new java.util.concurrent.ConcurrentHashMap<>();
    int callers = 8;
    CountDownLatch ready = new CountDownLatch(callers);
    CountDownLatch go = new CountDownLatch(1);
    ExecutorService pool = Executors.newFixedThreadPool(callers);
    try {
      for (int i = 0; i < callers; i++) {
        pool.submit(
            () -> {
              ready.countDown();
              try {
                go.await(5, TimeUnit.SECONDS);
              } catch (InterruptedException e) {
                Thread.currentThread().interrupt();
              }
              return flight.load(
                  "same-token",
                  () -> cache.get("same-token"),
                  () -> {
                    built.incrementAndGet();
                    String made = "catalog";
                    cache.put("same-token", made);
                    return made;
                  });
            });
      }
      assertTrue(ready.await(5, TimeUnit.SECONDS));
      go.countDown();
      pool.shutdown();
      assertTrue(pool.awaitTermination(10, TimeUnit.SECONDS));
    } finally {
      pool.shutdownNow();
    }
    assertEquals(
        1,
        built.get(),
        "a fresh token was initialized more than once under a race; every loser constructs a "
            + "RESTCatalog whose HTTP client and token-refresh thread then leak uncloseable");
  }
}
