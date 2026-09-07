package io.kahshe.common;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicReference;
import org.junit.jupiter.api.Test;

class SingleFlightTest {

  @Test
  void concurrentColdLoadsShareOneFlight() throws Exception {
    int threads = 16;
    SingleFlight<String> flight = new SingleFlight<>();
    ConcurrentHashMap<String, String> cache = new ConcurrentHashMap<>();
    AtomicInteger loads = new AtomicInteger();
    CountDownLatch start = new CountDownLatch(1);
    CountDownLatch done = new CountDownLatch(threads);
    ExecutorService pool = Executors.newFixedThreadPool(threads);
    AtomicReference<String> firstResult = new AtomicReference<>();
    try {
      for (int i = 0; i < threads; i++) {
        pool.submit(
            () -> {
              try {
                start.await();
                String value =
                    flight.load(
                        "key",
                        () -> cache.get("key"),
                        () -> {
                          loads.incrementAndGet();
                          try {
                            Thread.sleep(50); // widen the race window
                          } catch (InterruptedException ignored) {
                            Thread.currentThread().interrupt();
                          }
                          String loaded = "loaded";
                          cache.put("key", loaded);
                          return loaded;
                        });
                firstResult.compareAndSet(null, value);
                assertEquals("loaded", value);
              } catch (InterruptedException ignored) {
                Thread.currentThread().interrupt();
              } finally {
                done.countDown();
              }
            });
      }
      start.countDown();
      assertTrue(done.await(10, TimeUnit.SECONDS));
    } finally {
      pool.shutdownNow();
    }
    assertEquals(1, loads.get(), "all racing callers must share one load");
    assertEquals("loaded", firstResult.get());
  }

  @Test
  void distinctKeysLoadIndependently() {
    SingleFlight<String> flight = new SingleFlight<>();
    AtomicInteger loads = new AtomicInteger();
    String a = flight.load("a", () -> null, () -> "va" + loads.incrementAndGet());
    String b = flight.load("b", () -> null, () -> "vb" + loads.incrementAndGet());
    assertEquals("va1", a);
    assertEquals("vb2", b);
  }

  @Test
  void freshCheckUnderLockSkipsLoad() {
    SingleFlight<String> flight = new SingleFlight<>();
    String value = flight.load("k", () -> "cached", () -> "fresh-load");
    assertEquals("cached", value);
  }

  @Test
  void throwingLoaderPropagatesAndIsNotMemoized() {
    SingleFlight<String> flight = new SingleFlight<>();
    AtomicInteger loads = new AtomicInteger();
    assertThrows(
        IllegalStateException.class,
        () ->
            flight.load(
                "k",
                () -> null,
                () -> {
                  loads.incrementAndGet();
                  throw new IllegalStateException("boom");
                }));
    // failure not cached, lock released: the next load runs the loader again
    String value = flight.load("k", () -> null, () -> "ok" + loads.incrementAndGet());
    assertEquals("ok2", value);
  }

  @Test
  void manyKeysDoNotBreakMutualExclusion() throws Exception {
    SingleFlight<String> flight = new SingleFlight<>();
    ConcurrentHashMap<String, String> cache = new ConcurrentHashMap<>();
    AtomicInteger hotLoads = new AtomicInteger();
    CountDownLatch start = new CountDownLatch(1);
    ExecutorService pool = Executors.newFixedThreadPool(6);
    List<Future<?>> futures = new ArrayList<>();
    try {
      for (int t = 0; t < 4; t++) {
        int offset = t;
        futures.add(
            pool.submit(
                () -> {
                  start.await();
                  for (int i = 0; i < 250; i++) {
                    flight.load("key-" + offset + "-" + i, () -> null, () -> "v");
                  }
                  return null;
                }));
      }
      for (int t = 0; t < 2; t++) {
        futures.add(
            pool.submit(
                () -> {
                  start.await();
                  return flight.load(
                      "hot",
                      () -> cache.get("hot"),
                      () -> {
                        hotLoads.incrementAndGet();
                        try {
                          Thread.sleep(100); // let the distinct-key churn run alongside
                        } catch (InterruptedException ignored) {
                          Thread.currentThread().interrupt();
                        }
                        cache.put("hot", "loaded");
                        return "loaded";
                      });
                }));
      }
      start.countDown();
      for (Future<?> future : futures) {
        future.get(10, TimeUnit.SECONDS);
      }
    } finally {
      pool.shutdownNow();
    }
    assertEquals(1, hotLoads.get(), "distinct-key churn must not unpin an in-flight lock");
  }
}
