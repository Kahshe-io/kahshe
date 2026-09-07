package io.kahshe.common;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.junit.jupiter.api.Assertions.assertTrue;

import java.util.concurrent.CountDownLatch;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.LongAdder;
import org.junit.jupiter.api.Test;

class WeighedCacheTest {

  private static final long KIB = 1024;

  private record Weighted(String name, long weight) {}

  @Test
  void evictsByWeightNotCount() {
    LongAdder evictions = new LongAdder();
    WeighedCache<String, Weighted> cache =
        new WeighedCache<>(150 * KIB, Weighted::weight, evictions);
    cache.put("a", new Weighted("a", 100 * KIB));
    cache.put("b", new Weighted("b", 100 * KIB));
    int present = 0;
    present += cache.get("a") != null ? 1 : 0;
    present += cache.get("b") != null ? 1 : 0;
    assertEquals(1, present, "150 KiB budget holds only one 100 KiB entry");
    assertEquals(1, evictions.sum());
    assertTrue(cache.estimatedWeightBytes() <= 150 * KIB);
  }

  @Test
  void clampsZeroAndNegativeWeightsToOneKib() {
    LongAdder evictions = new LongAdder();
    WeighedCache<String, Weighted> cache =
        new WeighedCache<>(1000 * KIB, Weighted::weight, evictions);
    cache.put("zero", new Weighted("zero", 0));
    cache.put("negative", new Weighted("negative", -5));
    assertEquals("zero", cache.get("zero").name());
    assertEquals("negative", cache.get("negative").name());
    assertTrue(cache.estimatedWeightBytes() >= 2 * KIB);
    assertEquals(0, evictions.sum());
  }

  @Test
  void budgetsAboveIntMaxBytesStillWeighEntries() {
    LongAdder evictions = new LongAdder();
    // > 2^31 bytes: with byte-granular int weighing this budget would be unrepresentable
    WeighedCache<String, Weighted> cache =
        new WeighedCache<>(8L * 1024 * 1024 * 1024, Weighted::weight, evictions);
    cache.put("big", new Weighted("big", 3L * 1024 * 1024 * 1024));
    assertEquals("big", cache.get("big").name());
    assertEquals(0, evictions.sum());
    assertEquals(3L * 1024 * 1024 * 1024, cache.estimatedWeightBytes());
  }

  @Test
  void oversizedEntryIsRetained() {
    LongAdder evictions = new LongAdder();
    WeighedCache<String, Weighted> cache =
        new WeighedCache<>(100 * KIB, Weighted::weight, evictions);
    cache.put("huge", new Weighted("huge", 10_000 * KIB));
    long evictionsAfterPut = evictions.sum();
    for (int i = 0; i < 10; i++) {
      assertNotNull(cache.get("huge"), "an over-budget entry must stay resident, not thrash");
    }
    assertEquals(evictionsAfterPut, evictions.sum(), "gets must not trigger evictions");
    assertEquals("huge", cache.get("huge").name());
  }

  @Test
  void concurrentPutsRespectBudget() throws Exception {
    int threads = 8;
    int keysPerThread = 300;
    long entryWeight = 4 * KIB;
    long budget = threads * keysPerThread * entryWeight / 4;
    LongAdder evictions = new LongAdder();
    WeighedCache<String, Weighted> cache = new WeighedCache<>(budget, Weighted::weight, evictions);
    CountDownLatch start = new CountDownLatch(1);
    CountDownLatch done = new CountDownLatch(threads);
    ExecutorService pool = Executors.newFixedThreadPool(threads);
    try {
      for (int t = 0; t < threads; t++) {
        int offset = t;
        pool.submit(
            () -> {
              try {
                start.await();
                for (int i = 0; i < keysPerThread; i++) {
                  String key = "k-" + offset + "-" + i;
                  cache.put(key, new Weighted(key, entryWeight));
                }
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
    cache.cleanUp();
    assertTrue(
        cache.estimatedWeightBytes() <= budget + entryWeight,
        "weight " + cache.estimatedWeightBytes() + " must not exceed budget " + budget);
    assertTrue(evictions.sum() > 0);
  }

  @Test
  void removeIsNotAnEviction() {
    LongAdder evictions = new LongAdder();
    WeighedCache<String, Weighted> cache =
        new WeighedCache<>(1000 * KIB, Weighted::weight, evictions);
    cache.put("a", new Weighted("a", 10 * KIB));
    cache.remove("a");
    assertNull(cache.get("a"));
    assertEquals(0, evictions.sum());
  }
}
