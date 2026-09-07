package io.kahshe.common;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertTrue;

import java.util.ArrayList;
import java.util.List;
import org.junit.jupiter.api.Test;

/**
 * An LRU that drops a value owning a thread must say so, or the thread leaks.
 *
 * <p>{@code BackendCatalogs} caches caller-scoped {@code RESTCatalog} objects, each holding a
 * pooled HTTP client and a token-refresh executor. Under caller-identity planning the key is a
 * hash of the bearer token, so every rotation mints an entry and evicts one — and an evicted entry
 * that is never closed leaks its thread and sockets for the life of the process. Nothing about
 * "the entry disappeared from a map" makes that visible, which is why it needs a callback rather
 * than a comment.
 */
class BoundedCacheEvictionTest {

  /** Verified by breaking it: dropping the {@code onEvict.accept} call leaves this list empty. */
  @Test
  void everyEvictedValueIsHandedToTheCallback() {
    List<String> evicted = new ArrayList<>();
    BoundedCache<Integer, String> cache = new BoundedCache<>(2, evicted::add);

    cache.put(1, "one");
    cache.put(2, "two");
    assertTrue(evicted.isEmpty(), "nothing should be evicted while the cache is within bounds");

    cache.put(3, "three");
    assertEquals(List.of("one"), evicted, "the least recently used value was dropped unnoticed");
    assertEquals(2, cache.size());

    // access order matters: touching 2 makes 3 the eldest
    cache.get(2);
    cache.put(4, "four");
    assertEquals(List.of("one", "three"), evicted);
  }

  /** The callback-free constructor still works, so existing caches are unaffected. */
  @Test
  void theCallbackIsOptional() {
    BoundedCache<Integer, String> cache = new BoundedCache<>(1);
    cache.put(1, "one");
    cache.put(2, "two");
    assertEquals(1, cache.size());
    assertEquals("two", cache.get(2));
  }
}
