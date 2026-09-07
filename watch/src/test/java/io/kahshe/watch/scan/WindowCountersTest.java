package io.kahshe.watch.scan;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertNull;

import io.kahshe.common.Metrics;
import org.junit.jupiter.api.Test;

/**
 * The structure behind a window rule: the N most recent event times per key, and the claim that
 * this answers "N within T" EXACTLY.
 *
 * <p>The argument the tests stand on: a window of N events inside T ends at some event, and when
 * that event is offered the array holds those N or a tighter set, so the span check decides it.
 * Nothing older than the retained N can help — an older event only widens a span already too
 * wide. {@link #firesOnTheTightestWindowNotTheFirstOne} is that argument as a test.
 *
 * <p>Verified red with the span check relaxed to {@code >= 0}: every key fires on its Nth event
 * whatever the spread, which is the bug that would make every rate rule a match rule.
 */
class WindowCountersTest {

  private Metrics metrics = new Metrics();

  private WindowCounters counters(int count, long timeframeMs) {
    return counters(count, timeframeMs, 1_000);
  }

  private WindowCounters counters(int count, long timeframeMs, int maxKeys) {
    metrics = new Metrics();
    return new WindowCounters(count, timeframeMs, maxKeys, metrics);
  }

  @Test
  void firesOnTheNthEventInsideTheWindow() {
    WindowCounters counters = counters(3, 1_000);
    assertNull(counters.offer("a", 100));
    assertNull(counters.offer("a", 200));
    WindowCounters.Trip trip = counters.offer("a", 300);
    assertNotNull(trip);
    assertEquals(100, trip.startMs());
    assertEquals(300, trip.endMs());
    assertEquals(1, metrics.watchWindowTrips.sum());
  }

  @Test
  void doesNotFireWhenTheNEventsAreSpreadWiderThanTheWindow() {
    WindowCounters counters = counters(3, 1_000);
    assertNull(counters.offer("a", 0));
    assertNull(counters.offer("a", 900));
    assertNull(counters.offer("a", 5_000), "0..5000 is not three within one second");
    assertEquals(0, metrics.watchWindowTrips.sum());
  }

  /**
   * The exactness claim: events arrive too spread out to fire, then tighten. The window that
   * fires is the tight one at the end, not the first three, and it is found without keeping
   * anything older than the last N.
   */
  @Test
  void firesOnTheTightestWindowNotTheFirstOne() {
    WindowCounters counters = counters(3, 1_000);
    assertNull(counters.offer("a", 0));
    assertNull(counters.offer("a", 4_000));
    assertNull(counters.offer("a", 8_000), "spread far wider than a second");
    assertNull(counters.offer("a", 8_400), "8000, 8400 and one more would do it");
    WindowCounters.Trip trip = counters.offer("a", 8_800);
    assertNotNull(trip, "the last three span 800 ms");
    assertEquals(8_000, trip.startMs());
    assertEquals(8_800, trip.endMs());
  }

  @Test
  void countsAreIndependentPerKey() {
    WindowCounters counters = counters(2, 1_000);
    assertNull(counters.offer("a", 100));
    assertNull(counters.offer("b", 150));
    assertNotNull(counters.offer("a", 200), "a's second event");
    assertNull(counters.offer("c", 250));
    assertNotNull(counters.offer("b", 300), "b's second event");
    assertEquals(3, counters.keys());
  }

  @Test
  void aTripClearsTheKeySoItFiresOncePerNEvents() {
    WindowCounters counters = counters(2, 10_000);
    assertNull(counters.offer("a", 100));
    assertNotNull(counters.offer("a", 200));
    assertNull(counters.offer("a", 300), "the third event starts a new count, it does not re-fire");
    assertNotNull(counters.offer("a", 400), "the fourth completes it");
    assertEquals(2, metrics.watchWindowTrips.sum());
  }

  /** Log time is not arrival time: an event inside the retained span takes its place. */
  @Test
  void anOutOfOrderEventInsideTheRetainedSpanStillCounts() {
    WindowCounters counters = counters(3, 1_000);
    assertNull(counters.offer("a", 300));
    assertNull(counters.offer("a", 100), "arrived late, belongs earlier");
    WindowCounters.Trip trip = counters.offer("a", 200);
    assertNotNull(trip, "three events spanning 100..300");
    assertEquals(100, trip.startMs());
    assertEquals(300, trip.endMs());
    assertEquals(0, metrics.watchWindowLateDrops.sum());
  }

  /**
   * A drop happens only when the buffer is FULL and the event is older than everything in it:
   * there is then no room for it that does not evict something newer. While there is room, an
   * old event is placed and counts — 10 ms and 7,000 ms really are two events within ten
   * seconds, and dropping one would be the miss, not the drop's absence.
   */
  @Test
  void anEventOlderThanAFullBufferIsDroppedAndCounted() {
    WindowCounters counters = counters(2, 100);
    assertNull(counters.offer("a", 1_000));
    assertNull(counters.offer("a", 5_000), "full, but 4 s apart: no trip at a 100 ms window");
    assertNull(counters.offer("a", 10), "older than both retained, and no room to place it");
    assertEquals(1, metrics.watchWindowLateDrops.sum(), "a drop is a possible miss, so it counts");
  }

  @Test
  void anOldEventIsPlacedWhileThereIsStillRoomForIt() {
    WindowCounters counters = counters(2, 10_000);
    assertNull(counters.offer("a", 7_000));
    assertNotNull(counters.offer("a", 10), "10 ms and 7 s are two events inside ten seconds");
    assertEquals(0, metrics.watchWindowLateDrops.sum(), "nothing was dropped: there was room");
  }

  /** The memory bound bites, and says so: an evicted key is a partial window thrown away. */
  @Test
  void keysAreCappedAndEvictionsAreCounted() {
    // 16 shards, so a cap of 16 is one key per shard; enough keys to force eviction everywhere
    WindowCounters counters = counters(2, 10_000, 16);
    for (int i = 0; i < 2_000; i++) {
      counters.offer("key-" + i, 1_000 + i);
    }
    assertEquals(0, metrics.watchWindowTrips.sum(), "every key saw exactly one event");
    org.junit.jupiter.api.Assertions.assertTrue(counters.keys() <= 16,
        "the cap holds: " + counters.keys());
    org.junit.jupiter.api.Assertions.assertTrue(metrics.watchWindowKeyEvictions.sum() > 1_900,
        "and every key that fell out was counted: " + metrics.watchWindowKeyEvictions.sum());
  }
}
