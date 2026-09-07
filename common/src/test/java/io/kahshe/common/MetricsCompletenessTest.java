package io.kahshe.common;

import static org.junit.jupiter.api.Assertions.assertTrue;

import java.lang.reflect.Field;
import java.lang.reflect.Modifier;
import java.util.concurrent.atomic.LongAdder;
import org.junit.jupiter.api.Test;

/**
 * Every counter a component can move reaches the scrape.
 *
 * <p>Adding a metric is two edits — the field, and its line in {@code scrape()} — and nothing
 * enforced the second. A counter that is incremented and never emitted is a signal an operator
 * was promised and never gets, and the mirror case had already happened once: a series emitted
 * for a producer that was deleted, reading 0 forever. This pins the first half without knowing
 * any metric's name: give every adder a distinct value, and demand each value in the output.
 */
class MetricsCompletenessTest {

  @Test
  void everyPublicCounterFieldIsEmittedByScrape() throws Exception {
    Metrics metrics = new Metrics();
    long stamp = 1_000_003L; // distinct, prime-spaced, and larger than anything a fresh scrape holds
    int counters = 0;
    for (Field field : Metrics.class.getFields()) {
      if (field.getType() == LongAdder.class && !Modifier.isStatic(field.getModifiers())) {
        ((LongAdder) field.get(metrics)).add(stamp);
        counters++;
        stamp += 7;
      }
    }
    assertTrue(counters > 20, "found only " + counters + " adders: the reflection is looking in the wrong place");

    String scrape = metrics.scrape();
    long expected = 1_000_003L;
    for (Field field : Metrics.class.getFields()) {
      if (field.getType() == LongAdder.class && !Modifier.isStatic(field.getModifiers())) {
        assertTrue(scrape.contains(" " + expected + "\n"),
            field.getName() + " is a public counter that scrape() never emits (looked for the value "
                + expected + ")");
        expected += 7;
      }
    }
  }
}
