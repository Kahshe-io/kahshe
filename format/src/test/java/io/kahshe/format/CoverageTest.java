package io.kahshe.format;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

import java.util.List;
import org.junit.jupiter.api.Test;

/**
 * A path is covered once: two live ordinals for one path is the corrupt coverage
 * {@code Coverage.ordinalOfLive} refuses, and the refusal must name the path and say what to do.
 */
class CoverageTest {

  @Test
  void aPathIsCoveredOnce() {
    List<Coverage.Entry> corrupt = List.of(
        new Coverage.Entry("f1.parquet", 0, true),
        new Coverage.Entry("f1.parquet", 1, true),
        new Coverage.Entry("f2.parquet", 2, true));
    IllegalStateException e =
        assertThrows(IllegalStateException.class, () -> Coverage.ordinalOfLive(corrupt));
    assertTrue(e.getMessage().contains("f1.parquet"), e.getMessage());
    assertTrue(e.getMessage().contains("rebuild"), "the message says what to do: " + e.getMessage());
  }

  @Test
  void aTombstoneBesideItsLiveSuccessorIsNotADuplicate() {
    // A path that departed and came back holds a dead entry and a live one; only live ones count.
    List<Coverage.Entry> fine = List.of(
        new Coverage.Entry("f1.parquet", 0, false),
        new Coverage.Entry("f1.parquet", 1, true));
    assertEquals(1, Coverage.ordinalOfLive(fine).get("f1.parquet"));
  }
}
