package io.kahshe.indexer.maintain;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNotEquals;
import static org.junit.jupiter.api.Assertions.assertSame;
import static org.junit.jupiter.api.Assertions.assertTrue;

import java.util.HashSet;
import java.util.List;
import java.util.Set;
import org.junit.jupiter.api.Test;

/**
 * The claim order of one member of an indexer fleet.
 *
 * <p>Verified red with {@code rotate} returning its input: every ordinal started at the same
 * column, which is the whole failure the rotation exists to prevent.
 */
class FleetTest {

  @Test
  void aStatefulSetNameGivesItsOrdinalDirectly() {
    assertEquals(0, Fleet.ordinal(-1, "kahshe-indexer-0"));
    assertEquals(3, Fleet.ordinal(-1, "kahshe-indexer-3"));
    assertEquals(11, Fleet.ordinal(-1, "kahshe-indexer-11"));
  }

  @Test
  void aDeploymentNameGivesAStableSpreadInstead() {
    int first = Fleet.ordinal(-1, "kahshe-indexer-6d4f9c7b8-x2ktq");
    assertEquals(first, Fleet.ordinal(-1, "kahshe-indexer-6d4f9c7b8-x2ktq"), "stable per pod");
    assertNotEquals(first, Fleet.ordinal(-1, "kahshe-indexer-6d4f9c7b8-p9wla"),
        "two pods of one Deployment do not share a claim order");
    assertTrue(first >= 0, "an ordinal is never negative: it indexes a rotation");
  }

  @Test
  void aConfiguredOrdinalWinsAndAnAbsentHostnameIsZero() {
    assertEquals(7, Fleet.ordinal(7, "kahshe-indexer-2"), "configuration is the operator's word");
    assertEquals(0, Fleet.ordinal(-1, null));
    assertEquals(0, Fleet.ordinal(-1, "  "));
  }

  @Test
  void rotationStartsEachMemberElsewhereAndStillVisitsEverything() {
    List<String> columns = List.of("a", "b", "c", "d");
    assertEquals(List.of("b", "c", "d", "a"), Fleet.rotate(columns, 1));
    assertEquals(List.of("d", "a", "b", "c"), Fleet.rotate(columns, 3));
    assertEquals(List.of("b", "c", "d", "a"), Fleet.rotate(columns, 5), "ordinal wraps");

    Set<String> firsts = new HashSet<>();
    for (int ordinal = 0; ordinal < 4; ordinal++) {
      List<String> rotated = Fleet.rotate(columns, ordinal);
      firsts.add(rotated.get(0));
      assertEquals(new HashSet<>(columns), new HashSet<>(rotated), "every column is still visited");
    }
    assertEquals(4, firsts.size(), "four members start on four different columns");
  }

  @Test
  void aLoneBuilderGetsItsListBackUntouched() {
    List<String> columns = List.of("a", "b", "c");
    assertSame(columns, Fleet.rotate(columns, 0), "ordinal 0 is not a fleet: no copy, no rotation");
    assertSame(columns, Fleet.rotate(columns, 3), "a full turn is the same order");
    List<String> one = List.of("only");
    assertSame(one, Fleet.rotate(one, 2));
  }
}
