package io.kahshe.watch.rules;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.junit.jupiter.api.Assertions.assertTrue;

import java.util.List;
import java.util.Map;
import org.junit.jupiter.api.Test;

/**
 * {@link WatchRule#whyNotRidesIndex} is the one statement of the index-riding shape, and
 * {@link WatchRule#ridesIndex} is its null test. A reader that must SAY why it refused — the hunt
 * — reads the same clauses the boolean does, so the two cannot drift. This pins the half the
 * boolean's own tests do not: that every shape the boolean rejects has a reason, and the reason
 * names the shape. Red by returning null from any one clause: the boolean flips true (the
 * existing truth-table tests catch that) and the reason for that shape is gone (this one does).
 */
class RidesIndexReasonTest {

  private static WatchRule.Field field(String column, WatchRule.Op op, String value) {
    return new WatchRule.Field(column, op, List.of(value));
  }

  private static WatchRule flat(List<WatchRule.Field> where, WatchRule.Condition condition) {
    return new WatchRule("r", "r", WatchRule.Severity.HIGH, "p", "logs.t", where, condition, 1);
  }

  @Test
  void everyShapeThatDoesNotRideHasAReasonNamingIt() {
    WatchRule.Field alpha = field("msg", WatchRule.Op.MATCH, "alpha");
    WatchRule.Field bravo = field("msg", WatchRule.Op.MATCH, "bravo");
    Map<String, WatchRule> refused = Map.of(
        "window", new WatchRule("r", "r", WatchRule.Severity.HIGH, "p", "logs.t", List.of(alpha),
            WatchRule.Condition.ANY_OF, 1, WatchRule.Expr.flat(WatchRule.Condition.ANY_OF, 1),
            new WatchRule.Window("ts", 60_000, List.of(), 5)),
        "spans columns", flat(List.of(alpha, field("other", WatchRule.Op.MATCH, "bravo")),
            WatchRule.Condition.ANY_OF),
        "operator equals", flat(List.of(field("msg", WatchRule.Op.EQUALS, "alpha")),
            WatchRule.Condition.ANY_OF),
        "operator lte", flat(List.of(field("msg", WatchRule.Op.LTE, "5")),
            WatchRule.Condition.ANY_OF),
        "not a flat any-of or all-of", new WatchRule("r", "r", WatchRule.Severity.HIGH, "p",
            "logs.t", List.of(alpha), WatchRule.Condition.ANY_OF, 1,
            new WatchRule.Expr.Not(new WatchRule.Expr.FieldRef(0))),
        "all-of over 2 fields", flat(List.of(alpha, bravo), WatchRule.Condition.ALL_OF));

    for (Map.Entry<String, WatchRule> shape : refused.entrySet()) {
      String why = shape.getValue().whyNotRidesIndex();
      assertFalse(shape.getValue().ridesIndex(), shape.getKey() + " must not ride");
      assertNotNull(why, shape.getKey() + " is refused without a reason");
      assertTrue(why.contains(shape.getKey()),
          "the reason for '" + shape.getKey() + "' must name it: " + why);
    }
  }

  @Test
  void everyShapeThatRidesHasNoReason() {
    WatchRule.Field alpha = field("msg", WatchRule.Op.MATCH, "alpha");
    WatchRule.Field bravo = field("msg", WatchRule.Op.MATCH, "bravo");
    List<WatchRule> rides = List.of(
        flat(List.of(alpha), WatchRule.Condition.ANY_OF),
        flat(List.of(alpha), WatchRule.Condition.ALL_OF),
        flat(List.of(alpha, bravo), WatchRule.Condition.ANY_OF),
        flat(List.of(field("msg", WatchRule.Op.CONTAINS, "alph")), WatchRule.Condition.ANY_OF));
    for (WatchRule rule : rides) {
      assertNull(rule.whyNotRidesIndex(), "no reason for a shape that rides: " + rule);
      assertTrue(rule.ridesIndex());
    }
    assertEquals(4, rides.size());
  }
}
