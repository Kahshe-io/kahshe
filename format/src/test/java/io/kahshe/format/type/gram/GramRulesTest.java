package io.kahshe.format.type.gram;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.junit.jupiter.api.Assertions.assertSame;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

import java.util.ArrayList;
import java.util.HashSet;
import java.util.List;
import java.util.Set;
import org.junit.jupiter.api.Test;

/**
 * The gram seam: a rule is discovered by the service loader and keyed by the id it owns, so a new
 * cut is added without editing an enum.
 *
 * <p>{@link TestOnlyGramRule} is on the test classpath only, registered through
 * {@code src/test/resources/META-INF/services/io.kahshe.format.type.gram.GramRule}. Verified red with that
 * services file removed: {@code aRuleOnTheClasspathIsDiscovered} then fails with "no rule named
 * test-tumbling ... [utf16-v1, codepoint-v2]" and the id is refused as unknown, which is the
 * discovery this test is here for.
 */
class GramRulesTest {
  @Test
  void theBuiltInsAreDiscoveredFirstAndOnce() {
    List<String> names = new ArrayList<>();
    GramRules.rules().forEach(r -> names.add(r.name()));
    assertEquals(List.of("utf16-v1", "codepoint-v2"), names.subList(0, 2),
        "the built-ins come first, so nothing on the classpath can shadow an id kahshe writes");
    Set<Class<?>> classes = new HashSet<>();
    for (GramRule rule : GramRules.rules()) {
      assertTrue(classes.add(rule.getClass()),
          "a rule listed in a services file AND registered as a built-in is folded, not doubled: "
              + rule.getClass());
    }
    assertSame(GramRules.owner("kahshe-grams-v2-n3"), GramRules.owner("kahshe-grams-v2-n8"),
        "one instance per rule keeps Contract equality stable");
  }

  @Test
  void theBuiltInIdsStillParseExactlyAsBefore() {
    assertEquals(Grams.Contract.v1(), Grams.Contract.of("kahshe-grams-v1"));
    assertEquals(Grams.Contract.v1(), Grams.Contract.of(null), "no property is v1");
    assertEquals(Grams.Contract.v1(), Grams.Contract.of(""));
    assertEquals(Grams.Contract.current(4), Grams.Contract.of("kahshe-grams-v2-n4"));
    for (String id : List.of("kahshe-grams-v1", "kahshe-grams-v2-n3", "kahshe-grams-v2-n8")) {
      assertEquals(id, Grams.Contract.of(id).id(), "the id round-trips through its rule");
    }
  }

  @Test
  void aRuleOnTheClasspathIsDiscovered() {
    GramRule found = null;
    for (GramRule rule : GramRules.rules()) {
      if (rule.name().equals("test-tumbling")) {
        found = rule;
      }
    }
    assertNotNull(found, "no rule named test-tumbling among " + GramRules.names()
        + "; the service loader did not find the one on the test classpath");
  }

  @Test
  void theDiscoveredRuleParsesCutsAndRoundTripsItsId() {
    String id = TestOnlyGramRule.ID_PREFIX + 3;
    Grams.Contract contract = Grams.Contract.of(id);
    assertEquals("test-tumbling", contract.family().name());
    assertEquals(3, contract.size());
    assertEquals(Set.of("abc", "def"), contract.gramsOf("abcdefg"),
        "tumbling windows, not the five the sliding built-in would give");
    assertEquals(5, Grams.Contract.current(3).gramsOf("abcdefg").size(),
        "the built-in rule is untouched by the one beside it");
    assertEquals(Set.of("ab"), contract.gramsOf("AB"), "shorter than a window is one gram whole");
    assertEquals(id, contract.id(), "the id round-trips");
  }

  @Test
  void anIdNoRuleOwnsIsStillRefused() {
    assertNull(GramRules.owner("lucene-ngram-3"), "another engine's gram rule");
    assertThrows(IllegalStateException.class, () -> Grams.Contract.of("lucene-ngram-3"));
    assertThrows(IllegalStateException.class, () -> Grams.Contract.of("kahshe-grams-v9"));
    assertThrows(IllegalStateException.class, () -> Grams.Contract.of("kahshe-grams-v2-n12"),
        "a size outside the rule's range is unreadable, not a different rule");
    assertThrows(IllegalStateException.class, () -> Grams.Contract.of("kahshe-test-tumbling-v1-nwide"));
  }
}
