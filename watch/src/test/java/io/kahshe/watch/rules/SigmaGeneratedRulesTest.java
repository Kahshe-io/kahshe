package io.kahshe.watch.rules;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertTrue;

import io.kahshe.common.Metrics;
import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.ArrayList;
import java.util.List;
import java.util.stream.Stream;

import org.junit.jupiter.api.Test;

/**
 * The round trip: every rules file the pySigma backend generates must LOAD, here, with no rule
 * skipped.
 *
 * <p>This is the gate that makes the backend trustworthy, and it is deliberately on this side of
 * the language boundary. A Python test suite can only check that the converter agrees with the
 * converter's own author; whether the YAML it emits is a rule kahshe accepts is a question only
 * kahshe's loader answers. Without this, the failure mode is a converted library that looks
 * right, ships, and is skipped at load with a WARN nobody reads.
 *
 * <p>The failure it catches is concrete: a numeric comparison emitted as {@code gte: ["4000"]} —
 * a quoted string, because the value arrived wrapped in a {@code SigmaNumber} — is refused by the
 * loader, which takes no comparison whose value is not a number, while the Python assertions on
 * the other side of the boundary stay green.
 *
 * <p>The fixtures under sigma/tests/generated/ are written by the backend's own test suite
 * ({@code pytest sigma/tests}) and committed. Regenerating them is how a change to the backend
 * reaches this gate; if they are absent the test fails rather than passing vacuously, because a
 * round trip that silently checks nothing is worse than none.
 */
class SigmaGeneratedRulesTest {

  private static final Path GENERATED = Path.of("..", "sigma", "tests", "generated");

  @Test
  void everyRuleThePySigmaBackendGeneratesLoadsWithNothingSkipped() throws IOException {
    List<Path> files = generated();
    assertTrue(files.size() >= 8,
        "only " + files.size() + " generated rule file(s) under " + GENERATED.toAbsolutePath()
            + ": run `pytest sigma/tests` to write them. A round trip with nothing in it passes "
            + "for the wrong reason");

    List<String> failures = new ArrayList<>();
    int loaded = 0;
    for (Path file : files) {
      Metrics metrics = new Metrics();
      List<WatchRule> rules = new WatchRules(file.toString(), metrics).current();
      long skipped = metrics.watchRulesSkipped.sum();
      if (skipped > 0 || rules.isEmpty()) {
        failures.add(file.getFileName() + ": loaded " + rules.size() + ", skipped " + skipped);
      }
      loaded += rules.size();
    }
    assertEquals(List.of(), failures,
        "the backend emitted YAML this loader will not take: " + failures);
    assertTrue(loaded >= files.size(), "every file must carry at least one rule");
  }

  /**
   * The converted rules must also mean what the conversion claimed. Loading is necessary and not
   * sufficient: a rule whose condition parsed to the wrong tree loads perfectly.
   */
  @Test
  void theConvertedConditionsAreTheTreesTheSigmaRulesDescribed() throws IOException {
    WatchRule nested = only("a_nested_condition_keeps_its_shape.yaml");
    // (errors and not images) or server — an Or whose first term carries the negation
    assertTrue(nested.expr() instanceof WatchRule.Expr.Or, String.valueOf(nested.expr()));
    assertTrue(String.valueOf(nested.expr()).contains("Not"),
        "the filter's negation must survive conversion: " + nested.expr());
    assertFalse(nested.ridesIndex(),
        "a condition with a negation is answered by the scan alone, never half by the index");

    WatchRule window = only("an_event_count_correlation_becomes_a_window_rule.yaml");
    assertTrue(window.window() != null, "an event_count correlation must carry a window");
    assertEquals("ts", window.window().tsColumn());
    assertEquals(3, window.window().count());
    assertEquals(List.of("clientip"), window.window().groupBy(),
        "the correlation's group-by goes through the pipeline's field mapping");

    WatchRule quoted = only("a_value_that_looks_like_a_yaml_boolean_is_quoted.yaml");
    assertEquals("no", quoted.where().get(0).values().get(0),
        "an unquoted `no` would have parsed to the boolean false and matched the text \"false\"");
  }

  private static WatchRule only(String name) {
    Metrics metrics = new Metrics();
    List<WatchRule> rules =
        new WatchRules(GENERATED.resolve(name).toString(), metrics).current();
    assertEquals(0, metrics.watchRulesSkipped.sum(), name + " was skipped");
    assertEquals(1, rules.size(), name + " should hold one rule");
    return rules.get(0);
  }

  private static List<Path> generated() throws IOException {
    if (!Files.isDirectory(GENERATED)) {
      return List.of();
    }
    try (Stream<Path> files = Files.list(GENERATED)) {
      return files.filter(f -> f.getFileName().toString().endsWith(".yaml")).sorted().toList();
    }
  }
}
