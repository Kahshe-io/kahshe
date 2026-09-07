package io.kahshe.watch.rules;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.junit.jupiter.api.Assertions.assertTrue;

import io.kahshe.common.Metrics;
import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.List;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

class WatchRulesTest {
  @TempDir Path dir;

  private static final String VALID =
      """
      rules:
        - id: error-burst
          title: Error burst
          severity: high
          prefix: lakehouse
          table: logs.events
          column: msg
          match: [error, timeout]
          condition: all-of
          min_count: 10
        - id: substring
          severity: low
          prefix: lakehouse
          table: logs.events
          column: msg
          contains: ["private key"]
      """;

  private Path write(String content) throws IOException {
    Path file = dir.resolve("rules.yaml");
    Files.writeString(file, content);
    return file;
  }

  /**
   * The multi-field form parses to the field list the row scan evaluates, one field per operator
   * named in a {@code where} entry, in the order written.
   */
  @Test
  void multiFieldFormParses() throws IOException {
    Path file = write("""
        rules:
          - id: moriya
            severity: critical
            prefix: lakehouse
            table: logs.events
            where:
              - { column: provider_name, equals: ["Service Control Manager"] }
              - { column: event_id, equals: [7045] }
              - { column: service_name, contains: [ZzNetSvc], starts_with: [zz] }
            condition: all-of
        """);
    List<WatchRule> loaded = new WatchRules(file.toString(), new Metrics()).current();

    assertEquals(1, loaded.size());
    WatchRule rule = loaded.get(0);
    assertEquals(4, rule.where().size(), "an entry naming two operators is two fields");
    assertEquals(List.of("provider_name", "event_id", "service_name"), rule.columns());
    assertEquals(WatchRule.Op.EQUALS, rule.where().get(1).op());
    assertEquals(List.of("7045"), rule.where().get(1).values(), "a number keeps its text");
    assertEquals(WatchRule.Op.STARTS_WITH, rule.where().get(3).op());
    assertNull(rule.column(), "a rule spanning columns has no single column");
    assertFalse(rule.ridesIndex(), "equals has no index evidence; this rule is the scan's");
  }

  /** The old form's field list: one per value, so all-of still means every token. */
  @Test
  void singleColumnFormIsTheOneColumnCase() throws IOException {
    WatchRule rule = new WatchRules(write(VALID).toString(), new Metrics()).current().get(0);
    assertEquals("msg", rule.column());
    // all-of over two tokens asks for one ROW with both. A file with each somewhere is not that,
    // and the index can only answer "somewhere", so this shape goes to the row scan.
    assertFalse(rule.ridesIndex(), "all-of over more than one field is the scan's");

    // One field, or any-of over several, is the index's: OR commutes with "some row has".
    WatchRule single = new WatchRule(rule.id(), rule.title(), rule.severity(), rule.prefix(),
        rule.table(), List.of(rule.where().get(0)), WatchRule.Condition.ANY_OF, rule.minCount(),
        WatchRule.Expr.flat(WatchRule.Condition.ANY_OF, 1), null);
    assertTrue(single.ridesIndex(), "match on one column is what the index-riding path answers");

    // The same shape with a window must NOT ride the index: the index answers "some file holds
    // this token", and a rate rule answered that way fires on the FIRST match -- "N within T"
    // demoted to "ever".
    WatchRule windowed = new WatchRule(single.id(), single.title(), single.severity(),
        single.prefix(), single.table(), single.where(), single.condition(), single.minCount(),
        single.expr(), new WatchRule.Window("ts", 60_000L, List.of(), 5));
    assertFalse(windowed.ridesIndex(),
        "a window rule evaluated per file on first match is a rate rule turned into a match rule");
    assertEquals(2, rule.where().size());
    assertEquals(List.of("error"), rule.where().get(0).values());
  }

  /** Two forms in one rule have two answers to "which columns"; the quiet one would win. */
  @Test
  void aRuleMixingColumnAndWhereIsRefused() throws IOException {
    Path file = write("""
        rules:
          - id: mixed
            severity: low
            prefix: lakehouse
            table: logs.events
            column: msg
            where:
              - { column: other, contains: [abc] }
        """);
    Metrics metrics = new Metrics();
    assertEquals(0, new WatchRules(file.toString(), metrics).current().size());
    assertEquals(1, metrics.watchRulesSkipped.sum());
  }

  @Test
  void aWhereEntryWithNoOperatorIsRefused() throws IOException {
    Path file = write("""
        rules:
          - id: empty
            severity: low
            prefix: lakehouse
            table: logs.events
            where:
              - { column: msg }
        """);
    Metrics metrics = new Metrics();
    assertEquals(0, new WatchRules(file.toString(), metrics).current().size());
    assertEquals(1, metrics.watchRulesSkipped.sum());
  }

  @Test
  void validFileParses() throws IOException {
    Path file = write(VALID);
    WatchRules rules = new WatchRules(file.toString(), new Metrics());
    List<WatchRule> loaded = rules.current();
    assertEquals(2, loaded.size());
    WatchRule first = loaded.get(0);
    assertEquals(WatchRule.Severity.HIGH, first.severity());
    assertEquals(WatchRule.Condition.ALL_OF, first.condition());
    assertEquals(10, first.minCount());
    assertEquals(List.of("error", "timeout"), first.match());
    // defaults: title falls back to id, condition to any-of, min_count to 1
    WatchRule second = loaded.get(1);
    assertEquals("substring", second.title());
    assertEquals(WatchRule.Condition.ANY_OF, second.condition());
    assertEquals(1, second.minCount());
  }

  @Test
  void invalidRuleSkippedNotFatal() throws IOException {
    Path file = write(
        """
        rules:
          - id: bad-severity
            severity: catastrophic
            prefix: p
            table: ns.t
            column: msg
            match: [error]
          - id: unknown-field
            severity: low
            prefix: p
            table: ns.t
            column: msg
            match: [error]
            regexp: "never"
          - id: good
            severity: low
            prefix: p
            table: ns.t
            column: msg
            match: [error]
        """);
    Metrics metrics = new Metrics();
    WatchRules rules = new WatchRules(file.toString(), metrics);
    assertEquals(1, rules.current().size());
    assertEquals("good", rules.current().get(0).id());
    assertEquals(2, metrics.watchRulesSkipped.sum());
  }

  @Test
  void duplicateIdSkippedFirstOccurrenceWins() throws IOException {
    Path file = write(
        """
        rules:
          - id: dup
            severity: low
            prefix: p
            table: ns.t
            column: msg
            match: [error]
          - id: dup
            severity: high
            prefix: p
            table: ns.t
            column: msg
            match: [timeout]
        """);
    Metrics metrics = new Metrics();
    WatchRules rules = new WatchRules(file.toString(), metrics);
    assertEquals(1, rules.current().size());
    assertEquals(WatchRule.Severity.LOW, rules.current().get(0).severity());
    assertEquals(1, metrics.watchRulesSkipped.sum());
  }

  @Test
  void multiTokenMatchEntryRejected() throws IOException {
    Path file = write(
        """
        rules:
          - id: multi
            severity: low
            prefix: p
            table: ns.t
            column: msg
            match: ["connection refused"]
        """);
    WatchRules rules = new WatchRules(file.toString(), new Metrics());
    assertTrue(rules.current().isEmpty());
  }

  /**
   * A compound identifier is one token under analyzer v3 — an IP, a UUID, a dashed hex id is the
   * first thing a security rule names — so validating it against the plain alphabet would skip the
   * rule while the index held exactly that token. A dashed
   * WORD (quorum-epoch-777) is not a v3 compound: the index holds its pieces, and a rule on it is a
   * contains rule, which the validator still says.
   */
  @Test
  void aCompoundIdentifierIsOneMatchToken() throws IOException {
    Path file = write(
        """
        rules:
          - id: ip
            severity: high
            prefix: p
            table: ns.t
            column: msg
            match: [10.0.0.1, a1b2c3d4-e5f6]
        """);
    WatchRules rules = new WatchRules(file.toString(), new Metrics());
    assertEquals(1, rules.current().size(), "a compound identifier is a single indexable token");
    assertEquals(List.of("10.0.0.1", "a1b2c3d4-e5f6"), rules.current().get(0).match());
  }

  // ---------------------------------------------------------------- the detection form

  /** Sigma's shape: named selections, and a condition expression over their names. */
  private static String detection(String condition, String... extraSelections) {
    return """
        rules:
          - id: sigma
            severity: high
            prefix: lakehouse
            table: logs.events
            detection:
              selection:
                - { column: msg, equals: ["Service Control Manager"] }
                - { column: num, equals: [7045] }
              filter:
                - { column: other, contains: [benign] }
        %s      condition: %s
        """.formatted(String.join("", extraSelections), condition);
  }

  @Test
  void detectionFormParsesSelectionsAndTheCondition() throws IOException {
    List<WatchRule> loaded =
        new WatchRules(write(detection("selection and not filter")).toString(), new Metrics())
            .current();

    assertEquals(1, loaded.size());
    WatchRule rule = loaded.get(0);
    assertEquals(3, rule.where().size(), "two selection fields then the filter's one, in order");
    assertEquals(List.of("msg", "num", "other"), rule.columns());
    assertEquals(
        new WatchRule.Expr.And(List.of(
            new WatchRule.Expr.And(
                List.of(new WatchRule.Expr.FieldRef(0), new WatchRule.Expr.FieldRef(1))),
            new WatchRule.Expr.Not(new WatchRule.Expr.FieldRef(2)))),
        rule.expr());
    assertFalse(rule.ridesIndex(), "a not is not a shape the per-file evaluator can answer");
  }

  @Test
  void oneOfThemIsADisjunctionAndAllOfAWildcardAConjunction() throws IOException {
    WatchRule oneOf =
        new WatchRules(write(detection("1 of them")).toString(), new Metrics()).current().get(0);
    assertEquals(
        new WatchRule.Expr.Or(List.of(
            new WatchRule.Expr.And(
                List.of(new WatchRule.Expr.FieldRef(0), new WatchRule.Expr.FieldRef(1))),
            new WatchRule.Expr.FieldRef(2))),
        oneOf.expr(),
        "'1 of them' is an OR over the selections, each still an AND of its own fields");

    // '*' matches both selection names; a pattern matching only some would leave the other
    // unreferenced, which is refused (see aSelectionTheConditionNeverUsesIsSkipped)
    WatchRule allOf =
        new WatchRules(write(detection("all of *")).toString(), new Metrics()).current().get(0);
    assertEquals(WatchRule.Expr.And.class, allOf.expr().getClass());
    assertEquals(2, ((WatchRule.Expr.And) allOf.expr()).terms().size());
  }

  @Test
  void aQuantifierMatchingNoSelectionIsSkipped() throws IOException {
    Metrics metrics = new Metrics();
    assertTrue(
        new WatchRules(write(detection("1 of zzz*")).toString(), metrics).current().isEmpty(),
        "a pattern that selects nothing would make the condition trivially false");
    assertEquals(1, metrics.watchRulesSkipped.sum());
  }

  @Test
  void aConditionNamingAnUnknownSelectionIsSkipped() throws IOException {
    Metrics metrics = new Metrics();
    assertTrue(
        new WatchRules(write(detection("selection and typo")).toString(), metrics).current().isEmpty(),
        "a condition naming a selection that does not exist is a typo, not a rule");
    assertEquals(1, metrics.watchRulesSkipped.sum());
  }

  @Test
  void aSelectionTheConditionNeverUsesIsSkipped() throws IOException {
    Metrics metrics = new Metrics();
    assertTrue(
        new WatchRules(write(detection("selection")).toString(), metrics).current().isEmpty(),
        "a selection no condition references would silently never be evaluated");
    assertEquals(1, metrics.watchRulesSkipped.sum());
  }

  @Test
  void aDetectionRuleCarryingAFlatConditionBesideItIsRefused() throws IOException {
    Metrics metrics = new Metrics();
    Path file = write("""
        rules:
          - id: both
            severity: low
            prefix: lakehouse
            table: logs.events
            condition: all-of
            detection:
              selection:
                - { column: msg, equals: ["x"] }
              condition: selection
        """);
    assertTrue(new WatchRules(file.toString(), metrics).current().isEmpty(),
        "two answers to 'what is this rule's condition' is a refusal, not a merge");
    assertEquals(1, metrics.watchRulesSkipped.sum());
  }

  @Test
  void aSingleSelectionOnOneIndexedColumnStillRidesTheIndex() throws IOException {
    Path file = write("""
        rules:
          - id: one
            severity: low
            prefix: lakehouse
            table: logs.events
            detection:
              selection:
                - { column: msg, match: [error] }
              condition: selection
        """);
    WatchRule rule = new WatchRules(file.toString(), new Metrics()).current().get(0);
    assertTrue(rule.ridesIndex(), "one column, one match, a flat condition: the cheap path");
  }

  // ---------------------------------------------------------------- the new operators

  @Test
  void reAndNumericOperatorsParse() throws IOException {
    Path file = write("""
        rules:
          - id: ops
            severity: low
            prefix: lakehouse
            table: logs.events
            where:
              - { column: msg, re: ["^svc-[0-9]+$"] }
              - { column: num, gte: [400], lt: [500] }
            condition: all-of
        """);
    WatchRule rule = new WatchRules(file.toString(), new Metrics()).current().get(0);
    assertEquals(3, rule.where().size());
    assertEquals(WatchRule.Op.RE, rule.where().get(0).op());
    assertEquals(WatchRule.Op.GTE, rule.where().get(1).op());
    assertEquals(WatchRule.Op.LT, rule.where().get(2).op());
    assertFalse(rule.ridesIndex(), "no index answers a regex or a range");
  }

  @Test
  void anInvalidRegexAndANonNumericComparisonAreSkipped() throws IOException {
    Metrics badRegex = new Metrics();
    assertTrue(new WatchRules(write("""
        rules:
          - id: bad-re
            severity: low
            prefix: lakehouse
            table: logs.events
            where: [{ column: msg, re: ["([unclosed"] }]
        """).toString(), badRegex).current().isEmpty(), "an uncompilable regex never matches anything");

    Metrics badNumber = new Metrics();
    assertTrue(new WatchRules(write("""
        rules:
          - id: bad-gt
            severity: low
            prefix: lakehouse
            table: logs.events
            where: [{ column: num, gt: ["not-a-number"] }]
        """).toString(), badNumber).current().isEmpty(), "gt needs a number");
  }

  /**
   * The example the chart ships and the README points at must load, every rule of it. A shipped
   * example that is skipped at load teaches the wrong syntax to whoever copies it.
   */
  @Test
  void theShippedChartExampleLoadsEveryRule() {
    Path example = Path.of("..", "helm", "kahshe", "examples", "watch-rules.yaml");
    assertTrue(java.nio.file.Files.exists(example), "example not found at " + example.toAbsolutePath());
    Metrics metrics = new Metrics();
    List<WatchRule> loaded = new WatchRules(example.toString(), metrics).current();
    assertEquals(0, metrics.watchRulesSkipped.sum(), "the shipped example must not skip a rule");
    assertEquals(4, loaded.size(), "two single-column rules, one detection rule, one where rule");
  }

  /**
   * YAML reads {@code no}, {@code off} and {@code yes} as BOOLEANS, so an unquoted rule matching
   * the literal text "no" would silently match "false": it loads clean, reviews as correct, and
   * never fires. A silent false negative is the error a watcher may never make, so the loader
   * refuses it and says how to fix it. Verified red with the guard removed: the rule loads and
   * its value is "false".
   */
  @Test
  void anUnquotedYamlBooleanInAValueIsRefusedWithTheFix() throws IOException {
    Metrics metrics = new Metrics();
    Path file = write("""
        rules:
          - id: norway
            severity: high
            prefix: lakehouse
            table: logs.events
            where: [{ column: mfa, equals: [no] }]
        """);
    assertTrue(new WatchRules(file.toString(), metrics).current().isEmpty(),
        "matching the boolean false is not what this rule says");
    assertEquals(1, metrics.watchRulesSkipped.sum());
  }

  @Test
  void theSameValueQuotedIsTheTextItLooksLike() throws IOException {
    Path file = write("""
        rules:
          - id: norway-quoted
            severity: high
            prefix: lakehouse
            table: logs.events
            where: [{ column: mfa, equals: ["no", "off"] }]
        """);
    WatchRule rule = new WatchRules(file.toString(), new Metrics()).current().get(0);
    assertEquals(List.of("no", "off"), rule.where().get(0).values());
  }

  @Test
  void hotReloadPicksUpMtimeChanges() throws IOException {
    Path file = write(VALID);
    WatchRules rules = new WatchRules(file.toString(), new Metrics());
    assertEquals(2, rules.current().size());
    Files.writeString(
        file,
        """
        rules:
          - id: only-one
            severity: info
            prefix: p
            table: ns.t
            column: msg
            match: [warn]
        """);
    assertTrue(file.toFile().setLastModified(file.toFile().lastModified() + 5_000));
    assertEquals(1, rules.current().size());
    assertEquals("only-one", rules.current().get(0).id());
  }

  @Test
  void parseErrorKeepsPreviousSet() throws IOException {
    Path file = write(VALID);
    WatchRules rules = new WatchRules(file.toString(), new Metrics());
    assertEquals(2, rules.current().size());
    Files.writeString(file, "rules: {not: [a, list");
    assertTrue(file.toFile().setLastModified(file.toFile().lastModified() + 5_000));
    assertEquals(2, rules.current().size());
  }
}
