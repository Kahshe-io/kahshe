package io.kahshe.proxy.plan;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

import io.kahshe.format.IndexPruner;
import org.junit.jupiter.api.Test;

class ContainsExtractorTest {
  /**
   * The ordinary case: no table has a column literally named {@code __kahshe_match__something}, so
   * every sentinel here is a sentinel. Spelled once, here, rather than defaulted inside
   * {@code extract} — the whole point of that parameter is that there is no safe default for it.
   */
  private static ContainsExtractor.Extraction extract(String json) {
    return ContainsExtractor.extract(json, name -> false);
  }

  @Test
  void stripsTopLevelContains() {
    var extraction =
        extract(
            "{\"filter\":{\"type\":\"contains\",\"term\":\"msg\",\"value\":\"timeout\"}}");
    assertEquals(1, extraction.hints().size());
    assertEquals(IndexPruner.HintKind.CONTAINS, extraction.hints().get(0).kind());
    assertTrue(extraction.cleanedJson().contains("\"filter\":true"));
  }

  @Test
  void matchYieldsMatchHint() {
    var extraction =
        extract(
            "{\"filter\":{\"type\":\"match\",\"term\":\"msg\",\"value\":\"connection\"}}");
    assertEquals(IndexPruner.HintKind.MATCH, extraction.hints().get(0).kind());
  }

  @Test
  void stripsInsideConjunction() {
    var extraction =
        extract(
            "{\"filter\":{\"type\":\"and\","
                + "\"left\":{\"type\":\"gt-eq\",\"term\":\"id\",\"value\":5},"
                + "\"right\":{\"type\":\"contains\",\"term\":\"msg\",\"value\":\"x\"}}}");
    assertEquals(1, extraction.hints().size());
    assertTrue(extraction.cleanedJson().contains("gt-eq"));
    assertTrue(extraction.cleanedJson().contains("\"right\":true"));
  }

  @Test
  void rejectsUnderOr() {
    assertThrows(
        IllegalArgumentException.class,
        () ->
            extract(
                "{\"filter\":{\"type\":\"or\","
                    + "\"left\":{\"type\":\"contains\",\"term\":\"msg\",\"value\":\"x\"},"
                    + "\"right\":{\"type\":\"eq\",\"term\":\"id\",\"value\":1}}}"));
  }

  @Test
  void rejectsUnderNot() {
    assertThrows(
        IllegalArgumentException.class,
        () ->
            extract(
                "{\"filter\":{\"type\":\"not\","
                    + "\"child\":{\"type\":\"match\",\"term\":\"msg\",\"value\":\"x\"}}}"));
  }

  @Test
  void passesThroughStandardFilters() {
    String body = "{\"filter\":{\"type\":\"eq\",\"term\":\"id\",\"value\":7}}";
    var extraction = extract(body);
    assertTrue(extraction.hints().isEmpty());
    assertTrue(extraction.cleanedJson().contains("\"eq\""));
  }

  @Test
  void noFilterIsFine() {
    assertTrue(extract("{}").hints().isEmpty());
  }

  @Test
  void extractsSpecApplyForm() {
    var extraction = extract(
        "{\"filter\":{\"type\":\"apply\","
            + "\"function\":{\"catalog\":\"iceberg_functions\",\"identifier\":[\"contains\"]},"
            + "\"arguments\":[{\"type\":\"reference\",\"id\":5},\"needle\"]}}");
    assertEquals(1, extraction.hints().size());
    assertEquals(5, extraction.hints().get(0).fieldId());
    assertEquals("needle", extraction.hints().get(0).value());
    assertTrue(extraction.cleanedJson().contains("\"filter\":true"));
  }

  @Test
  void leavesUnknownApplyFunctionsUntouched() {
    var extraction = extract(
        "{\"filter\":{\"type\":\"apply\","
            + "\"function\":{\"catalog\":\"iceberg_functions\",\"identifier\":[\"bucket\"]},"
            + "\"arguments\":[256,{\"type\":\"reference\",\"id\":1}]}}");
    assertTrue(extraction.hints().isEmpty());
    assertTrue(extraction.cleanedJson().contains("bucket"));
  }

  @Test
  void rejectsForeignCatalogFunctions() {
    var extraction = extract(
        "{\"filter\":{\"type\":\"apply\","
            + "\"function\":{\"catalog\":\"my_udfs\",\"identifier\":[\"match\"]},"
            + "\"arguments\":[{\"type\":\"reference\",\"id\":5},\"needle\"]}}");
    assertTrue(extraction.hints().isEmpty(), "foreign-catalog match must not become a hint");
    assertTrue(extraction.cleanedJson().contains("my_udfs"));
  }

  @Test
  void rejectsExtraApplyArguments() {
    assertThrows(
        IllegalArgumentException.class,
        () -> extract(
            "{\"filter\":{\"type\":\"apply\","
                + "\"function\":{\"catalog\":\"iceberg_functions\",\"identifier\":[\"contains\"]},"
                + "\"arguments\":[{\"type\":\"reference\",\"id\":5},\"alpha\",\"beta\"]}}"));
  }

  // ---------------------------------------------------------------- sentinel terms
  //
  // The forms above can only reach kahshe from a client writing the JSON by hand: Iceberg's
  // algebra has no token operation and ExpressionParser.toJson has no extension point, so no
  // engine can serialize them. The sentinel form exists so a query engine can push a token
  // predicate using nothing but an ordinary `eq`, which every serializer already emits.

  @Test
  void aSentinelTermCarriesAMatchHintThroughAnOrdinaryEq() {
    var extraction =
        extract(
            "{\"filter\":{\"type\":\"eq\",\"term\":\"__kahshe_match__msg\","
                + "\"value\":\"00d5c571b1547b51\"}}");
    assertEquals(1, extraction.hints().size());
    assertEquals(IndexPruner.HintKind.MATCH, extraction.hints().get(0).kind());
    // The column is the suffix, not the sentinel term: pruning must consult msg's index.
    assertEquals("msg", extraction.hints().get(0).column());
    assertEquals("00d5c571b1547b51", extraction.hints().get(0).value());
  }

  @Test
  void aSentinelTermIsStrippedBeforeTheStandardParserSeesIt() {
    // Load-bearing rather than cosmetic: __kahshe_match__msg is not a column, so leaving it in
    // the filter makes ExpressionParser fail to bind it and the whole plan request errors.
    var extraction =
        extract(
            "{\"filter\":{\"type\":\"eq\",\"term\":\"__kahshe_match__msg\",\"value\":\"x\"}}");
    assertTrue(extraction.cleanedJson().contains("\"filter\":true"));
    assertFalse(extraction.cleanedJson().contains("__kahshe_match__"));
  }

  @Test
  void aContainsSentinelYieldsAContainsHint() {
    var extraction =
        extract(
            "{\"filter\":{\"type\":\"eq\",\"term\":\"__kahshe_contains__msg\",\"value\":\"tim\"}}");
    assertEquals(IndexPruner.HintKind.CONTAINS, extraction.hints().get(0).kind());
    assertEquals("msg", extraction.hints().get(0).column());
  }

  @Test
  void anOrdinaryEqOnARealColumnIsNotASentinel() {
    // The discriminator is the prefix. An eq on a real column must survive untouched, or every
    // engine-pushed equality would be silently replaced with `true` -- dropping the predicate.
    var extraction =
        extract(
            "{\"filter\":{\"type\":\"eq\",\"term\":\"level\",\"value\":\"ERROR\"}}");
    assertEquals(0, extraction.hints().size());
    assertTrue(extraction.cleanedJson().contains("\"term\":\"level\""));
  }

  @Test
  void aSentinelCarriedByAnOperationOtherThanEqIsLeftAlone() {
    // Producer and reader disagreeing about the encoding must not be guessed at: replacing this
    // with `true` would drop a predicate the engine believes it pushed, which is a false negative.
    // Left alone, the nonexistent column reaches ExpressionParser and fails the plan loudly.
    var extraction =
        extract(
            "{\"filter\":{\"type\":\"starts-with\",\"term\":\"__kahshe_match__msg\","
                + "\"value\":\"x\"}}");
    assertEquals(0, extraction.hints().size());
    assertTrue(extraction.cleanedJson().contains("__kahshe_match__msg"));
  }

  @Test
  void aSentinelUnderOrIsRejected() {
    // Same reason the other forms are: replacing it with `true` under OR widens the scan's
    // semantics, and pruning on it narrows them.
    assertThrows(
        IllegalArgumentException.class,
        () ->
            extract(
                "{\"filter\":{\"type\":\"or\","
                    + "\"left\":{\"type\":\"eq\",\"term\":\"level\",\"value\":\"ERROR\"},"
                    + "\"right\":{\"type\":\"eq\",\"term\":\"__kahshe_match__msg\","
                    + "\"value\":\"x\"}}}"));
  }

  @Test
  void aSentinelInsideAConjunctionIsStrippedAndTheRestSurvives() {
    var extraction =
        extract(
            "{\"filter\":{\"type\":\"and\","
                + "\"left\":{\"type\":\"gt-eq\",\"term\":\"id\",\"value\":5},"
                + "\"right\":{\"type\":\"eq\",\"term\":\"__kahshe_match__msg\","
                + "\"value\":\"x\"}}}");
    assertEquals(1, extraction.hints().size());
    assertEquals(IndexPruner.HintKind.MATCH, extraction.hints().get(0).kind());
    assertTrue(extraction.cleanedJson().contains("gt-eq"));
    assertTrue(extraction.cleanedJson().contains("\"right\":true"));
  }

  @Test
  void aSentinelWithNoColumnSuffixIsRejected() {
    assertThrows(
        IllegalArgumentException.class,
        () ->
            extract(
                "{\"filter\":{\"type\":\"eq\",\"term\":\"__kahshe_match__\",\"value\":\"x\"}}"));
  }

  @Test
  void aSentinelWithANonStringValueIsRejected() {
    // An eq against a number cannot be a token. Accepting it would coerce silently.
    assertThrows(
        IllegalArgumentException.class,
        () ->
            extract(
                "{\"filter\":{\"type\":\"eq\",\"term\":\"__kahshe_match__msg\",\"value\":7}}"));
  }

  /**
   * A table may legitimately declare a column named {@code __kahshe_match__foo}. The prefix is a
   * convention kahshe picked, not a name it owns.
   *
   * <p>Read as a sentinel, that predicate becomes a MATCH hint on a DIFFERENT column, {@code foo},
   * and every file whose {@code foo} lacks the token is pruned — including the ones whose
   * {@code __kahshe_match__foo} matched exactly. Rows vanish with no error and no metric, which is
   * the one failure kahshe forbids outright.
   *
   * <p>So a real column wins and the sentinel loses: no hint, and the node stays in the filter to
   * be planned as the ordinary predicate it is. That costs pruning and never rows.
   *
   * <p>Verified by breaking it: dropping the {@code isRealColumn} check from {@code sentinelHint}
   * produces a MATCH hint on {@code msg} and rewrites the filter to {@code true}.
   */
  @Test
  void aRealColumnNamedLikeTheSentinelIsNotReadAsOne() {
    String body =
        "{\"filter\":{\"type\":\"eq\",\"term\":\"__kahshe_match__msg\",\"value\":\"x\"}}";

    var collides =
        ContainsExtractor.extract(body, name -> name.equals("__kahshe_match__msg"));
    assertTrue(collides.hints().isEmpty(), "a real column must not become a hint on another column");
    assertTrue(
        collides.cleanedJson().contains("__kahshe_match__msg"),
        "the predicate must survive to be planned normally: " + collides.cleanedJson());

    // The control. Same request against a table WITHOUT that column is still the sentinel path,
    // or this test would pass just as well against a build that had deleted the feature.
    var sentinel = extract(body);
    assertEquals(1, sentinel.hints().size());
    assertEquals(IndexPruner.HintKind.MATCH, sentinel.hints().get(0).kind());
    assertEquals("msg", sentinel.hints().get(0).column());
    assertTrue(sentinel.cleanedJson().contains("\"filter\":true"));
  }

  /**
   * And it must stay usable where an ordinary predicate is allowed but an extension is not.
   *
   * <p>Extensions are refused under OR/NOT because stripping one there would change scan
   * semantics. A real column that merely looks like a sentinel is not an extension, so refusing it
   * would make an ordinary column unusable in half of SQL — correct in that it returns no wrong
   * rows, and wrong in every other way.
   *
   * <p>Verified by breaking it: leaving {@code extensionAnywhere} unaware of the schema throws
   * here.
   */
  @Test
  void aRealColumnNamedLikeTheSentinelIsStillUsableUnderOr() {
    String body =
        "{\"filter\":{\"type\":\"or\","
            + "\"left\":{\"type\":\"eq\",\"term\":\"__kahshe_match__msg\",\"value\":\"x\"},"
            + "\"right\":{\"type\":\"eq\",\"term\":\"id\",\"value\":5}}}";

    var extraction = ContainsExtractor.extract(body, name -> name.equals("__kahshe_match__msg"));
    assertTrue(extraction.hints().isEmpty());
    assertTrue(extraction.cleanedJson().contains("__kahshe_match__msg"));

    // The control: a genuine sentinel under OR is still refused.
    assertThrows(IllegalArgumentException.class, () -> extract(body));
  }

  private static final String SPEC_FORM =
      "{\"type\":\"eq\","
          + "\"left\":{\"type\":\"apply\","
          + "\"function\":{\"catalog\":\"kahshe_functions\",\"identifier\":[\"match\"]},"
          + "\"arguments\":[{\"type\":\"reference\",\"name\":\"msg\"},\"needle\"]},"
          + "\"right\":true}";

  /**
   * The merged expressions spec (apache/iceberg main, format/expressions-spec.md, Appendix B): a
   * value expression is not a predicate, so a boolean function arrives as {@code eq(apply, true)}
   * with {@code left}/{@code right}, and a scan-planning client references the column by NAME.
   * Verified red with the wrapped-apply branch removed: the comparison passes through untouched
   * and no hint is produced.
   */
  @Test
  void extractsTheMergedSpecComparisonForm() {
    var extraction = extract("{\"filter\":" + SPEC_FORM + "}");
    assertEquals(1, extraction.hints().size());
    assertEquals("msg", extraction.hints().get(0).column());
    assertEquals(-1, extraction.hints().get(0).fieldId());
    assertEquals("needle", extraction.hints().get(0).value());
    assertEquals(IndexPruner.HintKind.MATCH, extraction.hints().get(0).kind());
    assertTrue(extraction.cleanedJson().contains("\"filter\":true"), extraction.cleanedJson());
  }

  @Test
  void theDeprecatedTermValueWrappingAndLiteralObjectsAreAccepted() {
    var extraction = extract(
        "{\"filter\":{\"type\":\"eq\","
            + "\"term\":{\"type\":\"apply\","
            + "\"function\":{\"catalog\":\"kahshe_functions\",\"identifier\":[\"contains\"]},"
            + "\"arguments\":[{\"type\":\"reference\",\"term\":\"msg\"},"
            + "{\"type\":\"literal\",\"value\":\"needle\"}]},"
            + "\"value\":{\"type\":\"literal\",\"value\":true}}}");
    assertEquals(1, extraction.hints().size());
    assertEquals("msg", extraction.hints().get(0).column());
    assertEquals("needle", extraction.hints().get(0).value());
    assertEquals(IndexPruner.HintKind.CONTAINS, extraction.hints().get(0).kind());
  }

  @Test
  void anApplyComparedToAnythingButTrueIsLeftForTheStandardParser() {
    var extraction = extract("{\"filter\":" + SPEC_FORM.replace("\"right\":true", "\"right\":false") + "}");
    assertTrue(extraction.hints().isEmpty(), "eq-to-false is not a hint");
    assertTrue(extraction.cleanedJson().contains("apply"), "and the node is not consumed");
    var notEq = extract("{\"filter\":" + SPEC_FORM.replace("\"eq\"", "\"not-eq\"") + "}");
    assertTrue(notEq.hints().isEmpty());
  }

  @Test
  void aSentinelInTheComparisonFormCarriesItsHint() {
    var extraction = extract(
        "{\"filter\":{\"type\":\"eq\","
            + "\"left\":{\"type\":\"reference\",\"name\":\"__kahshe_match__msg\"},"
            + "\"right\":\"needle\"}}");
    assertEquals(1, extraction.hints().size(), "verified red with the left-reference reading removed");
    assertEquals("msg", extraction.hints().get(0).column());
    assertEquals(IndexPruner.HintKind.MATCH, extraction.hints().get(0).kind());
    assertTrue(extraction.cleanedJson().contains("\"filter\":true"));
  }

  @Test
  void theComparisonFormUnderOrIsRejectedLikeEveryOtherHint() {
    assertThrows(
        IllegalArgumentException.class,
        () -> extract("{\"filter\":{\"type\":\"or\",\"left\":" + SPEC_FORM
            + ",\"right\":{\"type\":\"eq\",\"term\":\"level\",\"value\":\"x\"}}}"));
  }

  @Test
  void matchPrefixYieldsAPrefixHint() {
    var extraction = extract("{\"filter\":{\"type\":\"match_prefix\",\"term\":\"msg\",\"value\":\"71.162.\"}}");
    assertEquals(1, extraction.hints().size(), "verified red with match_prefix unrecognised");
    assertEquals(IndexPruner.HintKind.PREFIX, extraction.hints().get(0).kind());
    assertEquals("71.162.", extraction.hints().get(0).value());
    assertTrue(extraction.cleanedJson().contains("\"filter\":true"));
    assertThrows(
        IllegalArgumentException.class,
        () -> extract("{\"filter\":{\"type\":\"or\",\"left\":{\"type\":\"match_prefix\",\"term\":\"msg\",\"value\":\"x\"},"
            + "\"right\":{\"type\":\"eq\",\"term\":\"level\",\"value\":\"x\"}}}"));
  }
}
