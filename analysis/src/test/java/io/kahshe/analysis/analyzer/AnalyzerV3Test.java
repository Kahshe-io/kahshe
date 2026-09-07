package io.kahshe.analysis.analyzer;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertTrue;

import java.util.ArrayList;
import java.util.List;
import java.util.Random;
import java.util.regex.Pattern;
import org.junit.jupiter.api.Test;

/**
 * v3 of the tokens analyzer: v2's pieces plus every compound identifier, whole. Three properties
 * pin it. Additive: every v2 token of a text is a v3 token of it. Shaped: only IPv4, IPv6, UUID and
 * dashed/underscored hex become compounds, after separators at the run's edges are stripped. And
 * sound against SQL: a row that the canonical {@code regexp_like} pattern for a needle matches is a
 * row whose v3 terms contain the needle — the property the dev/trino-patch recogniser relies on when
 * it turns that pattern into a token match, checked here over random text.
 *
 * <p>Verified red three times: with compound emission removed (the shapes fail); with edge
 * separators no longer stripped (the soundness property fails on {@code 10.0.4.17.}); and with the
 * compound pattern's literal changed on one side only (the agreement assertion fails).
 */
class AnalyzerV3Test {
  private static final Analyzer.Contract V3 =
      Analyzer.contract(Analyzer.Kind.TOKENS, Analyzer.DEFAULT_MAX_TOKEN_LEN);

  @Test
  void compoundsAreEmittedWholeAndInPieces() {
    assertEquals(
        List.of("10", "0", "4", "17", "10.0.4.17"),
        Analyzer.tokensV3("10.0.4.17"));
    assertEquals(
        List.of("connection", "refused", "peer", "10", "0", "4", "17", "10.0.4.17"),
        Analyzer.tokensV3("connection refused peer=10.0.4.17"),
        "a compound at the end of a run, after '=' which is outside the run class");
    assertEquals(
        List.of("10", "0", "4", "17", "10.0.4.17"),
        Analyzer.tokensV3("10.0.4.17."),
        "a trailing separator is stripped before the shape is judged");
    assertEquals(
        List.of("10", "0", "4", "17", "1"),
        Analyzer.tokensV3("10.0.4.17.1"),
        "five octets are not an IPv4 address: pieces only");
    assertEquals(
        List.of("fe80", "1", "fe80::1"), Analyzer.tokensV3("FE80::1"), "IPv6, lowercased");
    assertEquals(
        List.of("id", "de305d54", "75b4", "431b", "adb2", "eb6b9e546014", "de305d54-75b4-431b-adb2-eb6b9e546014"),
        Analyzer.tokensV3("id=de305d54-75b4-431b-adb2-eb6b9e546014;"));
    assertEquals(
        List.of("00d5", "c571", "00d5_c571"), Analyzer.tokensV3("00d5_c571"), "underscored hex");
    assertEquals(List.of("re", "do"), Analyzer.tokensV3("re-do"), "'r' and 'o' are not hex: no compound");
    assertEquals(List.of("10", "0", "4"), Analyzer.tokensV3("10.0.4"), "three octets are not an address");
    assertEquals(List.of("12", "30"), Analyzer.tokensV3("12:30"), "one colon is not IPv6");
  }

  @Test
  void queryTermsProbeTheCompoundAndTheUncoveredPieces() {
    assertEquals(List.of("10.0.4.17"), V3.queryTerms("10.0.4.17"));
    assertEquals(List.of("peer", "10.0.4.17", "refused"), V3.queryTerms("peer 10.0.4.17 refused"));
    assertEquals(List.of("10", "0", "4"), V3.queryTerms("10.0.4"), "no compound: the pieces");
    Analyzer.Contract v2 = Analyzer.contractOf(Analyzer.ID_PREFIX + "256");
    assertEquals(List.of("10", "0", "4", "17"), v2.queryTerms("10.0.4.17"), "v2 reads as v2");
    assertEquals(List.of("10", "0", "4", "17"), v2.tokens("10.0.4.17"));
  }

  @Test
  void v3IsAdditiveOverV2() {
    Random random = new Random(3);
    for (int i = 0; i < 2000; i++) {
      String text = randomText(random, 40);
      List<String> v3 = Analyzer.tokensV3(text);
      for (String token : Analyzer.tokenize(text)) {
        assertTrue(v3.contains(token), "v2 token '" + token + "' lost in v3 for: " + text);
      }
    }
  }

  @Test
  void theCanonicalPatternIsSoundAgainstTheIndex() {
    assertEquals("(^|[^a-z0-9])abc([^a-z0-9]|$)", Analyzer.matchPattern("abc"));
    // the literal the dev/trino-patch test asserts, verbatim: the two sides agree by this equality
    assertEquals(
        "(^|[^a-z0-9.:_-])[.:_-]*10\\.0\\.4\\.17[.:_-]*([^a-z0-9.:_-]|$)",
        Analyzer.matchPattern("10.0.4.17"));
    Random random = new Random(7);
    int matched = 0;
    for (int i = 0; i < 4000; i++) {
      String text = randomText(random, 30);
      List<String> terms = Analyzer.tokensV3(text);
      List<String> needles = new ArrayList<>(terms);
      needles.add(randomNeedle(random)); // and one the text may not hold at all
      for (String needle : needles) {
        String pattern;
        try {
          pattern = Analyzer.matchPattern(needle);
        } catch (IllegalArgumentException e) {
          continue;
        }
        if (Pattern.compile(pattern).matcher(text.toLowerCase(java.util.Locale.ROOT)).find()) {
          matched++;
          assertTrue(
              terms.contains(needle),
              "pattern for '" + needle + "' matches \"" + text + "\" but its terms are " + terms);
        }
      }
    }
    assertTrue(matched > 2000, "the property was exercised: " + matched + " matches");
    assertFalse(
        Pattern.compile(Analyzer.matchPattern("10.0.4.17")).matcher("x 10.0.4.17.1 y").find(),
        "the compound boundary refuses the row the index would not hold");
    assertTrue(
        Pattern.compile(Analyzer.matchPattern("10.0.4.17")).matcher("x 10.0.4.17. y").find(),
        "sentence-final punctuation is allowed on both sides");
  }

  private static final String ALPHABET = "aab1029.:_-  =fe";

  private static String randomText(Random random, int max) {
    int n = 1 + random.nextInt(max);
    StringBuilder b = new StringBuilder(n);
    for (int i = 0; i < n; i++) {
      b.append(ALPHABET.charAt(random.nextInt(ALPHABET.length())));
    }
    return b.toString();
  }

  private static String randomNeedle(Random random) {
    String[] pool = {"10.0.4.17", "a", "1", "fe80::1", "ab-1", "1.2.3.4", "a_b", "0.0.0.0", "ab"};
    return pool[random.nextInt(pool.length)];
  }
}
