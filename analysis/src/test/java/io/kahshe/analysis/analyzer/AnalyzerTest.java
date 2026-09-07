package io.kahshe.analysis.analyzer;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertTrue;

import java.util.List;
import org.junit.jupiter.api.Test;

class AnalyzerTest {

  /**
   * A contract that cannot round-trip through its own id must not construct: a cap of 0 makes an
   * id ("...-max0") contractOf cannot parse, so every reader refuses the index and the term tier
   * prunes nothing, behind one WARN per load; a kind its family does not own parses back as the
   * family's, silently overwriting the claim.
   */
  @org.junit.jupiter.api.Test
  void aContractThatCannotRoundTripDoesNotConstruct() {
    org.junit.jupiter.api.Assertions.assertThrows(IllegalArgumentException.class,
        () -> Analyzer.contract(Analyzer.Kind.TOKENS, 0), "a cap of 0 is not unlimited");
    org.junit.jupiter.api.Assertions.assertThrows(IllegalArgumentException.class,
        () -> Analyzer.contract(Analyzer.Kind.TOKENS, -5));
    Analyzer.Contract ok = Analyzer.contract(Analyzer.Kind.TOKENS, 256);
    org.junit.jupiter.api.Assertions.assertThrows(IllegalArgumentException.class,
        () -> new Analyzer.Contract(ok.family(), Analyzer.Kind.VALUE, ok.version(), 256),
        "the family decides the kind");
    org.junit.jupiter.api.Assertions.assertEquals(ok, Analyzer.contractOf(ok.id()),
        "and every contract that constructs round-trips through its id");
  }

  @Test
  void tokenizesOnNonAlphanumericRuns() {
    assertEquals(
        List.of("connection", "refused", "peer", "10", "0", "4", "17"),
        Analyzer.tokenize("connection refused peer=10.0.4.17"));
  }

  @Test
  void foldsAsciiCase() {
    assertEquals(List.of("quorum", "epoch", "777"), Analyzer.tokenize("Quorum-Epoch-777"));
  }

  @Test
  void emptyAndSeparatorOnlyInputs() {
    assertEquals(List.of(), Analyzer.tokenize(""));
    assertEquals(List.of(), Analyzer.tokenize("=== --- ..."));
  }

  @Test
  void nonAsciiTerminatesTokens() {
    assertEquals(List.of("caf", "log"), Analyzer.tokenize("café log"));
  }

  /**
   * Indexability is LENGTH, and nothing else.
   *
   * <p>A rule that discriminates on character class instead — v1's, which excludes pure-numeric
   * tokens over four digits — leaves a 12-digit order id out of the dictionary while the
   * 16-character hex trace id beside it goes in: same cardinality, same cost, decided by which
   * characters happened to appear, and the token left out is exactly the kind of needle a user
   * searches for. The symmetry assertions below are the point of the rule: two tokens of equal
   * length are treated identically whatever they contain.
   */
  @Test
  void indexabilityIsLengthAndNothingElse() {
    int cap = Analyzer.DEFAULT_MAX_TOKEN_LEN;
    assertTrue(Analyzer.contract(Analyzer.Kind.TOKENS, cap).isIndexable("connection"));
    assertTrue(Analyzer.contract(Analyzer.Kind.TOKENS, cap).isIndexable("9092"));
    assertTrue(Analyzer.contract(Analyzer.Kind.TOKENS, cap).isIndexable("cell1234567"));

    // The case a character-class rule refuses, and the pair that shows why length alone decides.
    assertTrue(Analyzer.contract(Analyzer.Kind.TOKENS, cap).isIndexable("48291733"), "a pure-numeric id is indexed now");
    assertEquals(
        Analyzer.contract(Analyzer.Kind.TOKENS, cap).isIndexable("88213347abcd"),
        Analyzer.contract(Analyzer.Kind.TOKENS, cap).isIndexable("882133470000"),
        "two tokens of the same length must be treated the same whatever characters they hold");

    // The bound itself, from both sides.
    assertTrue(Analyzer.contract(Analyzer.Kind.TOKENS, cap).isIndexable("x".repeat(cap)), "exactly the cap is admitted");
    assertFalse(Analyzer.contract(Analyzer.Kind.TOKENS, cap).isIndexable("x".repeat(cap + 1)), "one past it is not");
    assertFalse(Analyzer.contract(Analyzer.Kind.TOKENS, cap).isIndexable("1".repeat(cap + 1)), "and digits are not special");
  }

  /**
   * The analyzer's edge behaviour, pinned.
   *
   * <p>Golden assertions, not self-consistency ones. {@link Analyzer} defines the term space on
   * both sides — the build tokenizes a data file with it, the query tokenizes a predicate with it —
   * so if its rules change, both change together, every existing index silently disagrees with
   * every new query, and a test that only checked the two against each other would still pass.
   * These state what the analyzer must produce for inputs chosen at the boundaries; a change that
   * alters them is a term-space change, requiring a new analyzer id and a full reindex.
   */
  @Test
  void theTermSpaceIsPinnedAtItsBoundaries() {
    // ASCII letters fold to lower case; digits are kept; everything else is a separator
    assertEquals(List.of("abc", "def"), Analyzer.tokenize("ABC-def"));
    assertEquals(List.of("a1b2"), Analyzer.tokenize("a1B2"));
    assertEquals(List.of(), Analyzer.tokenize("...---..."));
    assertEquals(List.of(), Analyzer.tokenize(""));

    // NON-ASCII IS A SEPARATOR, not a letter. This is the property the analyzer's name promises
    // and the one most likely to be "improved" by accident: unicode letters split tokens rather
    // than joining them, so "naïve" is two tokens and an emoji between words separates them.
    assertEquals(List.of("caf"), Analyzer.tokenize("café"));
    assertEquals(List.of("na", "ve"), Analyzer.tokenize("naïve"));
    assertEquals(List.of("a", "b"), Analyzer.tokenize("a\uD83D\uDE00b"));
    assertEquals(List.of("alpha", "beta"), Analyzer.tokenize("alpha\u00A0beta"));
    // Greek final sigma: NOT folded, because folding it is what lets a truly-matching file be
    // pruned away. It is a separator like any other non-ASCII character.
    assertEquals(List.of(), Analyzer.tokenize("\u03C2"));

    // indexability is length alone; see indexabilityIsLengthAndNothingElse for the bound
    assertTrue(Analyzer.contract(Analyzer.Kind.TOKENS, 5).isIndexable("12345"), "exactly the cap is admitted");
    assertFalse(Analyzer.contract(Analyzer.Kind.TOKENS, 5).isIndexable("123456"), "one past it is not");
    assertTrue(Analyzer.contract(Analyzer.Kind.TOKENS, 5).isIndexable("1234a"), "characters do not enter into it");
    assertTrue(Analyzer.contract(Analyzer.Kind.TOKENS, 5).isIndexable("a"), "a single letter is indexable");
    assertTrue(Analyzer.contract(Analyzer.Kind.TOKENS, 5).isIndexable(""), "the empty token is not special-cased here");

    // a long run is one token, not a truncation or a split
    String long_ = "a".repeat(5000);
    assertEquals(List.of(long_), Analyzer.tokenize(long_), "a long run is ONE token, not split");
    // ...but it is not INDEXED. Without a length cap a 5000-character run -- a base64 blob, a
    // stack frame with the spaces eaten -- goes into the dictionary. The cap is the valve
    // Elasticsearch's ignore_above is; tokenizing is unaffected, only indexability.
    assertFalse(Analyzer.contract(Analyzer.Kind.TOKENS, Analyzer.DEFAULT_MAX_TOKEN_LEN).isIndexable(long_),
        "a 5000-character token is past the default cap and is not indexed");
    assertTrue(Analyzer.contract(Analyzer.Kind.TOKENS, 5000).isIndexable(long_), "a column that wants it can raise the cap");

    // and tokenizing is a pure function of the text: the build and the query must never diverge
    String mixed = "ERROR 404 at /var/log/app.log: connexion échouée (id=99999)";
    assertEquals(Analyzer.tokenize(mixed), Analyzer.tokenize(mixed));
    assertEquals(
        List.of("error", "404", "at", "var", "log", "app", "log", "connexion", "chou", "e", "id",
            "99999"),
        Analyzer.tokenize(mixed));
  }
}