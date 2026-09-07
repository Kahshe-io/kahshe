package io.kahshe.analysis.analyzer;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.junit.jupiter.api.Assertions.assertSame;
import static org.junit.jupiter.api.Assertions.assertTrue;

import java.util.ArrayList;
import java.util.HashSet;
import java.util.List;
import java.util.Set;
import org.junit.jupiter.api.Test;

/**
 * The analyzer seam: a family is discovered by the service loader and keyed by the id it owns, so
 * a language-specific tokenizer is added without editing an enum.
 *
 * <p>{@link TestOnlyAnalyzerFamily} is on the test classpath only, registered through
 * {@code src/test/resources/META-INF/services/io.kahshe.analysis.analyzer.AnalyzerFamily}. Verified red with
 * that services file removed: {@code aFamilyOnTheClasspathIsDiscovered} then fails with "no family
 * named test-words ... [ascii, value]" and the id parses to null, which is the discovery this
 * test is here for.
 */
class AnalyzerFamiliesTest {
  @Test
  void theBuiltInsAreDiscoveredFirstAndOnce() {
    List<String> names = new ArrayList<>();
    Analyzers.families().forEach(f -> names.add(f.name()));
    assertEquals(List.of("ascii", "value"), names.subList(0, 2),
        "the built-ins come first, so nothing on the classpath can shadow an id kahshe writes");
    Set<Class<?>> classes = new HashSet<>();
    for (AnalyzerFamily family : Analyzers.families()) {
      assertTrue(classes.add(family.getClass()),
          "a family listed in a services file AND registered as a built-in is folded, not doubled: "
              + family.getClass());
    }
    assertSame(Analyzers.owner("kahshe-ascii-v1"), Analyzers.owner("kahshe-ascii-v3-max256"),
        "one instance per family keeps Contract equality stable");
  }

  @Test
  void theBuiltInIdsStillParseExactlyAsBefore() {
    assertEquals(new Analyzer.Contract(Analyzer.Kind.TOKENS, 1, Integer.MAX_VALUE),
        Analyzer.contractOf("kahshe-ascii-v1"));
    assertEquals(new Analyzer.Contract(Analyzer.Kind.TOKENS, 2, 256),
        Analyzer.contractOf("kahshe-ascii-v2-max256"));
    assertEquals(new Analyzer.Contract(Analyzer.Kind.TOKENS, 3, 64),
        Analyzer.contractOf("kahshe-ascii-v3-max64"));
    assertEquals(new Analyzer.Contract(Analyzer.Kind.VALUE, 1, 256),
        Analyzer.contractOf("kahshe-value-v1-max256"));
    for (String id : List.of("kahshe-ascii-v1", "kahshe-ascii-v2-max256", "kahshe-ascii-v3-max64",
        "kahshe-value-v1-max256")) {
      assertEquals(id, Analyzer.contractOf(id).id(), "the id round-trips through its family");
    }
  }

  @Test
  void aFamilyOnTheClasspathIsDiscovered() {
    AnalyzerFamily found = null;
    for (AnalyzerFamily family : Analyzers.families()) {
      if (family.name().equals("test-words")) {
        found = family;
      }
    }
    assertNotNull(found, "no family named test-words among " + Analyzers.names()
        + "; the service loader did not find the one on the test classpath");
    assertEquals(Analyzer.Kind.TOKENS, found.kind());
  }

  @Test
  void theDiscoveredFamilyParsesTokenizesAndRoundTripsItsId() {
    String id = TestOnlyAnalyzerFamily.ID_PREFIX + 64;
    Analyzer.Contract contract = Analyzer.contractOf(id);
    assertNotNull(contract, "an id a loaded family owns is not foreign");
    assertEquals("test-words", contract.family().name());
    assertEquals(64, contract.maxTokenLen());
    assertEquals(List.of("Alpha", "beta-42"), contract.tokens("Alpha  beta-42"),
        "the registry tokenizes through the family, not through the ASCII one");
    assertEquals(contract.tokens("Alpha  beta-42"), contract.queryTerms("Alpha  beta-42"));
    assertTrue(contract.isIndexable("x".repeat(64)));
    assertTrue(contract.prefixable("Al"), "this family admits a prefix the ASCII one would not");
    assertEquals(id, contract.id(), "the id round-trips");
  }

  @Test
  void aForeignIdIsStillNull() {
    assertNull(Analyzer.contractOf("lucene-standard-v1"), "another engine's analyzer");
    assertNull(Analyzer.contractOf("kahshe-ascii-v4-max256"), "a version no family owns");
    assertNull(Analyzer.contractOf("kahshe-ascii-v2-max0"), "a cap that is not positive");
    assertNull(Analyzer.contractOf("kahshe-ascii-v2-maxwide"), "a cap that is not a number");
    assertNull(Analyzer.contractOf("kahshe-test-words-v2-max64"), "a version the test family does not own");
    assertNull(Analyzer.contractOf(null), "an index metadata with no analyzer property");
    assertNull(Analyzer.contractOf(""), "an empty analyzer property");
  }
}
