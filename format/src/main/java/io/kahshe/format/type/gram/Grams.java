package io.kahshe.format.type.gram;

import java.util.HashSet;
import java.util.Locale;
import java.util.Set;
import io.kahshe.analysis.analyzer.Analyzer;

/**
 * The gram rule: how a value is cut into the grams the bloom and gram tiers hold. Pinned and
 * versioned like the analyzer ({@link Analyzer.Contract}), because build and probe must cut
 * identically and the artifact says which rule it was built under.
 *
 * <p>Two rules exist. {@code kahshe-grams-v1} is the original: windows of three UTF-16 code
 * units, which can split a surrogate pair -- a half character no Rust or Go string can hold,
 * so no second implementation could reproduce it. {@code kahshe-grams-v2-n<size>} cuts windows
 * of whole code points, {@code size} of them, {@value #MIN_SIZE}..{@value #MAX_SIZE}, resolved
 * per column, then table, then deployment default. v1 is read, never written.
 */
public final class Grams {
  private Grams() {}

  public static final int MIN_SIZE = 2;
  public static final int MAX_SIZE = 8;
  /** The deployment default, and v1's only size. */
  public static final int DEFAULT_SIZE = 3;
  static final String V1_ID = "kahshe-grams-v1";
  static final String V2_ID_PREFIX = "kahshe-grams-v2-n";

  /**
   * How the units of a window are counted. Not the set of rules — that is {@link GramRules}, which
   * the service loader extends — but the classification a rule declares so the accumulator and the
   * bloom probe know what to count. A rule discovered on the classpath picks one of the two.
   */
  public enum Rule {
    /** Windows of UTF-16 code units; the legacy rule, size fixed at three. */
    UTF16_V1,
    /** Windows of Unicode code points; the current rule. */
    CODEPOINT_V2
  }

  /** A rule and a size: what one index was, or will be, built under. */
  public record Contract(GramRule family, Rule rule, int size) {
    public Contract {
      if (family == null) {
        throw new IllegalArgumentException("a contract belongs to a gram rule");
      }
      if (rule == Rule.UTF16_V1 && size != DEFAULT_SIZE) {
        throw new IllegalArgumentException("kahshe-grams-v1 is always size 3, not " + size);
      }
      if (size < MIN_SIZE || size > MAX_SIZE) {
        throw new IllegalArgumentException(
            "gram size must be " + MIN_SIZE + ".." + MAX_SIZE + ", not " + size);
      }
    }

    /** A contract of the built-in rule for a classification; what a kahshe build cuts under. */
    public Contract(Rule rule, int size) {
      this(GramRules.builtin(rule), rule, size);
    }

    /** The current rule at the given size. */
    public static Contract current(int size) {
      return new Contract(Rule.CODEPOINT_V2, size);
    }

    /** The legacy rule. */
    public static Contract v1() {
      return new Contract(Rule.UTF16_V1, DEFAULT_SIZE);
    }

    /** The id written into the index metadata. */
    public String id() {
      return family.id(this);
    }

    /**
     * The contract an index metadata names. Absent (a document written before the rule had an id)
     * is v1; an id no loaded {@link GramRule} owns is refused, as an unknown analyzer is
     * (docs/FORMAT.md §8.2). {@link GramRules} is consulted in registration order: the built-ins
     * first, then whatever the service loader found.
     */
    public static Contract of(String id) {
      if (id == null || id.isBlank()) {
        return v1();
      }
      GramRule rule = GramRules.owner(id);
      if (rule == null) {
        throw new IllegalStateException(
            "unknown gram rule " + id + "; this reader loaded " + GramRules.names());
      }
      return rule.parse(id);
    }

    /** Whether {@code v} (already lowercased) is shorter than one window, so it is one gram whole. */
    public boolean shorterThanWindow(String v) {
      return units(v) < size;
    }

    /** The number of units the rule counts in {@code v}. */
    int units(String v) {
      return rule == Rule.UTF16_V1 ? v.length() : v.codePointCount(0, v.length());
    }

    /**
     * The grams of a value: lowercased once ({@code Locale.ROOT}), then one gram per window, or
     * the whole value as one gram when it is shorter than a window. The reference cut; the
     * accumulator and the bloom probe reproduce it without allocating.
     */
    public Set<String> gramsOf(String value) {
      Set<String> grams = new HashSet<>();
      String v = value.toLowerCase(Locale.ROOT);
      if (shorterThanWindow(v)) {
        grams.add(v);
        return grams;
      }
      forEachWindow(v, grams::add);
      return grams;
    }

    /** Every window of {@code v} (already lowercased, at least one window long), in order. */
    public void forEachWindow(String v, java.util.function.Consumer<String> window) {
      family.forEachWindow(this, v, window);
    }
  }
}
