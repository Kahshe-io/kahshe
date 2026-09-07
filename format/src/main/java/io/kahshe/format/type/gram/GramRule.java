package io.kahshe.format.type.gram;

import java.util.function.Consumer;

/**
 * One gram rule: the owner of a prefix of the gram-id space and the whole of how a value is cut
 * into the grams the bloom and gram tiers hold.
 *
 * <p>Discovered by {@link java.util.ServiceLoader} through
 * {@code META-INF/services/io.kahshe.format.type.gram.GramRule}; the built-ins are
 * {@link Utf16GramRule} and {@link CodePointGramRule}.
 *
 * <p>What an implementation must honour: {@link #owns} is a pure test on the id text alone and false
 * for every id another rule owns; {@link #parse} is its partner and {@link #id} its inverse; and the
 * cut must be deterministic per id, since a rule that cuts differently for one id in two processes
 * desynchronises every index it wrote.
 *
 * <p>Probing goes through {@link #forEachWindow} and the build through
 * {@link #forEachWindowUnits}, which defaults to decomposing the first; overriding it is the one
 * place the two can be made to disagree, and its contract says what that costs.
 */
public interface GramRule {
  /** A short, stable name for this rule; used in logs and to say which rules are loaded. */
  String name();

  /** Whether {@code id} names a contract of this rule. Pure, and false for a null id. */
  boolean owns(String id);

  /** The contract {@code id} names. Called only when {@link #owns} said yes. */
  Grams.Contract parse(String id);

  /** Every window of {@code v} (already lowercased, at least one window long), in order. */
  void forEachWindow(Grams.Contract contract, String v, Consumer<String> window);

  /** One window, as {@code count} units in {@code units[0..count)}. The array is reused. */
  @FunctionalInterface
  interface Units {
    void window(int[] units, int count);
  }

  /**
   * The same windows as {@link #forEachWindow}, delivered as primitive units instead of Strings.
   *
   * <p>This exists for the build, which walks every gram position of every value and cannot
   * afford a String per position. The default implementation is correct for any rule and simply
   * decomposes what {@link #forEachWindow} yields, so a rule that does not override it still
   * builds an index its own probe can read; overriding it is a performance choice, not a
   * correctness one.
   *
   * <p>A unit is a UTF-16 code unit under {@link Grams.Rule#UTF16_V1} and a code point under
   * {@link Grams.Rule#CODEPOINT_V2}, matching how the accumulator encodes and decodes them.
   * An override MUST agree with {@link #forEachWindow} — they are two views of one cut, and a
   * rule whose two views disagree builds an index under one and probes it under the other, which
   * loses files silently.
   */
  default void forEachWindowUnits(Grams.Contract contract, String v, Units sink) {
    int[] buffer = new int[contract.size()];
    boolean utf16 = contract.rule() == Grams.Rule.UTF16_V1;
    forEachWindow(contract, v, gram -> {
      int n = 0;
      if (utf16) {
        for (int i = 0; i < gram.length() && n < buffer.length; i++) {
          buffer[n++] = gram.charAt(i);
        }
      } else {
        for (int i = 0; i < gram.length() && n < buffer.length; ) {
          int cp = gram.codePointAt(i);
          buffer[n++] = cp;
          i += Character.charCount(cp);
        }
      }
      sink.window(buffer, n);
    });
  }

  /** The id an index built under {@code contract} records; the inverse of {@link #parse}. */
  String id(Grams.Contract contract);
}
