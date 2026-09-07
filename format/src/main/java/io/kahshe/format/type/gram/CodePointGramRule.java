package io.kahshe.format.type.gram;

import java.util.function.Consumer;

/**
 * The built-in current rule, {@code kahshe-grams-v2-n<size>}: windows of whole Unicode code
 * points, {@code size} of them, so a gram is something a second implementation in Rust or Go can
 * reproduce byte for byte.
 */
public final class CodePointGramRule implements GramRule {
  @Override
  public String name() {
    return "codepoint-v2";
  }

  @Override
  public boolean owns(String id) {
    return id != null && id.startsWith(Grams.V2_ID_PREFIX);
  }

  @Override
  public Grams.Contract parse(String id) {
    try {
      return new Grams.Contract(
          this,
          Grams.Rule.CODEPOINT_V2,
          Integer.parseInt(id.substring(Grams.V2_ID_PREFIX.length())));
    } catch (IllegalArgumentException e) {
      throw new IllegalStateException("unreadable gram rule id " + id, e);
    }
  }

  @Override
  public void forEachWindow(Grams.Contract contract, String v, Consumer<String> window) {
    int size = contract.size();
    int count = v.codePointCount(0, v.length());
    int start = 0;
    for (int w = 0; w + size <= count; w++) {
      int end = v.offsetByCodePoints(start, size);
      window.accept(v.substring(start, end));
      start = v.offsetByCodePoints(start, 1);
    }
  }

  /** The same windows as {@link #forEachWindow}, without a String per position. */
  @Override
  public void forEachWindowUnits(Grams.Contract contract, String v, Units sink) {
    int size = contract.size();
    int count = v.codePointCount(0, v.length());
    int[] units = new int[size];
    int start = 0;
    for (int w = 0; w + size <= count; w++) {
      int at = start;
      for (int j = 0; j < size; j++) {
        int cp = v.codePointAt(at);
        units[j] = cp;
        at += Character.charCount(cp);
      }
      sink.window(units, size);
      start = v.offsetByCodePoints(start, 1);
    }
  }

  @Override
  public String id(Grams.Contract contract) {
    return Grams.V2_ID_PREFIX + contract.size();
  }
}
