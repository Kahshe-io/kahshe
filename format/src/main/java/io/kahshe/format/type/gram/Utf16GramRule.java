package io.kahshe.format.type.gram;

import java.util.function.Consumer;

/**
 * The built-in legacy rule, {@code kahshe-grams-v1}: windows of three UTF-16 code units, which can
 * split a surrogate pair. Read, never written — a v1 index keeps pruning, and reproducing its cut
 * exactly is the only way that stays true.
 */
public final class Utf16GramRule implements GramRule {
  @Override
  public String name() {
    return "utf16-v1";
  }

  @Override
  public boolean owns(String id) {
    return Grams.V1_ID.equals(id);
  }

  @Override
  public Grams.Contract parse(String id) {
    return new Grams.Contract(this, Grams.Rule.UTF16_V1, Grams.DEFAULT_SIZE);
  }

  @Override
  public void forEachWindow(Grams.Contract contract, String v, Consumer<String> window) {
    for (int i = 0; i <= v.length() - contract.size(); i++) {
      window.accept(v.substring(i, i + contract.size()));
    }
  }

  /** The same windows as {@link #forEachWindow}, without a String per position. */
  @Override
  public void forEachWindowUnits(Grams.Contract contract, String v, Units sink) {
    int size = contract.size();
    int[] units = new int[size];
    for (int i = 0; i <= v.length() - size; i++) {
      for (int j = 0; j < size; j++) {
        units[j] = v.charAt(i + j);
      }
      sink.window(units, size);
    }
  }

  @Override
  public String id(Grams.Contract contract) {
    return Grams.V1_ID;
  }
}
