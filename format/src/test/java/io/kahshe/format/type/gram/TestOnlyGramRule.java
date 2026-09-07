package io.kahshe.format.type.gram;

import java.util.function.Consumer;

/**
 * A third gram rule that exists only on the test classpath, registered through
 * {@code format/src/test/resources/META-INF/services/io.kahshe.format.type.gram.GramRule}. It owns
 * {@code kahshe-test-tumbling-v1-n<size>} and cuts TUMBLING windows — one window per {@code size}
 * code points, no overlap — which neither built-in does, so a test that sees two grams where the
 * sliding cut gives four has gone through this rule.
 *
 * <p>It classifies itself {@link Grams.Rule#CODEPOINT_V2}, which is what the enum is for now: how
 * a window's units are counted, not which rules exist.
 */
public final class TestOnlyGramRule implements GramRule {
  static final String ID_PREFIX = "kahshe-test-tumbling-v1-n";

  @Override
  public String name() {
    return "test-tumbling";
  }

  @Override
  public boolean owns(String id) {
    return size(id) > 0;
  }

  @Override
  public Grams.Contract parse(String id) {
    return new Grams.Contract(this, Grams.Rule.CODEPOINT_V2, size(id));
  }

  @Override
  public void forEachWindow(Grams.Contract contract, String v, Consumer<String> window) {
    int size = contract.size();
    int count = v.codePointCount(0, v.length());
    int start = 0;
    for (int w = 0; (w + 1) * size <= count; w++) {
      int end = v.offsetByCodePoints(start, size);
      window.accept(v.substring(start, end));
      start = end;
    }
  }

  @Override
  public String id(Grams.Contract contract) {
    return ID_PREFIX + contract.size();
  }

  private static int size(String id) {
    if (id == null || !id.startsWith(ID_PREFIX)) {
      return -1;
    }
    try {
      int size = Integer.parseInt(id.substring(ID_PREFIX.length()));
      return size > 0 ? size : -1;
    } catch (NumberFormatException e) {
      return -1;
    }
  }
}
