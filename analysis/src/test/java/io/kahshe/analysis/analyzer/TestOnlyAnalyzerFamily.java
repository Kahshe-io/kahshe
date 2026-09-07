package io.kahshe.analysis.analyzer;

import java.util.ArrayList;
import java.util.List;

/**
 * A third analyzer family that exists only on the test classpath, registered through
 * {@code analysis/src/test/resources/META-INF/services/io.kahshe.analysis.analyzer.AnalyzerFamily}. It owns
 * {@code kahshe-test-words-v1-max<cap>} and cuts on whitespace WITHOUT lowercasing, which no
 * built-in does — so a test that sees {@code Alpha} come back capitalised has gone through this
 * family and not through the ASCII one.
 *
 * <p>This is the whole point of the seam: adding it required no edit to {@link Analyzer},
 * {@link Analyzers} or an enum.
 */
public final class TestOnlyAnalyzerFamily implements AnalyzerFamily {
  static final String ID_PREFIX = "kahshe-test-words-v1-max";

  @Override
  public String name() {
    return "test-words";
  }

  @Override
  public Analyzer.Kind kind() {
    return Analyzer.Kind.TOKENS;
  }

  @Override
  public boolean owns(String id) {
    return cap(id) > 0;
  }

  @Override
  public Analyzer.Contract parse(String id) {
    return new Analyzer.Contract(this, Analyzer.Kind.TOKENS, 1, cap(id));
  }

  @Override
  public List<String> tokens(Analyzer.Contract contract, String text) {
    List<String> out = new ArrayList<>();
    for (String word : text.split("\\s+")) {
      if (!word.isEmpty()) {
        out.add(word);
      }
    }
    return out;
  }

  @Override
  public List<String> queryTerms(Analyzer.Contract contract, String text) {
    return tokens(contract, text);
  }

  @Override
  public boolean isIndexable(Analyzer.Contract contract, String token) {
    return token.length() <= contract.maxTokenLen();
  }

  @Override
  public boolean prefixable(Analyzer.Contract contract, String prefix) {
    return !prefix.isEmpty();
  }

  @Override
  public String id(Analyzer.Contract contract) {
    return ID_PREFIX + contract.maxTokenLen();
  }

  private static int cap(String id) {
    if (id == null || !id.startsWith(ID_PREFIX)) {
      return -1;
    }
    try {
      int cap = Integer.parseInt(id.substring(ID_PREFIX.length()));
      return cap > 0 ? cap : -1;
    } catch (NumberFormatException e) {
      return -1;
    }
  }
}
