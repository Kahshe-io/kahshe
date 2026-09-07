package io.kahshe.analysis.analyzer;

import java.util.List;

/**
 * The built-in whole-value family: {@code kahshe-value-v1-max<cap>}. The canonical value is one
 * term, exact and case-sensitive, which is what SQL {@code =} means; the empty value has no term.
 */
public final class ValueAnalyzerFamily implements AnalyzerFamily {
  @Override
  public String name() {
    return "value";
  }

  @Override
  public Analyzer.Kind kind() {
    return Analyzer.Kind.VALUE;
  }

  @Override
  public boolean owns(String id) {
    return Analyzer.capOf(id, Analyzer.VALUE_ID_PREFIX) > 0;
  }

  @Override
  public Analyzer.Contract parse(String id) {
    return new Analyzer.Contract(
        this, Analyzer.Kind.VALUE, 1, Analyzer.capOf(id, Analyzer.VALUE_ID_PREFIX));
  }

  @Override
  public List<String> tokens(Analyzer.Contract contract, String text) {
    return text.isEmpty() ? List.of() : List.of(text);
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
    return Analyzer.VALUE_ID_PREFIX + contract.maxTokenLen();
  }
}
