package io.kahshe.analysis.analyzer;

import java.util.List;

/**
 * The built-in ASCII tokens family: {@code kahshe-ascii-v1}, {@code kahshe-ascii-v2-max<cap>} and
 * {@code kahshe-ascii-v3-max<cap>}.
 *
 * <p>The tokenizing lives in {@link Analyzer}; this class only says which of it an id selects. v3
 * is what a build writes today, and the older ids stay parseable so indexes already written under
 * them are read under their own rules and keep pruning until they are rebuilt.
 */
public final class AsciiAnalyzerFamily implements AnalyzerFamily {
  @Override
  public String name() {
    return "ascii";
  }

  @Override
  public Analyzer.Kind kind() {
    return Analyzer.Kind.TOKENS;
  }

  @Override
  public boolean owns(String id) {
    return Analyzer.V1_ID.equals(id)
        || Analyzer.capOf(id, Analyzer.V3_ID_PREFIX) > 0
        || Analyzer.capOf(id, Analyzer.ID_PREFIX) > 0;
  }

  @Override
  public Analyzer.Contract parse(String id) {
    if (Analyzer.V1_ID.equals(id)) {
      return new Analyzer.Contract(this, Analyzer.Kind.TOKENS, 1, Integer.MAX_VALUE);
    }
    int v3 = Analyzer.capOf(id, Analyzer.V3_ID_PREFIX);
    if (v3 > 0) {
      return new Analyzer.Contract(this, Analyzer.Kind.TOKENS, 3, v3);
    }
    return new Analyzer.Contract(
        this, Analyzer.Kind.TOKENS, 2, Analyzer.capOf(id, Analyzer.ID_PREFIX));
  }

  @Override
  public List<String> tokens(Analyzer.Contract contract, String text) {
    return contract.version() >= 3 ? Analyzer.tokensV3(text) : Analyzer.tokenize(text);
  }

  @Override
  public List<String> queryTerms(Analyzer.Contract contract, String text) {
    return contract.version() < 3 ? tokens(contract, text) : Analyzer.compoundQueryTerms(text);
  }

  @Override
  public boolean isIndexable(Analyzer.Contract contract, String token) {
    if (contract.version() == 1) {
      // v1's rule: any length, but a pure-numeric token over four digits is not admitted.
      return !(token.length() > 4 && Analyzer.allDigits(token));
    }
    return token.length() <= contract.maxTokenLen();
  }

  @Override
  public boolean prefixable(Analyzer.Contract contract, String prefix) {
    for (int i = 0; i < prefix.length(); i++) {
      char c = prefix.charAt(i);
      boolean piece = (c >= 'a' && c <= 'z') || (c >= '0' && c <= '9');
      if (!piece && !(contract.version() >= 3 && Analyzer.COMPOUND_SEPARATORS.indexOf(c) >= 0)) {
        return false;
      }
    }
    return !prefix.isEmpty();
  }

  @Override
  public String id(Analyzer.Contract contract) {
    if (contract.version() == 1) {
      return Analyzer.V1_ID;
    }
    return (contract.version() == 2 ? Analyzer.ID_PREFIX : Analyzer.V3_ID_PREFIX)
        + contract.maxTokenLen();
  }
}
