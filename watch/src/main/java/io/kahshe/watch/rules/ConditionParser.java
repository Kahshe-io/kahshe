package io.kahshe.watch.rules;

import io.kahshe.watch.rules.WatchRule.Expr;
import java.util.ArrayList;
import java.util.LinkedHashSet;
import java.util.List;
import java.util.Locale;
import java.util.Map;
import java.util.Set;
import java.util.regex.Pattern;

/**
 * The {@code detection} form's condition, Sigma's grammar over named selections:
 *
 * <pre>
 *   expr    := or
 *   or      := and ( 'or' and )*
 *   and     := unary ( 'and' unary )*
 *   unary   := 'not' unary | primary
 *   primary := '(' expr ')' | ( '1' | 'all' ) 'of' ( 'them' | pattern ) | name
 * </pre>
 *
 * A name is a selection; a pattern is a name with {@code *} wildcards, and {@code them} is every
 * selection. Keywords are case-insensitive. A selection referenced nowhere is an error, not a
 * warning: it is a typo in the condition until proven otherwise. Errors carry the message the
 * rule is skipped with.
 */
final class ConditionParser {
  private final List<String> tokens;
  private final Map<String, Expr> selections;
  private final Set<String> referenced = new LinkedHashSet<>();
  private int pos;

  private ConditionParser(String text, Map<String, Expr> selections) {
    this.tokens = tokenize(text);
    this.selections = selections;
  }

  /** Parses {@code text}; throws {@link IllegalArgumentException} with the reason on any fault. */
  static Expr parse(String text, Map<String, Expr> selections) {
    ConditionParser parser = new ConditionParser(text, selections);
    if (parser.tokens.isEmpty()) {
      throw new IllegalArgumentException("condition is empty");
    }
    Expr expr = parser.or();
    if (parser.pos < parser.tokens.size()) {
      throw new IllegalArgumentException(
          "condition has trailing input at '" + parser.tokens.get(parser.pos) + "'");
    }
    for (String name : selections.keySet()) {
      if (!parser.referenced.contains(name)) {
        throw new IllegalArgumentException("selection '" + name + "' is not used by the condition");
      }
    }
    return expr;
  }

  private static List<String> tokenize(String text) {
    List<String> out = new ArrayList<>();
    StringBuilder word = new StringBuilder();
    for (char c : text.toCharArray()) {
      if (c == '(' || c == ')') {
        flush(word, out);
        out.add(String.valueOf(c));
      } else if (Character.isWhitespace(c)) {
        flush(word, out);
      } else {
        word.append(c);
      }
    }
    flush(word, out);
    return out;
  }

  private static void flush(StringBuilder word, List<String> out) {
    if (word.length() > 0) {
      out.add(word.toString());
      word.setLength(0);
    }
  }

  private Expr or() {
    List<Expr> terms = new ArrayList<>();
    terms.add(and());
    while (keyword("or")) {
      pos++;
      terms.add(and());
    }
    return terms.size() == 1 ? terms.get(0) : new Expr.Or(terms);
  }

  private Expr and() {
    List<Expr> terms = new ArrayList<>();
    terms.add(unary());
    while (keyword("and")) {
      pos++;
      terms.add(unary());
    }
    return terms.size() == 1 ? terms.get(0) : new Expr.And(terms);
  }

  private Expr unary() {
    if (keyword("not")) {
      pos++;
      return new Expr.Not(unary());
    }
    return primary();
  }

  private Expr primary() {
    String token = next("a selection, 'not', '(' or a quantifier");
    if (token.equals("(")) {
      Expr inner = or();
      String close = next("')'");
      if (!close.equals(")")) {
        throw new IllegalArgumentException("condition expected ')' but found '" + close + "'");
      }
      return inner;
    }
    if (token.equals(")")) {
      throw new IllegalArgumentException("condition has an unmatched ')'");
    }
    String lowered = token.toLowerCase(Locale.ROOT);
    if (lowered.equals("1") || lowered.equals("all")) {
      String of = next("'of'");
      if (!of.equalsIgnoreCase("of")) {
        throw new IllegalArgumentException("condition expected 'of' after '" + token + "'");
      }
      String target = next("'them' or a selection pattern");
      List<Expr> matched = select(target);
      return lowered.equals("1") ? new Expr.Or(matched) : new Expr.And(matched);
    }
    if (isReserved(lowered)) {
      throw new IllegalArgumentException("condition has '" + token + "' where a selection was expected");
    }
    if (lowered.matches("\\d+")) {
      throw new IllegalArgumentException(
          "condition quantifier '" + token + " of' is not supported: use '1 of' or 'all of'");
    }
    Expr selection = selections.get(token);
    if (selection == null) {
      throw new IllegalArgumentException("condition names an unknown selection '" + token + "'");
    }
    referenced.add(token);
    return selection;
  }

  /** {@code them}, or every selection whose name matches the pattern; at least one must. */
  private List<Expr> select(String target) {
    List<Expr> matched = new ArrayList<>();
    if (target.equalsIgnoreCase("them")) {
      for (Map.Entry<String, Expr> entry : selections.entrySet()) {
        referenced.add(entry.getKey());
        matched.add(entry.getValue());
      }
    } else {
      Pattern pattern = Pattern.compile(
          "\\Q" + target.replace("*", "\\E.*\\Q") + "\\E");
      for (Map.Entry<String, Expr> entry : selections.entrySet()) {
        if (pattern.matcher(entry.getKey()).matches()) {
          referenced.add(entry.getKey());
          matched.add(entry.getValue());
        }
      }
    }
    if (matched.isEmpty()) {
      throw new IllegalArgumentException("condition's '" + target + "' matches no selection");
    }
    return matched;
  }

  private static boolean isReserved(String lowered) {
    return switch (lowered) {
      case "and", "or", "not", "of", "them" -> true;
      default -> false;
    };
  }

  private boolean keyword(String word) {
    return pos < tokens.size() && tokens.get(pos).equalsIgnoreCase(word);
  }

  private String next(String expected) {
    if (pos >= tokens.size()) {
      throw new IllegalArgumentException("condition ended where " + expected + " was expected");
    }
    return tokens.get(pos++);
  }
}
