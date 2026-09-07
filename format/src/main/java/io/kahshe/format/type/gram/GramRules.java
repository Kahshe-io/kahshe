package io.kahshe.format.type.gram;

import java.util.ArrayList;
import java.util.HashSet;
import java.util.List;
import java.util.Set;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * The gram rules this process knows: the two built-ins, then whatever
 * {@link java.util.ServiceLoader} finds under {@code META-INF/services/io.kahshe.format.type.gram.GramRule}.
 *
 * <p>{@link Grams.Contract#of} walks this list in registration order and takes the first rule that
 * {@link GramRule#owns owns} the id. The built-ins are placed first and by identity, so nothing on
 * the classpath can shadow an id kahshe already writes — a provider entry naming one of them is
 * folded into the singleton rather than added twice, which also keeps {@link Grams.Contract}
 * equality (a record over its rule) stable across constructions.
 */
public final class GramRules {
  private static final Logger LOG = LoggerFactory.getLogger(GramRules.class);

  private static final GramRule UTF16 = new Utf16GramRule();
  private static final GramRule CODEPOINT = new CodePointGramRule();
  private static final List<GramRule> RULES = load();

  private GramRules() {}

  /** Every rule, built-ins first, in the order {@link Grams.Contract#of} consults them. */
  public static List<GramRule> rules() {
    return RULES;
  }

  /** The rule that owns {@code id}, or null when none does. */
  public static GramRule owner(String id) {
    if (id == null) {
      return null;
    }
    for (GramRule rule : RULES) {
      if (rule.owns(id)) {
        return rule;
      }
    }
    return null;
  }

  /**
   * The built-in rule for a classification. {@link Grams.Rule} stays as the built-ins' unit
   * classification — it is what the accumulator and the bloom probe switch on to count UTF-16
   * units or code points — so a rule discovered through the service loader declares which of the
   * two counting rules its windows are measured in.
   */
  static GramRule builtin(Grams.Rule rule) {
    return rule == Grams.Rule.UTF16_V1 ? UTF16 : CODEPOINT;
  }

  /** The names of the loaded rules, for the message a refused id carries. */
  static String names() {
    StringBuilder out = new StringBuilder();
    for (GramRule rule : RULES) {
      out.append(out.length() == 0 ? "" : ", ").append(rule.name());
    }
    return out.toString();
  }

  private static List<GramRule> load() {
    List<GramRule> out = new ArrayList<>(List.of(UTF16, CODEPOINT));
    Set<Class<?>> seen = new HashSet<>(List.of(UTF16.getClass(), CODEPOINT.getClass()));
    try {
      for (GramRule rule :
          java.util.ServiceLoader.load(GramRule.class, GramRules.class.getClassLoader())) {
        if (seen.add(rule.getClass())) {
          out.add(rule);
          LOG.info("gram rule {} loaded from {}", rule.name(), rule.getClass().getName());
        }
      }
    } catch (java.util.ServiceConfigurationError e) {
      // A broken provider on the classpath must not take the built-ins with it: an index kahshe
      // wrote stays readable, and the rule that failed simply is not there.
      LOG.warn("a gram rule failed to load; the built-ins are unaffected", e);
    }
    return List.copyOf(out);
  }
}
