package io.kahshe.analysis.analyzer;

import java.util.ArrayList;
import java.util.HashSet;
import java.util.List;
import java.util.Set;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * The analyzer families this process knows: the two built-ins, then whatever
 * {@link java.util.ServiceLoader} finds under
 * {@code META-INF/services/io.kahshe.analysis.analyzer.AnalyzerFamily}.
 *
 * <p>{@link Analyzer#contractOf} walks this list in registration order and takes the first family
 * that {@link AnalyzerFamily#owns owns} the id. The built-ins are placed first and held by
 * identity, so nothing on the classpath can shadow an id kahshe already writes — a provider entry
 * naming one of them is folded into the singleton rather than added twice, which also keeps
 * {@link Analyzer.Contract} equality (a record over its family) stable across constructions.
 *
 * <p>Loaded once, at class initialisation: a family that appeared or vanished mid-process would
 * mean one id parsing two ways in one JVM.
 */
public final class Analyzers {
  private static final Logger LOG = LoggerFactory.getLogger(Analyzers.class);

  private static final AnalyzerFamily ASCII = new AsciiAnalyzerFamily();
  private static final AnalyzerFamily VALUE = new ValueAnalyzerFamily();
  private static final List<AnalyzerFamily> FAMILIES = load();

  private Analyzers() {}

  /** Every family, built-ins first, in the order {@link Analyzer#contractOf} consults them. */
  public static List<AnalyzerFamily> families() {
    return FAMILIES;
  }

  /** The family that owns {@code id}, or null when none does. */
  public static AnalyzerFamily owner(String id) {
    if (id == null) {
      return null;
    }
    for (AnalyzerFamily family : FAMILIES) {
      if (family.owns(id)) {
        return family;
      }
    }
    return null;
  }

  /** The built-in family for a kind: what {@link Analyzer#contract} builds under. */
  static AnalyzerFamily builtin(Analyzer.Kind kind) {
    return kind == Analyzer.Kind.VALUE ? VALUE : ASCII;
  }

  /** The names of the loaded families, for the message a refused id carries. */
  public static String names() {
    StringBuilder out = new StringBuilder();
    for (AnalyzerFamily family : FAMILIES) {
      out.append(out.length() == 0 ? "" : ", ").append(family.name());
    }
    return out.toString();
  }

  private static List<AnalyzerFamily> load() {
    List<AnalyzerFamily> out = new ArrayList<>(List.of(ASCII, VALUE));
    Set<Class<?>> seen = new HashSet<>(List.of(ASCII.getClass(), VALUE.getClass()));
    try {
      for (AnalyzerFamily family :
          java.util.ServiceLoader.load(AnalyzerFamily.class, Analyzers.class.getClassLoader())) {
        if (seen.add(family.getClass())) {
          out.add(family);
          LOG.info("analyzer family {} loaded from {}", family.name(), family.getClass().getName());
        }
      }
    } catch (java.util.ServiceConfigurationError e) {
      // A broken provider must not take the built-ins with it: an index kahshe wrote stays
      // readable, and the family that failed simply is not there.
      LOG.warn("an analyzer family failed to load; the built-ins are unaffected", e);
    }
    return List.copyOf(out);
  }
}
