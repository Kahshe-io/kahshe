package io.kahshe.indexer.build;

import io.kahshe.analysis.analyzer.Analyzer;
import io.kahshe.format.type.gram.Grams;
import org.apache.iceberg.Table;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import io.kahshe.indexer.BuildConfig;

/**
 * Resolution of per-column and per-table index settings from table properties.
 *
 * <p>Order: {@code kahshe.index.<column>.<key>}, then {@code kahshe.index.<key>}, then the
 * deployment default the caller passes. A malformed value WARNs and falls through to the NEXT
 * level rather than silently becoming false or zero: an operator who set a property and got the
 * deployment behaviour must be able to see why. Every per-column knob resolves through this class,
 * so the order cannot fork per knob.
 *
 * <p>Settings that change what the build WRITES are safe to scope per column only because every
 * artifact records what it was built under — the analyzer id carries the token cap, a serialized
 * bloom carries its own geometry, the bloom metadata records fpp — so a reader never interprets
 * an artifact from its own configuration. Keep that property when adding knobs here.
 *
 * <p>Overrides are written by column NAME, as {@code kahshe.index} itself is, and resolved by
 * field ID: {@link #resolveField} follows a name through the table's schema history, so a
 * property written before a rename still applies and a renamed column keeps being maintained.
 */
public final class IndexSettings {
  private static final Logger LOG = LoggerFactory.getLogger(IndexSettings.class);

  private IndexSettings() {}

  /**
   * The field a configured column name refers to, following a RENAME: the current schema first,
   * then every historical schema the table keeps, because the artifacts are keyed by field id
   * while an operator's property keeps the name it was written with. Null when no schema ever
   * had the name — a dropped or misspelled column, which stays the caller's quiet skip.
   */
  public static org.apache.iceberg.types.Types.NestedField resolveField(Table table, String column) {
    String name = column.trim();
    org.apache.iceberg.types.Types.NestedField current = table.schema().findField(name);
    if (current != null) {
      return current;
    }
    for (org.apache.iceberg.Schema old : table.schemas().values()) {
      org.apache.iceberg.types.Types.NestedField was = old.findField(name);
      if (was != null) {
        org.apache.iceberg.types.Types.NestedField now = table.schema().findField(was.fieldId());
        if (now != null) {
          return now;
        }
      }
    }
    return null;
  }

  /** The current full name of a configured column, or the name as given when it does not resolve. */
  static String currentName(Table table, String column) {
    org.apache.iceberg.types.Types.NestedField field = resolveField(table, column);
    return field == null ? column : table.schema().findColumnName(field.fieldId());
  }

  /** Every name a field has had, current first: a property written before a rename still applies. */
  static java.util.List<String> namesOf(Table table, int fieldId) {
    java.util.List<String> names = new java.util.ArrayList<>();
    String now = table.schema().findColumnName(fieldId);
    if (now != null) {
      names.add(now);
    }
    for (org.apache.iceberg.Schema old : table.schemas().values()) {
      String was = old.findColumnName(fieldId);
      if (was != null && !names.contains(was)) {
        names.add(was);
      }
    }
    return names;
  }

  private static java.util.List<String> candidates(Table table, String column, String key) {
    java.util.List<String> out = new java.util.ArrayList<>();
    org.apache.iceberg.types.Types.NestedField field = resolveField(table, column);
    if (field == null) {
      out.add(table.properties().get("kahshe.index." + column.trim() + "." + key));
    } else {
      // by IDENTITY: every name this field has had, current first
      for (String name : namesOf(table, field.fieldId())) {
        out.add(table.properties().get("kahshe.index." + name + "." + key));
      }
    }
    out.add(table.properties().get("kahshe.index." + key));
    return out;
  }

  /**
   * The analyzer a column is built under: {@code kahshe.index[.<column>].analyzer} is
   * {@code tokens} (the default: ASCII tokens, lowercased) or {@code value} (the canonical value
   * whole, exact and case-sensitive — for ids and addresses that tokenize into pieces every row
   * shares), at the column's {@code max-token-length}. Any other spelling WARNs and falls
   * through, like every knob here.
   */
  public static Analyzer.Contract contract(Table table, String column, BuildConfig config) {
    Analyzer.Kind kind = Analyzer.Kind.TOKENS;
    for (String candidate : candidates(table, column, "analyzer")) {
      if (candidate == null || candidate.isBlank()) {
        continue;
      }
      String value = candidate.trim().toLowerCase(java.util.Locale.ROOT);
      if (value.equals("tokens")) {
        break;
      }
      if (value.equals("value")) {
        kind = Analyzer.Kind.VALUE;
        break;
      }
      LOG.warn("analyzer must be tokens or value, got '{}'; ignoring this level", candidate);
    }
    return Analyzer.contract(
        kind, positiveInt(table, column, "max-token-length", config.maxTokenLength()));
  }

  /**
   * The gram contract a build writes: the current rule at the size resolved column, then table,
   * then deployment default ({@code KAHSHE_NGRAM}); a size outside the rule's range is refused at
   * that level with a WARN, as an unparseable one is.
   */
  public static Grams.Contract grams(Table table, String column, BuildConfig config) {
    int size = positiveInt(table, column, "ngram", config.ngram());
    if (size < Grams.MIN_SIZE || size > Grams.MAX_SIZE) {
      LOG.warn("ngram must be {}..{}, got {}; using the deployment default {}",
          Grams.MIN_SIZE, Grams.MAX_SIZE, size, config.ngram());
      size = config.ngram();
    }
    return Grams.Contract.current(size);
  }

  /**
   * A boolean knob, or null when no level sets one. Only the literals {@code true} and
   * {@code false} are accepted — deliberately NOT {@code Boolean.parseBoolean}, which reads every
   * typo as false, and for a tier toggle "false" is a tier silently switched off.
   */
  static Boolean flagOrNull(Table table, String column, String key) {
    for (String candidate : candidates(table, column, key)) {
      if (candidate == null || candidate.isBlank()) {
        continue;
      }
      String value = candidate.trim();
      if (value.equalsIgnoreCase("true")) {
        return Boolean.TRUE;
      }
      if (value.equalsIgnoreCase("false")) {
        return Boolean.FALSE;
      }
      LOG.warn("{} must be true or false, got '{}'; ignoring this level", key, candidate);
    }
    return null;
  }

  /** A positive-integer knob; zero and negatives are refused with a WARN, like malformed text. */
  static int positiveInt(Table table, String column, String key, int deploymentDefault) {
    for (String candidate : candidates(table, column, key)) {
      if (candidate == null || candidate.isBlank()) {
        continue;
      }
      try {
        int value = Integer.parseInt(candidate.trim());
        if (value > 0) {
          return value;
        }
        LOG.warn("{} must be positive, got {}; ignoring this level", key, value);
      } catch (NumberFormatException e) {
        LOG.warn("unparseable {} '{}'; ignoring this level", key, candidate);
      }
    }
    return deploymentDefault;
  }

  /** A probability knob, exclusive on both ends: 0 and 1 are not false-positive rates. */
  static double probability(Table table, String column, String key, double deploymentDefault) {
    for (String candidate : candidates(table, column, key)) {
      if (candidate == null || candidate.isBlank()) {
        continue;
      }
      try {
        double value = Double.parseDouble(candidate.trim());
        if (value > 0.0 && value < 1.0) {
          return value;
        }
        LOG.warn("{} must be between 0 and 1 exclusive, got {}; ignoring this level", key, value);
      } catch (NumberFormatException e) {
        LOG.warn("unparseable {} '{}'; ignoring this level", key, candidate);
      }
    }
    return deploymentDefault;
  }
}
