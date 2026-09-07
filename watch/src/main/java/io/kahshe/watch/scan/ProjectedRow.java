package io.kahshe.watch.scan;

import io.kahshe.analysis.Canonical;
import io.kahshe.analysis.analyzer.Analyzer;
import java.util.ArrayList;
import java.util.Collection;
import java.util.HashMap;
import java.util.List;
import java.util.Locale;
import java.util.Map;
import org.apache.iceberg.Schema;
import org.apache.iceberg.data.Record;
import io.kahshe.analysis.ValueKind;
import io.kahshe.format.IcebergKinds;
import org.apache.iceberg.types.Types;

/**
 * The {@link Row} the pass reuses across the rows of one file.
 *
 * <p>Reused rather than allocated per row because a scan of a wide file would otherwise allocate
 * a map, a lowercase copy and a token list per row per scanner. Derived forms are computed on
 * first ask and cleared on the next row, so two scanners asking the same column for its tokens
 * tokenize once between them and a column nobody asks about is never lowercased at all.
 *
 * <p>Not thread-safe: one instance belongs to one file's read, on one thread.
 */
final class ProjectedRow implements Row {
  /**
   * The tokens contract the rule loader validated {@code match} entries against. The default
   * rather than a column's configured analyzer: this path reads no index, so binding it to one
   * column's contract would make the same rule mean different things on an indexed and an
   * unindexed table.
   */
  private static final Analyzer.Contract TOKENS =
      Analyzer.contract(Analyzer.Kind.TOKENS, Analyzer.DEFAULT_MAX_TOKEN_LEN);

  private final List<String> columns = new ArrayList<>();
  private final Map<String, Integer> slots = new HashMap<>();
  private final List<ValueKind> kinds = new ArrayList<>();
  /** Per slot: whether the column is a list or a map, decided once from the projection. */
  private final List<Boolean> repeated = new ArrayList<>();
  private final Object[] values;
  private final String[] canonical;
  private final String[] lowered;
  private final List<List<String>> tokenized = new ArrayList<>();
  /**
   * Per slot, for container columns only: this row's members, COPIED.
   *
   * <p>The copy is not defensive tidiness. Iceberg's Parquet reader hands back one collection
   * instance per column and clears it for the next row, so anything held across rows — a memoized
   * form, a scanner that kept the value — would read the following row's members. The rest of this
   * class already memoizes per row, so the copy is what makes that safe.
   */
  private final List<List<Object>> members = new ArrayList<>();
  private final List<String[]> memberCanonical = new ArrayList<>();
  private final List<String[]> memberLowered = new ArrayList<>();
  private final List<List<String>[]> memberTokens = new ArrayList<>();

  ProjectedRow(Schema projection) {
    for (Types.NestedField field : projection.columns()) {
      slots.put(field.name(), columns.size());
      columns.add(field.name());
      // The one place Iceberg's types meet this path: ProjectedRow IS the Iceberg row adapter,
      // so the binding belongs here and the scanners above it never see a Type. shapeOf, not of:
      // a container's kind is the kind of its MEMBERS, which is what every operator here compares.
      IcebergKinds.Shape shape = IcebergKinds.shapeOf(field.type());
      kinds.add(shape.kind());
      repeated.add(shape.repetition() != IcebergKinds.Repetition.SCALAR);
      tokenized.add(null);
      members.add(null);
      memberCanonical.add(null);
      memberLowered.add(null);
      memberTokens.add(null);
    }
    this.values = new Object[columns.size()];
    this.canonical = new String[columns.size()];
    this.lowered = new String[columns.size()];
  }

  /**
   * Binds this row to one record and drops the previous row's derived forms.
   *
   * <p>Read BY POSITION, not through {@code Schema.accessorForField}. Those accessors are built
   * for Iceberg's INTERNAL representation — a {@code timestamp} is a {@code Long} of microseconds
   * there — while the pass reads through the generic data model, where the same column is a
   * {@code LocalDateTime}. The accessor's checked cast then throws for every row of every file,
   * so a rule naming a time column could not fire at all. The record's field order IS the
   * projection's, so the position is the field.
   */
  void read(Record record) {
    for (int i = 0; i < columns.size(); i++) {
      values[i] = record.get(i);
      canonical[i] = null;
      lowered[i] = null;
      tokenized.set(i, null);
      if (repeated.get(i)) {
        members.set(i, membersOf(values[i]));
        memberCanonical.set(i, null);
        memberLowered.set(i, null);
        memberTokens.set(i, null);
      }
    }
  }

  /**
   * This row's members of a container, copied out of the reader's own collection before it is
   * cleared for the next row. A map contributes its VALUES, the same half the index writes, so a
   * rule and a term mean the same thing.
   */
  private static List<Object> membersOf(Object value) {
    if (value == null) {
      return List.of();
    }
    Collection<?> source =
        value instanceof Map<?, ?> map ? map.values() : (Collection<?>) value;
    return new ArrayList<>(source);
  }

  private int slot(String column) {
    Integer slot = slots.get(column);
    if (slot == null) {
      throw new IllegalArgumentException(
          "column " + column + " is not in the scan projection " + columns);
    }
    return slot;
  }

  @Override
  public Object value(String column) {
    int slot = slot(column);
    // the copy, never the reader's collection: see members
    return repeated.get(slot) ? members.get(slot) : values[slot];
  }

  @Override
  public int arity(String column) {
    int slot = slot(column);
    if (repeated.get(slot)) {
      return members.get(slot).size();
    }
    return values[slot] == null ? 0 : 1;
  }

  @Override
  public String canonical(String column, int member) {
    int slot = slot(column);
    if (!repeated.get(slot)) {
      return canonical(column);
    }
    String[] cache = memberCanonical.get(slot);
    if (cache == null) {
      cache = new String[members.get(slot).size()];
      memberCanonical.set(slot, cache);
    }
    if (cache[member] == null) {
      Object raw = members.get(slot).get(member);
      if (raw != null) {
        String text = Canonical.form(kinds.get(slot), raw);
        cache[member] = text != null ? text : String.valueOf(raw);
      }
    }
    return cache[member];
  }

  @Override
  public String lowered(String column, int member) {
    int slot = slot(column);
    if (!repeated.get(slot)) {
      return lowered(column);
    }
    String[] cache = memberLowered.get(slot);
    if (cache == null) {
      cache = new String[members.get(slot).size()];
      memberLowered.set(slot, cache);
    }
    if (cache[member] == null) {
      String text = canonical(column, member);
      cache[member] = text == null ? null : text.toLowerCase(Locale.ROOT);
    }
    return cache[member];
  }

  @Override
  @SuppressWarnings("unchecked")
  public List<String> tokens(String column, int member) {
    int slot = slot(column);
    if (!repeated.get(slot)) {
      return tokens(column);
    }
    List<String>[] cache = memberTokens.get(slot);
    if (cache == null) {
      cache = new List[members.get(slot).size()];
      memberTokens.set(slot, cache);
    }
    if (cache[member] == null) {
      String text = canonical(column, member);
      cache[member] = text == null ? List.of() : TOKENS.tokens(text);
    }
    return cache[member];
  }

  @Override
  public ValueKind kind(String column) {
    return kinds.get(slot(column));
  }

  @Override
  public String canonical(String column) {
    int slot = slot(column);
    if (canonical[slot] == null && values[slot] != null) {
      String text = Canonical.form(kinds.get(slot), values[slot]);
      canonical[slot] = text != null ? text : String.valueOf(values[slot]);
    }
    return canonical[slot];
  }

  @Override
  public String lowered(String column) {
    int slot = slot(column);
    if (lowered[slot] == null) {
      String text = canonical(column);
      lowered[slot] = text == null ? null : text.toLowerCase(Locale.ROOT);
    }
    return lowered[slot];
  }

  @Override
  public List<String> tokens(String column) {
    int slot = slot(column);
    if (tokenized.get(slot) == null) {
      String text = canonical(column);
      tokenized.set(slot, text == null ? List.of() : TOKENS.tokens(text));
    }
    return tokenized.get(slot);
  }
}
