package io.kahshe.watch.scan;

import io.kahshe.analysis.Canonical;
import java.util.List;
import io.kahshe.analysis.ValueKind;

/**
 * One row's projected columns, in the four forms a scanner asks for.
 *
 * <p>Every form but {@link #value} is derived lazily and memoized for the row, so two scanners
 * asking the same column for its lowercase text tokenize and lowercase it once between them.
 * Every method answers null (or an empty token list) for a null value, and throws
 * {@link IllegalArgumentException} for a column outside the projection — a scanner reading a
 * column it did not declare is a bug, not a missing value.
 */
public interface Row {

  /** The value as the Parquet reader produced it: a String, a Long, a ByteBuffer, null. */
  Object value(String column);

  /**
   * What the column's values ARE, in the terms analysis needs — not the table format's own type.
   * A scanner that reads this rather than an Iceberg {@code Type} is one a second table format
   * can serve unchanged.
   */
  ValueKind kind(String column);

  /**
   * The value's canonical string form — the same one the index writes, so a rule literal and an
   * indexed term agree. A type {@code Canonical} does not cover falls back to the value's own
   * text rather than to null: a rule on a boolean or a timestamp column matching nothing at all
   * would be a silent failure.
   */
  String canonical(String column);

  /** {@link #canonical} lowercased, for the case-insensitive operators. */
  String lowered(String column);

  /** {@link #canonical} under the default tokens contract — what {@code match} is tested against. */
  List<String> tokens(String column);

  /**
   * How many values this column holds for THIS row: one for a scalar, one per member for a list or
   * a map, and zero for a null value or an empty container.
   *
   * <p>A container's members are separate values, never one joined string. Joining them would
   * invent text that appears in no value — {@code contains 'alpha, bravo'} matching a row whose
   * tags are {@code ["alpha","bravo"]} is a false alert produced by the rendering, not by the
   * data — so an operator is applied to each member and the field is satisfied if ANY member
   * satisfies it. That is the same existential the index answers (FORMAT.md §6.7), which is what
   * keeps the two paths from disagreeing about what a rule means.
   *
   * <p>The quantifier is per FIELD, not per rule; rules whose truth would depend on that are
   * refused ({@link PreparedRule}).
   *
   * <p>Defaulted for the scalar reading, so an implementation written before containers existed —
   * a test double, a second table format's adapter — keeps working and answers as what it is.
   */
  default int arity(String column) {
    return value(column) == null ? 0 : 1;
  }

  /** {@link #canonical(String)} for one member; member 0 of a scalar is the scalar. */
  default String canonical(String column, int member) {
    return canonical(column);
  }

  /** {@link #lowered(String)} for one member. */
  default String lowered(String column, int member) {
    return lowered(column);
  }

  /** {@link #tokens(String)} for one member. */
  default List<String> tokens(String column, int member) {
    return tokens(column);
  }
}
