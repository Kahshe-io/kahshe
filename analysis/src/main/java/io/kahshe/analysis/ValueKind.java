package io.kahshe.analysis;

/**
 * What a column's values ARE, in the only terms analysis needs to know them.
 *
 * <p>Deliberately smaller than any table format's type system, so a format binds to it with one
 * function from its own types instead of being translated whole. {@link Canonical} defines a form
 * for {@code STRING}, {@code INTEGRAL}, {@code DECIMAL}, {@code UUID} and {@code BINARY}, and
 * those five are exactly what is indexable; the others exist because a caller must still be able
 * to tell a column apart where no form is defined for it. {@code INTEGRAL} covers every
 * whole-number width and {@code BINARY} both fixed- and variable-width bytes, since the canonical
 * form does not distinguish them.
 *
 * <p>Everything no form is defined for is {@code OTHER}, which callers read as "not indexable":
 * the build refuses the column, the pruner keeps every file.
 */
public enum ValueKind {
  STRING,
  INTEGRAL,
  DECIMAL,
  FLOATING,
  UUID,
  BINARY,
  BOOLEAN,
  DATE,
  /**
   * A wall clock carrying no zone; {@link #TIMESTAMPTZ} carries an instant. The two stay apart
   * even though neither has a canonical form, because a bound rendered for the wrong one of them
   * is read in the engine's session time zone and silently selects a different range.
   */
  TIMESTAMP,
  TIMESTAMPTZ,
  OTHER;

  /** Whether values of this kind are compared as numbers rather than as text. */
  public boolean numeric() {
    return this == INTEGRAL || this == DECIMAL || this == FLOATING;
  }
}
