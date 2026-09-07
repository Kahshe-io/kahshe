package io.kahshe.watch.rules;

import io.kahshe.watch.rules.WatchRule.Expr;

/**
 * The row interpreter of a rule's condition tree.
 *
 * <p>{@link WatchRule.Expr} is data — leaves indexing a field list, {@code and}/{@code or}/
 * {@code not} above them — and exactly two things read it: {@link #eval} here, which decides
 * whether ONE ROW satisfies the rule, and {@link ConfirmationSql}, which compiles the same tree
 * into the predicate an operator runs to confirm the alert. They must agree, which is why they
 * sit side by side.
 *
 * <p>The invariant that makes them agree is null. A null column does not satisfy a field here —
 * false, not unknown — so {@code not} of that field HOLDS and the row fires. SQL's three-valued
 * logic disagrees: a predicate over NULL is NULL and {@code NOT NULL} is NULL, which would drop a
 * row this side counted. The SQL side therefore wraps every negated term in
 * {@code COALESCE(…, false)}. A third interpreter would owe the same.
 */
public final class Conditions {
  private Conditions() {}

  /**
   * Whether one row satisfies the condition.
   *
   * @param expr the rule's condition tree
   * @param hits which of the rule's fields this row matched, indexed as the tree's leaves are
   * @return the row's verdict
   */
  public static boolean eval(Expr expr, boolean[] hits) {
    if (expr instanceof Expr.FieldRef ref) {
      return hits[ref.index()];
    }
    if (expr instanceof Expr.Not not) {
      return !eval(not.term(), hits);
    }
    if (expr instanceof Expr.And and) {
      for (Expr term : and.terms()) {
        if (!eval(term, hits)) {
          return false;
        }
      }
      return true;
    }
    for (Expr term : ((Expr.Or) expr).terms()) {
      if (eval(term, hits)) {
        return true;
      }
    }
    return false;
  }
}
