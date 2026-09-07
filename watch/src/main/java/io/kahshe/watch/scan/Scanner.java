package io.kahshe.watch.scan;

import java.util.Set;

/**
 * A row-level evaluator over new data files: the seam {@link ScanPass} iterates, discovered
 * through {@code META-INF/services/io.kahshe.watch.scan.Scanner} with a public no-argument
 * constructor. The built-ins are declared the same way as anything else.
 *
 * <p>The pass owns the read. It asks every registered scanner which columns it needs, projects
 * the union of those answers, reads each added file once, and hands every row to every open
 * {@link FileScan} — so a second scanner costs no second read of the file, and one that answers
 * "no columns" for a table costs nothing at all.
 *
 * <p>Implementations must be safe to call from several threads: the pass scans files in parallel,
 * so {@link #open} is called concurrently, though each {@link FileScan} it returns is used by one
 * thread only. {@link #configure} is the one lifecycle call.
 */
public interface Scanner {

  /** This scanner's name, stable and unique; it appears in the pass's logs. */
  String name();

  /**
   * The columns of this table this scanner needs read, resolved against the table's schema —
   * empty when it has nothing to do for this table, which skips it entirely.
   *
   * <p>A name the schema does not hold must not be returned: the projection would fail and take
   * every other scanner's evaluation of that file with it.
   */
  Set<String> columns(TableView table);

  /**
   * A per-file evaluation, or null when this scanner has nothing to do for this file. Called once
   * per data file, before any row is read.
   */
  FileScan open(FileScanContext ctx);

  /**
   * The column whose order this scanner needs rows delivered in, or null — the default — when it
   * does not care.
   *
   * <p>Naming a column makes the pass read that table's files ONE AT A TIME and in ascending
   * order of their lower bound on this column. The cost is the parallelism, for every scanner on
   * that table, since the pass reads each file once for all of them — so return null unless the
   * scanner's answer depends on row order, as one holding state ACROSS rows does.
   */
  default String orderRowsBy(TableView table) {
    return null;
  }

  /**
   * How far back this scanner needs a table REPLAYED the first time this process sees it, in
   * milliseconds of wall clock; 0, the default, means prospective only.
   *
   * <p>A scanner holding state ACROSS files cannot start empty — a window rule that restarts
   * mid-window silently never fires it — so the pass re-reads the files added within the span
   * asked for, for that scanner alone. What replaying them cannot rebuild is which windows
   * already alerted, so one that fired shortly before a restart fires again, marked
   * {@code replayed} — a duplicate rather than a miss.
   */
  default long replayMs(TableView table) {
    return 0;
  }

  /**
   * Handed the process's watch state once, before any table is scanned. A scanner that needs
   * nothing from kahshe ignores it; the built-in rule scanner reads its (hot-reloadable) rules
   * and the shared alert helper from it.
   */
  default void configure(ScanContext ctx) {}
}
