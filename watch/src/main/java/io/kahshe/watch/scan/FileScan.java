package io.kahshe.watch.scan;

import java.util.List;
import java.util.Map;

/**
 * One scanner's evaluation of one data file: every row, then a verdict.
 *
 * <p>Used by a single thread, which is why it may hold plain mutable tallies. {@link #finish} is
 * called exactly once, after the last row, even when the file held none.
 */
public interface FileScan {

  /**
   * One row of the file, at its position within it. The {@link Row} is reused across rows — read
   * what is needed and keep no reference to it.
   */
  void row(int rowPosition, Row row);

  /**
   * The alerts this file raised, in the payload shape every sink and every build report carries
   * (build one through {@code Alerts.payload}), or an empty list. The pass applies the per-rule,
   * per-file suppression and delivers them; an implementation never delivers its own.
   */
  List<Map<String, Object>> finish();
}
