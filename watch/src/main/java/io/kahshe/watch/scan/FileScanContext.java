package io.kahshe.watch.scan;

import io.kahshe.analysis.ValueKind;
import java.util.Map;

/**
 * What a scanner is told about the data file it is about to see: enough to identify the rows to
 * an operator (the catalog prefix, the table, the snapshot, the file) and enough to resolve its
 * own columns against the projection the pass actually read.
 *
 * <p>It names NO table format, and that is the point rather than an accident: this record is the
 * scan seam's argument, so anything on it is reachable by every scanner anyone writes. An Iceberg
 * {@code Table} or {@code Schema} here would make the seam Iceberg's, and every third-party
 * scanner unportable along with it.
 *
 * @param prefix the catalog prefix the table was loaded under
 * @param namespace the table's namespace, as the confirmation SQL spells it
 * @param tableName the table's name within that namespace
 * @param snapshotId the snapshot that added this file — what the alert pins to
 * @param path the data file's location
 * @param replay whether this file is being re-read to rebuild state a restart lost, rather than
 *     seen for the first time — an alert raised from it is a repeat of one that may already have
 *     been delivered, and says so
 * @param deleteBearing whether the snapshot carries delete files. The scan reads raw rows and
 *     applies none, so on such a snapshot a count is an upper bound: the rows are really in the
 *     file, but the engine no longer returns all of them. Evidence from such a file must not
 *     claim to be exact
 * @param kinds what each projected column's values ARE, in analysis's own vocabulary; supplied by
 *     the reader so a scanner never has to read a table format's type system
 * @param repeated the projected columns that hold MANY values per row — a list or a map. Named
 *     rather than typed, so this stays analysis's vocabulary: a scanner needs to know that an
 *     operator on such a column is tested per member and satisfied by any of them, because two
 *     conjoined fields on one of them then ask a different question than an engine would
 */
public record FileScanContext(
    String prefix,
    String namespace,
    String tableName,
    long snapshotId,
    String path,
    boolean replay,
    boolean deleteBearing,
    Map<String, ValueKind> kinds,
    java.util.Set<String> repeated) {

  // No convenience constructors, on purpose: one that defaulted the repeated set to empty let
  // every rule on a window-rule table bypass the cross-member refusal. A seam fact a constructor
  // can leave out WILL be left out.

  /** The confidence evidence from this file may honestly claim. */
  public String confidence() {
    return deleteBearing ? "advisory" : "exact";
  }

  public FileScanContext {
    kinds = Map.copyOf(kinds);
    repeated = java.util.Set.copyOf(repeated);
  }
}
