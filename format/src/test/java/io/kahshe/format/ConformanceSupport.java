package io.kahshe.format;

import io.kahshe.common.Metrics;
import io.kahshe.common.Records;
import io.kahshe.indexer.BuildConfig;
import io.kahshe.indexer.LocalTableFixture;
import java.io.IOException;
import java.nio.file.Path;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.TreeMap;
import java.util.TreeSet;
import java.util.stream.Collectors;
import org.apache.iceberg.Table;
import io.kahshe.format.type.term.TermIndex;

/** The table, config and query shapes the conformance tests share. */
final class ConformanceSupport {
  private ConformanceSupport() {}

  /**
   * The fixture's table, written from {@code rows.json} with the committed file names.
   *
   * <p>Two indexed columns: the scalar {@code msg} under the default tokens contract, the
   * {@code list<string>} {@code tags} under the whole-value one ({@link #LIST_COUNTS} says why that
   * contract is the one that bites), and an unindexed {@code props} map to show a container nobody
   * named disturbs nothing.
   */
  static Table table(Path dir) throws IOException {
    List<Map.Entry<String, List<String>>> rows = Conformance.rows();
    List<Map.Entry<String, List<List<String>>>> tags = Conformance.listRows();
    Table table = LocalTableFixture.createCollectionTable(dir);
    for (int i = 0; i < rows.size(); i++) {
      List<String> msgs = rows.get(i).getValue();
      List<List<String>> perRow = tags.get(i).getValue();
      Object[][] records = new Object[msgs.size()][];
      for (int r = 0; r < msgs.size(); r++) {
        records[r] = new Object[] {msgs.get(r), r < perRow.size() ? perRow.get(r) : null, null};
      }
      LocalTableFixture.appendCollectionRows(table, rows.get(i).getKey(), records);
    }
    table.updateProperties()
        .set("kahshe.index." + LIST_COLUMN + ".analyzer", Conformance.property("list-analyzer"))
        .commit();
    return table;
  }

  static final String LIST_COLUMN = LocalTableFixture.LIST_COLUMN;

  static int listFieldId(Table table) {
    return table.schema().findField(LIST_COLUMN).fieldId();
  }

  /** The fixture's build settings, with the index under {@code indexRoot}. */
  static BuildConfig config(String indexRoot) {
    BuildConfig base = LocalTableFixture.config();
    FormatConfig format = Records.with(base.format(), Map.of("indexRoot", indexRoot));
    return Records.with(base, Map.of("format", format));
  }

  static int fieldId(Table table) {
    return table.schema().findField(LocalTableFixture.COLUMN).fieldId();
  }

  /** The files a hint keeps, by name. */
  static Set<String> kept(Table table, FormatConfig format, IndexPruner.HintKind kind, String value)
      throws IOException {
    return kept(table, format, kind, value, LocalTableFixture.COLUMN);
  }

  /** The files a hint on one column keeps, by name. */
  static Set<String> kept(Table table, FormatConfig format, IndexPruner.HintKind kind, String value,
      String column) throws IOException {
    Metrics metrics = new Metrics();
    IndexPruner pruner = new IndexPruner(new TermIndex(format, metrics), metrics, format);
    return pruner.prune(table, null,
            List.of(new IndexPruner.ContainsHint(column,
                table.schema().findField(column).fieldId(), value, kind)),
            LocalTableFixture.planTasks(table))
        .stream().map(t -> Path.of(t.file().location()).getFileName().toString())
        .collect(Collectors.toCollection(TreeSet::new));
  }

  /** A token's entry: files and total occurrences, or null when the dictionary lacks it. */
  static Map<String, Object> count(Table table, FormatConfig format, String token) throws IOException {
    return count(table, format, token, fieldId(table));
  }

  /** A token's entry in one column's dictionary: files and total, or null when it is absent. */
  static Map<String, Object> count(Table table, FormatConfig format, String token, int fieldId)
      throws IOException {
    TermIndex index = new TermIndex(format, new Metrics());
    TermIndex.Loaded loaded = index.forField(table, fieldId);
    TermIndex.Entry entry = index.entriesFor(table, loaded, List.of(token)).get(token);
    if (entry == null) {
      return null;
    }
    Map<String, Object> out = new TreeMap<>();
    out.put("files", entry.fileCount());
    out.put("total", entry.totalCount());
    return out;
  }

  static final List<String[]> QUERIES = List.of(
      new String[] {"MATCH", "hello"}, new String[] {"MATCH", "world"}, new String[] {"MATCH", "fox"},
      new String[] {"MATCH", "192.168.0.1"}, new String[] {"MATCH", "168"}, new String[] {"MATCH", "abc123def"},
      new String[] {"MATCH", "kahshe"}, new String[] {"MATCH", "zzz"},
      new String[] {"CONTAINS", "quick brown"}, new String[] {"CONTAINS", "HELLO"}, new String[] {"CONTAINS", "b😀c"},
      new String[] {"CONTAINS", "😀c"}, new String[] {"CONTAINS", "zzq"}, new String[] {"CONTAINS", "9d2e"},
      new String[] {"PREFIX", "hel"}, new String[] {"PREFIX", "19"}, new String[] {"PREFIX", "zz"});
  static final List<String> COUNTS = List.of("hello", "world", "kahshe", "192.168.0.1", "zzz");

  /**
   * Queries against the list column, each pinning something the scalar column cannot.
   *
   * <p>{@code connectionrefused} and {@code "connection refused"} are the boundary: a build that
   * joined a row's members before analysing them would emit one of those instead of the two terms,
   * and both must keep nothing. {@code alpha} and {@code zulu} span files. Under the whole-value
   * contract a member IS a term, so MATCH is the operative kind.
   */
  static final List<String[]> LIST_QUERIES = List.of(
      new String[] {"MATCH", "alpha"}, new String[] {"MATCH", "bravo"},
      new String[] {"MATCH", "zulu"}, new String[] {"MATCH", "charlie"},
      new String[] {"MATCH", "connection"}, new String[] {"MATCH", "refused"},
      new String[] {"MATCH", "connectionrefused"}, new String[] {"MATCH", "connection refused"},
      new String[] {"MATCH", "zzz"}, new String[] {"PREFIX", "al"}, new String[] {"PREFIX", "zz"});

  /**
   * {@code alpha} is the dedup: it is in three rows across two files -- and one of those rows holds
   * it TWICE -- so a total of 3 means rows and a total of 4 means occurrences.
   */
  static final List<String> LIST_COUNTS =
      List.of("alpha", "bravo", "zulu", "connection", "connection refused", "zzz");
  static final List<String> VECTOR_INPUTS = List.of(
      "Hello WORLD hello", "192.168.0.1 GET /index.html 200", "trace=abc123def", "ab😀cd",
      "550e8400-e29b-41d4-a716-446655440000", "fe80::1", "ok", "");
}
