package io.kahshe.indexer.maintain;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.node.ArrayNode;
import com.fasterxml.jackson.databind.node.ObjectNode;
import io.kahshe.format.BuildReport;
import io.kahshe.format.IndexPaths;
import io.kahshe.format.type.gram.Grams;
import io.kahshe.indexer.BuildConfig;
import io.kahshe.indexer.build.IndexSettings;
import java.net.URLDecoder;
import java.nio.charset.StandardCharsets;
import java.time.Instant;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import org.apache.iceberg.Snapshot;
import org.apache.iceberg.Table;
import org.apache.iceberg.catalog.TableIdentifier;
import org.apache.iceberg.io.FileIO;

/**
 * One table's index status: what the admin port's {@code GET /index} answers per table and what
 * {@code kahshe status} prints. Assembled in-process from {@link IndexFreshness} and the builds
 * the {@link IndexerService} has done or verified since startup, or from storage by
 * {@link #fromStorage} — the build reports beside the index and the table's own snapshot history,
 * which is all a CLI process with no memory of any observation has to go on.
 *
 * <p>The record's components are the JSON's fields. {@code currentSnapshot} is the snapshot last
 * observed (the catalog's, read from storage); {@code indexedSnapshot} the one the last completed
 * maintenance pass covered, null until one has; {@code behindSeconds} 0 when the two agree.
 */
public record IndexStatus(
    String prefix,
    String namespace,
    String table,
    List<String> declaredColumns,
    long currentSnapshot,
    Long indexedSnapshot,
    long behindSeconds,
    Map<String, Column> columns,
    Failure lastFailure,
    List<Refusal> refused) {

  /** The table property naming the columns to index; the proxy reads it off a loadTable response. */
  public static final String PROPERTY = "kahshe.index";

  private static final ObjectMapper MAPPER = new ObjectMapper();

  /**
   * The last build of one column as its report recorded it, minus the file lists: a report names
   * every file the build added and departed, and holding those per column for every tracked
   * table would cost memory in proportion to the tables' sizes for numbers nobody asked for.
   */
  public record Column(
      String kind,
      long snapshot,
      long filesCovered,
      long filesAdded,
      long filesDeparted,
      long builtAtMs,
      long durationMs,
      String analyzer,
      String grams,
      List<String> warnings) {

    public static Column of(BuildReport report) {
      return new Column(
          report.kind().name(),
          report.snapshotId(),
          report.counters().getOrDefault("files-covered", 0L),
          report.counters().getOrDefault("files-added", (long) report.added().size()),
          report.counters().getOrDefault("files-departed", (long) report.departed().size()),
          report.publishedMs(),
          Math.max(0, report.publishedMs() - report.startedMs()),
          report.analyzer(),
          report.grams(),
          List.copyOf(report.warnings()));
    }
  }

  /** The most recent build failure of a table, kept across later successes so a flapping build shows. */
  public record Failure(String message, long atMs) {}

  /** A column {@code kahshe.index} names that kahshe can never index, and why. */
  public record Refusal(String column, String reason) {}

  /** Where a listing comes from: the indexer in this process, or nothing when none is wired. */
  public interface Source {
    /** Every table this process tracks, in no particular order. */
    List<IndexStatus> tables();

    /** Why {@link #tables()} is empty when it is so by construction, else null. */
    String note();

    Source NONE =
        new Source() {
          @Override
          public List<IndexStatus> tables() {
            return List.of();
          }

          @Override
          public String note() {
            return "no indexer in this process";
          }
        };
  }

  /** The columns the table's {@link #PROPERTY} names, split as the proxy splits them. */
  public static List<String> declared(Table table) {
    String value = table.properties().getOrDefault(PROPERTY, "");
    return Arrays.stream(value.split(",")).map(String::trim).filter(s -> !s.isEmpty()).toList();
  }

  /**
   * The status of one table read from storage rather than memory: the CLI's path. The catalog
   * gives the current snapshot and the declared columns; each column's build report gives its
   * last build; the snapshot history dates the gap. {@link #storageSources} says the same to the
   * reader of the output.
   */
  public static IndexStatus fromStorage(
      Table table, String prefix, String namespace, String tableName, BuildConfig config,
      long nowMs) {
    List<String> declared = declared(table);
    long current = table.currentSnapshot() == null ? -1 : table.currentSnapshot().snapshotId();
    FileIO io = IndexPaths.io(table, config.format());
    String root = IndexPaths.root(table, config.format().indexRoot());
    Map<String, Column> columns = new LinkedHashMap<>();
    List<Refusal> refused = new ArrayList<>();
    // The table is covered up to the OLDEST snapshot its columns' complete builds name, and only
    // when every declared column has one: ids are random, so "oldest" is by the snapshot's place
    // in the history, and a snapshot the table has since expired is older than any it still holds.
    Long indexed = null;
    Snapshot covered = null;
    boolean every = true;
    for (String column : declared) {
      String reason = IndexerService.refusalReason(table, column);
      if (reason != null) {
        refused.add(new Refusal(column, reason));
        continue;
      }
      BuildReport report;
      try {
        report = BuildReport.read(io, root, IndexSettings.resolveField(table, column).fieldId());
      } catch (RuntimeException e) {
        throw new IllegalStateException("the build report for " + column + " under " + root
            + " cannot be read", e);
      }
      if (report == null) {
        every = false;
        continue;
      }
      columns.put(column, Column.of(report));
      if (!covers(table, column, report, config)) {
        every = false;
        continue;
      }
      Snapshot named = table.snapshot(report.snapshotId());
      if (indexed == null || older(named, covered)) {
        indexed = report.snapshotId();
        covered = named;
      }
    }
    if (!every) {
      indexed = null;
    }
    // Nothing indexable is nothing to be behind on: a table declaring no column is never tracked
    // in-process, and one whose every column is refused is claimed like a built one there (no
    // retry changes a configuration error's answer). An operator scripting `behind_seconds > N`
    // over the CLI must not page on either.
    long behind = 0;
    boolean indexable = declared.size() > refused.size();
    if (indexable && current >= 0 && (indexed == null || indexed != current)) {
      behind = behindSeconds(table, indexed == null ? null : covered, nowMs);
    }
    return new IndexStatus(
        prefix, namespace, tableName, declared, current, indexed, behind, columns, null, refused);
  }

  /**
   * Whether a report is a complete build under the rules the column is configured with: the
   * worker's {@code indexCurrent} reads a checkpoint, or an index cut under another analyzer or
   * gram rule, as not current and rebuilds, so the CLI must not call it covered.
   */
  private static boolean covers(Table table, String column, BuildReport report, BuildConfig config) {
    for (String warning : report.warnings()) {
      if (warning.startsWith("partial:")) {
        return false;
      }
    }
    return IndexSettings.contract(table, column, config).id().equals(report.analyzer())
        && IndexSettings.grams(table, column, config).id()
            .equals(Grams.Contract.of(report.grams()).id());
  }

  /** Whether {@code a} comes before {@code b} in the history; an expired snapshot (null) before any retained one. */
  private static boolean older(Snapshot a, Snapshot b) {
    if (b == null) {
      return false;
    }
    if (a == null) {
      return true;
    }
    // sequence numbers order commits, and are all 0 on a v1 table, where the timestamps must do
    return a.sequenceNumber() != b.sequenceNumber()
        ? a.sequenceNumber() < b.sequenceNumber()
        : a.timestampMillis() < b.timestampMillis();
  }

  /**
   * The age of the oldest retained snapshot the index does not cover: the storage reading of
   * "how long has this table been behind". In-process the gap is dated from the first uncovered
   * OBSERVATION, which no CLI process has. At least 1, as the in-process figure is, so a table
   * that is behind never reads 0.
   */
  private static long behindSeconds(Table table, Snapshot covered, long nowMs) {
    long since = Long.MAX_VALUE;
    for (Snapshot snapshot : table.snapshots()) {
      if (covered == null || older(covered, snapshot)) {
        since = Math.min(since, snapshot.timestampMillis());
      }
    }
    if (since == Long.MAX_VALUE) {
      since = table.currentSnapshot().timestampMillis();
    }
    return Math.max(1, (Math.max(0, nowMs - since) + 999) / 1000);
  }

  /**
   * Field by field, where {@link #fromStorage} got it — printed under {@code sources} so a reader
   * of the CLI's output does not mistake a storage-derived age for an observed one, or an absent
   * failure for a clean history.
   */
  public static Map<String, String> storageSources(String backend, String indexRoot) {
    Map<String, String> sources = new LinkedHashMap<>();
    sources.put("prefix, namespace, table", "the command line");
    sources.put("declared_columns", "the table property " + PROPERTY + ", read from " + backend);
    sources.put("current_snapshot", "the table's current snapshot, read from " + backend);
    sources.put("columns", "build-report.json beside each column's index under " + indexRoot);
    sources.put(
        "indexed_snapshot",
        "the oldest snapshot those reports name, when every declared column has a complete build "
            + "under the configured analyzer and gram rule; null otherwise");
    sources.put(
        "behind_seconds",
        "the table's snapshot history: the age of the oldest retained commit the index does not "
            + "cover; 0 when no declared column is indexable. In-process (GET /index) it is the age of the "
            + "first uncovered observation");
    sources.put(
        "last_failure",
        "not known outside the indexer's process: GET /index on its admin port has it");
    sources.put("refused", "the table's schema, by the rule the indexer refuses a column under");
    return sources;
  }

  /**
   * The one table three raw path segments name, or null. Matched by DECODED identity: the
   * tracker's keys keep the client's own encoding of each segment, so {@code %2D} and {@code -}
   * are two keys for one table, and a caller must be able to ask by either.
   *
   * @throws IllegalArgumentException when a segment is not valid percent-encoding
   */
  public static IndexStatus find(
      Source source, String prefixRaw, String namespaceRaw, String tableRaw) {
    String prefix = URLDecoder.decode(prefixRaw, StandardCharsets.UTF_8);
    TableIdentifier ident = IndexerService.decodeIdent(namespaceRaw, tableRaw);
    String namespace = ident.namespace().toString();
    for (IndexStatus status : source.tables()) {
      if (status.prefix().equals(prefix)
          && status.namespace().equals(namespace)
          && status.table().equals(ident.name())) {
        return status;
      }
    }
    return null;
  }

  /** The listing {@code GET /index} answers: every table, and the note when the list is empty by construction. */
  public static ObjectNode listing(Source source) {
    ObjectNode root = MAPPER.createObjectNode();
    ArrayNode tables = root.putArray("tables");
    for (IndexStatus status : source.tables()) {
      tables.add(status.toJson());
    }
    String note = source.note();
    if (note != null) {
      root.put("note", note);
    }
    return root;
  }

  public ObjectNode toJson() {
    return toJson(Map.of());
  }

  /** As {@link #toJson()}, with a {@code sources} object naming where each field came from. */
  public ObjectNode toJson(Map<String, String> sources) {
    ObjectNode root = MAPPER.createObjectNode();
    root.put("prefix", prefix);
    root.put("namespace", namespace);
    root.put("table", table);
    ArrayNode declared = root.putArray("declared_columns");
    declaredColumns.forEach(declared::add);
    root.put("current_snapshot", currentSnapshot);
    if (indexedSnapshot == null) {
      root.putNull("indexed_snapshot");
    } else {
      root.put("indexed_snapshot", indexedSnapshot);
    }
    root.put("behind_seconds", behindSeconds);
    ObjectNode columnsNode = root.putObject("columns");
    columns.forEach(
        (name, column) -> {
          ObjectNode node = columnsNode.putObject(name);
          node.put("kind", column.kind());
          node.put("snapshot", column.snapshot());
          node.put("files_covered", column.filesCovered());
          node.put("files_added", column.filesAdded());
          node.put("files_departed", column.filesDeparted());
          node.put("built_at", iso(column.builtAtMs()));
          node.put("duration_ms", column.durationMs());
          node.put("analyzer", column.analyzer());
          node.put("grams", column.grams());
          ArrayNode warnings = node.putArray("warnings");
          column.warnings().forEach(warnings::add);
        });
    if (lastFailure == null) {
      root.putNull("last_failure");
    } else {
      ObjectNode failure = root.putObject("last_failure");
      failure.put("message", lastFailure.message());
      failure.put("at", iso(lastFailure.atMs()));
    }
    ArrayNode refusals = root.putArray("refused");
    for (Refusal refusal : refused) {
      ObjectNode node = refusals.addObject();
      node.put("column", refusal.column());
      node.put("reason", refusal.reason());
    }
    if (!sources.isEmpty()) {
      ObjectNode node = root.putObject("sources");
      sources.forEach(node::put);
    }
    return root;
  }

  private static String iso(long ms) {
    return Instant.ofEpochMilli(ms).toString();
  }
}
