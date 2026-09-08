package io.kahshe.watch.scan;

import io.kahshe.format.FormatConfig;
import io.kahshe.format.IndexPaths;
import io.kahshe.common.Metrics;
import java.nio.ByteBuffer;
import java.util.HashMap;
import org.apache.iceberg.types.Conversions;
import org.apache.iceberg.types.Types;
import java.util.LinkedHashMap;
import io.kahshe.analysis.ValueKind;
import io.kahshe.format.IcebergKinds;
import io.kahshe.watch.Alerts;
import java.io.IOException;
import java.util.ArrayList;
import java.util.LinkedHashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;
import java.util.concurrent.atomic.AtomicInteger;
import org.apache.iceberg.DataFile;
import org.apache.iceberg.Schema;
import org.apache.iceberg.Snapshot;
import org.apache.iceberg.Table;
import org.apache.iceberg.catalog.TableIdentifier;
import org.apache.iceberg.data.Record;
import org.apache.iceberg.data.parquet.GenericParquetReaders;
import org.apache.iceberg.io.CloseableIterable;
import org.apache.iceberg.parquet.Parquet;
import org.apache.iceberg.util.SnapshotUtil;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * The one read of new data files, shared by every registered {@link Scanner}.
 *
 * <p>Reads exactly the columns the registered scanners name — indexed or not — out of every data
 * file the table has added since the last snapshot this process scanned, and hands each row to
 * each scanner. This is where a rule spanning columns is answered: {@code WatchEngine} rides the
 * index build and sees one column at a time, so a conjunction there could only say that the file
 * holds a row matching each conjunct, not that one row matches them all.
 *
 * <p>What it costs is that read: the named columns of every new file, projected, on
 * KAHSHE_WATCH_SCAN_THREADS threads. KAHSHE_WATCH_SCAN=false builds no pass at all and leaves the
 * index-riding path alone. Nothing here is retroactive — the first poll of a table starts at its
 * current snapshot and never walks back through history.
 *
 * <p>Suppression is (rule, file) once per process, shared with the engine through {@link Alerts},
 * so a single-column rule on an indexed column that both paths can answer alerts once.
 */
public final class ScanPass {
  private static final Logger LOG = LoggerFactory.getLogger(ScanPass.class);

  /**
   * Optional payload field: what makes this alert distinct from another the same rule raised on
   * the same file. Absent, the file is the claim. Stripped before the payload reaches a sink.
   */
  public static final String CLAIM_KEY = "_claim_key";

  private final List<Scanner> scanners;
  private final Alerts alerts;
  private final FormatConfig format;
  private final ExecutorService pool;
  /** Per table, the snapshot this process has already scanned. Not durable. */
  private final Map<String, Long> scanned = new ConcurrentHashMap<>();
  private final Metrics metrics;
  /**
   * What one table's first-sight replay may spend. A day-long timeframe on a busy table is
   * otherwise an unbounded startup read, and a watcher that takes an hour to become ready is one
   * that is not watching. Stopping early leaves state unrebuilt, which is a MISS, so it is loud
   * rather than quiet.
   */
  private volatile int replayMaxFiles = 2_000;

  /** Discovers the registered scanners, configures them, and sizes the read pool. */
  public ScanPass(ScanContext ctx, FormatConfig format, int threads) {
    this(Scanners.discover(ctx), ctx.alerts(), format, threads, ctx.metrics());
    this.replayMaxFiles = ctx.replayMaxFiles();
  }

  /** As above, over an explicit scanner list — the seam a test drives directly. */
  public ScanPass(List<Scanner> scanners, Alerts alerts, FormatConfig format, int threads) {
    this(scanners, alerts, format, threads, new Metrics());
  }

  /** As above, counting what a replay reads. */
  public ScanPass(
      List<Scanner> scanners, Alerts alerts, FormatConfig format, int threads, Metrics metrics) {
    this.scanners = List.copyOf(scanners);
    this.alerts = alerts;
    this.format = format;
    this.metrics = metrics;
    AtomicInteger seq = new AtomicInteger();
    this.pool =
        Executors.newFixedThreadPool(
            Math.max(1, threads),
            runnable -> {
              Thread thread = new Thread(runnable, "kahshe-watch-scan-" + seq.incrementAndGet());
              thread.setDaemon(true);
              return thread;
            });
    LOG.info("watch scan pass: {} scanner(s) {} on {} thread(s)", this.scanners.size(),
        this.scanners.stream().map(Scanner::name).toList(), Math.max(1, threads));
  }

  /** The registered scanners, built-ins first — what discovery actually found. */
  public List<Scanner> scanners() {
    return scanners;
  }

  /**
   * Scans the files this table added since the last scan and delivers what the scanners raise.
   *
   * <p>Called from discovery for every polled table, whether or not it declares
   * {@code kahshe.index}: what is read is decided by the scanners' columns, not by the index.
   */
  public void scan(String prefix, TableIdentifier ident, Table table) {
    Snapshot current = table.currentSnapshot();
    if (current == null) {
      return;
    }
    String key = prefix + "|" + ident;
    Long last = scanned.get(key);
    if (last != null && last == current.snapshotId()) {
      return;
    }
    // The port: everything a scanner is told about this table, in analysis's vocabulary rather
    // than Iceberg's. Built here because THIS is the Iceberg reader.
    TableView view = viewOf(table);
    Set<String> columns = new LinkedHashSet<>();
    List<Scanner> wanted = new ArrayList<>();
    for (Scanner scanner : scanners) {
      Set<String> asked = scanner.columns(view);
      if (asked == null || asked.isEmpty()) {
        continue;
      }
      wanted.add(scanner);
      columns.addAll(asked);
    }
    // Does any scanner need rows in time order? If so the files are read ONE AT A TIME and in
    // ascending order of their lower bound on that column: concurrency is what interleaves event
    // time between threads, and the manifest's order is not the data's.
    String orderBy = null;
    for (Scanner scanner : wanted) {
      String asked = scanner.orderRowsBy(view);
      if (asked != null && view.has(asked)) {
        orderBy = asked;
        break;
      }
    }
    List<String> paths = columns.isEmpty() ? List.of() : addedSince(table, current, last);
    // First sight of this table: a scanner holding state across files cannot start empty, so it
    // says how far back it needs and the pass reads those older files FOR IT ALONE. The
    // prospective scanners never see them — replaying them through the row scanner would alert on
    // every row of the last window.
    List<Scanner> replayWanted = new ArrayList<>();
    List<String> replayPaths = List.of();
    if (last == null && !columns.isEmpty()) {
      long horizon = 0;
      for (Scanner scanner : wanted) {
        long want = scanner.replayMs(view);
        if (want > 0) {
          replayWanted.add(scanner);
          horizon = Math.max(horizon, want);
        }
      }
      if (horizon > 0) {
        replayPaths = addedWithin(table, current, horizon, paths);
        if (replayPaths.size() > replayMaxFiles) {
          LOG.warn("watch replay for {} wants {} file(s) to cover {} ms but the budget is {}: "
                  + "replaying the {} most recent and NOT rebuilding what the rest held. Windows "
                  + "open before them will not fire. Raise KAHSHE_WATCH_REPLAY_MAX_FILES, shorten "
                  + "the rule's timeframe, or accept the gap deliberately.",
              ident, replayPaths.size(), horizon, replayMaxFiles, replayMaxFiles);
          metrics.watchReplayTruncated.increment();
          replayPaths = replayPaths.subList(replayPaths.size() - replayMaxFiles, replayPaths.size());
        }
        LOG.info("watch replay: {} older file(s) of {} for {}, covering {} ms — rebuilding the "
                + "state a restart lost; windows that already fired will fire again, marked replayed",
            replayPaths.size(), ident, replayWanted.stream().map(Scanner::name).toList(), horizon);
      }
    }
    // Claimed before the read, not after: a scan that throws part way through has already alerted
    // on what it read, and re-reading those files on the next poll would deliver nothing new (the
    // claim is per rule and file) while paying for the read again.
    scanned.put(key, current.snapshotId());
    if (paths.isEmpty() && replayPaths.isEmpty()) {
      return;
    }
    Schema projection = table.schema().select(columns);
    // The Iceberg binding, done once and in the reader where it belongs: a scanner is handed the
    // kinds and never a type system.
    Map<String, ValueKind> kinds = kindsOf(projection);
    java.util.Set<String> repeated = repeatedOf(projection);
    // The same signal the plan path refuses on, read once per pass. The scan reads a data file's
    // raw rows and applies no delete file, so on a merge-on-read snapshot its counts are upper
    // bounds. The proxy REFUSES such a snapshot because a wrong file list is a wrong answer; a
    // detection cannot refuse without going blind, so it alerts and says the evidence is
    // advisory rather than exact.
    boolean deleteBearing = deleteBearing(current);
    if (deleteBearing) {
      metrics.watchScanDeleteBearingFiles.add(paths.size());
      LOG.warn("watch scan of {} is over a snapshot carrying delete files: rows the engine no "
              + "longer returns are still in the data files this reads, so matched_rows is an "
              + "UPPER BOUND and the evidence says advisory. The confirmation SQL applies the "
              + "deletes and will return fewer rows — that is the deletes, not a false positive. "
              + "{} file(s) counted on kahshe_watch_scan_delete_bearing_files_total",
          ident, paths.size());
    }
    LOG.info("watch scan: {} file(s) added by snapshot {} of {}, scanners {}, columns {}",
        paths.size(), current.snapshotId(), ident, wanted.stream().map(Scanner::name).toList(),
        columns);
    List<Future<?>> futures = new ArrayList<>();
    // Unordered path only: replay first, oldest to newest. The ordered path above merges the two
    // lists instead, because commit order is not event order.
    for (String path : orderBy != null ? List.<String>of() : replayPaths) {
      FileScanContext ctx = new FileScanContext(prefix, ident.namespace().toString(), ident.name(),
          current.snapshotId(), path, true, deleteBearing, kinds, repeated);
      try {
        scanFile(ctx, replayWanted, table, projection);
        metrics.watchReplayFilesRead.increment();
      } catch (Throwable e) {
        LOG.warn("watch replay of {} failed; the state it carried is not rebuilt", path, e);
      }
    }
    if (orderBy != null) {
      // The replay files and the current ones are ONE sequence, ordered together. Reading the
      // replayed ones first because they were COMMITTED first would repeat the mistake one level
      // up: a file committed earlier can hold newer events, and then the current file's older
      // burst arrives after the buffer is full of newer times and never fires.
      List<String> all = new ArrayList<>(replayPaths);
      all.addAll(paths);
      Set<String> isReplay = new LinkedHashSet<>(replayPaths);
      all = orderByLowerBound(all, table, orderBy);
      LOG.info("watch scan of {} is ordered by {} and sequential over {} file(s) ({} replayed): a "
              + "scanner counts across rows and concurrent reads would interleave their time",
          ident, orderBy, all.size(), replayPaths.size());
      for (String path : all) {
        boolean replayed = isReplay.contains(path);
        FileScanContext ctx = new FileScanContext(prefix, ident.namespace().toString(),
            ident.name(), current.snapshotId(), path, replayed, deleteBearing, kinds, repeated);
        try {
          scanFile(ctx, replayed ? replayWanted : wanted, table, projection);
          if (replayed) {
            metrics.watchReplayFilesRead.increment();
          }
        } catch (Throwable e) {
          LOG.warn("watch scan of {} failed; no scanner ran on this file", path, e);
        }
      }
      return;
    }
    {
      for (String path : paths) {
        FileScanContext ctx = new FileScanContext(prefix, ident.namespace().toString(),
            ident.name(), current.snapshotId(), path, false, deleteBearing, kinds, repeated);
        futures.add(pool.submit(() -> {
          try {
            scanFile(ctx, wanted, table, projection);
          } catch (Throwable e) {
            // one unreadable file must not lose the alerts of the others
            LOG.warn("watch scan of {} failed; no scanner ran on this file", path, e);
          }
        }));
      }
    }
    for (Future<?> future : futures) {
      try {
        future.get();
      } catch (InterruptedException e) {
        Thread.currentThread().interrupt();
        return;
      } catch (ExecutionException e) {
        LOG.warn("watch scan task failed", e.getCause());
      }
    }
  }

  /** The table as a scanner sees it: top-level columns and their kinds, and a name for logs. */
  private static TableView viewOf(Table table) {
    Map<String, io.kahshe.analysis.ValueKind> columns = new LinkedHashMap<>();
    for (Types.NestedField field : table.schema().columns()) {
      columns.put(field.name(), IcebergKinds.shapeOf(field.type()).kind());
    }
    return new TableView(table.name(), columns);
  }

  /** The projected columns holding many values per row: a list or a map. */
  private static java.util.Set<String> repeatedOf(Schema projection) {
    java.util.Set<String> repeated = new java.util.LinkedHashSet<>();
    for (Types.NestedField field : projection.columns()) {
      if (IcebergKinds.shapeOf(field.type()).repetition() != IcebergKinds.Repetition.SCALAR) {
        repeated.add(field.name());
      }
    }
    return java.util.Set.copyOf(repeated);
  }

  /** What each projected column's values ARE, in analysis's vocabulary rather than Iceberg's. */
  private static Map<String, ValueKind> kindsOf(Schema projection) {
    Map<String, ValueKind> kinds = new LinkedHashMap<>();
    for (Types.NestedField field : projection.columns()) {
      kinds.put(field.name(), IcebergKinds.shapeOf(field.type()).kind());
    }
    return Map.copyOf(kinds);
  }

  /**
   * The data files one snapshot added, read from its manifests. One helper so the three walks
   * that need it read identically; {@code Snapshot.addedDataFiles(FileIO)} is deprecated for
   * removal in Iceberg 2.0 in favour of this builder, which answers the same question.
   */
  private static Iterable<DataFile> addedDataFiles(Table table, Snapshot snapshot) {
    return org.apache.iceberg.SnapshotChanges.builderFor(table).snapshot(snapshot).build()
        .addedDataFiles();
  }

  /**
   * Whether the snapshot carries delete files, read off its summary exactly as the plan path
   * reads it. An absent key is treated as delete-bearing: the summary not proving the absence is
   * not the same as proving it, and the safe reading here is the one that downgrades confidence.
   */
  static boolean deleteBearing(Snapshot snapshot) {
    if (snapshot == null || snapshot.summary() == null) {
      return true;
    }
    return !"0".equals(snapshot.summary().get("total-delete-files"));
  }

  /** One file, read once, fed to every scanner that opened on it. */
  private void scanFile(FileScanContext ctx, List<Scanner> wanted, Table table,
      Schema projection) throws IOException {
    List<Scanner> open = new ArrayList<>();
    List<FileScan> scans = new ArrayList<>();
    for (Scanner scanner : wanted) {
      FileScan scan = scanner.open(ctx);
      if (scan != null) {
        open.add(scanner);
        scans.add(scan);
      }
    }
    if (scans.isEmpty()) {
      return;
    }
    ProjectedRow row = new ProjectedRow(projection);
    int position = 0;
    // As the build does: without the mapping, a file with no field ids is read by column
    // position and every rule then evaluates the wrong column. See DataFileIds.
    org.apache.iceberg.mapping.NameMapping mapping = io.kahshe.format.DataFileIds.mappingOf(table);
    org.apache.iceberg.io.InputFile input =
        IndexPaths.dataIo(table, format).newInputFile(ctx.path());
    // And a file carrying neither ids nor a mapping is refused rather than read positionally.
    io.kahshe.format.DataFileIds.requireResolvable(input, mapping);
    try (CloseableIterable<Record> records =
        io.kahshe.format.DataFileIds.withMapping(Parquet.read(input), mapping)
            .project(projection)
            .createReaderFunc(fs -> GenericParquetReaders.buildReader(projection, fs))
            .build()) {
      for (Record record : records) {
        row.read(record);
        for (FileScan scan : scans) {
          scan.row(position, row);
        }
        position++;
      }
    }
    for (int i = 0; i < scans.size(); i++) {
      deliver(open.get(i), scans.get(i), ctx);
    }
  }

  /** A scanner's verdict on one file: suppressed per (rule, file), then delivered. */
  private void deliver(Scanner scanner, FileScan scan, FileScanContext ctx) {
    List<Map<String, Object>> raised;
    try {
      raised = scan.finish();
    } catch (RuntimeException e) {
      // a poisoned scanner must not take down the others
      LOG.warn("watch scanner {} failed on {}; its alerts are lost for this file", scanner.name(),
          ctx.path(), e);
      return;
    }
    if (raised == null) {
      return;
    }
    for (Map<String, Object> payload : raised) {
      Object rule = payload.get("rule");
      String ruleId = rule instanceof Map<?, ?> map && map.get("id") != null
          ? String.valueOf(map.get("id"))
          : scanner.name();
      // The file is the claim for a scanner that raises at most one alert per file. One that
      // raises several — a window rule tripping on two keys in the same file — says what
      // distinguishes them in CLAIM_KEY, or the second would be suppressed as a duplicate of the
      // first, which is a miss. Removed before delivery: it is a word between the scanner and
      // this pass, not part of the alert.
      Object discriminator = payload.remove(CLAIM_KEY);
      String claim = discriminator instanceof String text ? text : ctx.path();
      if (!alerts.claim(ruleId, claim)) {
        continue;
      }
      alerts.deliver(ruleId, payload);
    }
  }

  /**
   * The files added by snapshots committed within {@code horizonMs} of now, oldest first, minus
   * the ones {@code already} covers.
   *
   * <p>Bounded by wall clock rather than by history length: a table with ten years of snapshots
   * and a five-minute window replays five minutes. Commit time is the bound, not event time —
   * the two differ for late data, and a file's rows are what decides membership of a window
   * anyway, so this only has to be wide enough to CONTAIN the events, not exact about them.
   */
  private static List<String> addedWithin(
      Table table, Snapshot current, long horizonMs, List<String> already) {
    long floor = System.currentTimeMillis() - horizonMs;
    List<Snapshot> walk = new ArrayList<>();
    Snapshot snapshot = current;
    while (snapshot != null && snapshot.timestampMillis() >= floor) {
      walk.add(snapshot);
      Long parent = snapshot.parentId();
      snapshot = parent == null ? null : table.snapshot(parent);
    }
    Set<String> skip = new LinkedHashSet<>(already);
    List<String> paths = new ArrayList<>();
    for (int i = walk.size() - 1; i >= 0; i--) { // oldest first
      for (DataFile file : addedDataFiles(table, walk.get(i))) {
        if (skip.add(file.location())) {
          paths.add(file.location());
        }
      }
    }
    return List.copyOf(paths);
  }

  /**
   * The data files added between {@code last} and the current snapshot, current first.
   *
   * <p>On the first sight of a table this is the current snapshot's own added files and nothing
   * older: rules are prospective, and walking a table's whole history on startup would read every
   * file it has ever written. The same applies when {@code last} is not an ancestor of the current
   * snapshot (a rollback, a branch, an expired snapshot) — there is no forward path to walk.
   */
  private static List<String> addedSince(Table table, Snapshot current, Long last) {
    List<Snapshot> snapshots = new ArrayList<>();
    if (last != null && isAncestor(table, current.snapshotId(), last)) {
      SnapshotUtil.ancestorsBetween(current.snapshotId(), last, table::snapshot)
          .forEach(snapshots::add);
    }
    if (snapshots.isEmpty()) {
      snapshots.add(current);
    }
    Set<String> paths = new LinkedHashSet<>();
    for (Snapshot snapshot : snapshots) {
      for (DataFile file : addedDataFiles(table, snapshot)) {
        paths.add(file.location());
      }
    }
    return List.copyOf(paths);
  }

  /**
   * The files sorted by their lower bound on {@code column} — Iceberg records one per file, so
   * this costs nothing to read. A file whose bound is missing or unreadable sorts LAST rather
   * than first: it is the file whose position we cannot argue for, and putting it after the ones
   * we can keeps the ordered prefix genuinely ordered.
   */
  private List<String> orderByLowerBound(List<String> paths, Table table, String column) {
    Types.NestedField field = table.schema().findField(column);
    if (field == null || paths.size() < 2) {
      return paths;
    }
    Map<String, Comparable<Object>> bounds = new HashMap<>();
    Set<String> wanted = new LinkedHashSet<>(paths);
    // One walk of the manifests for every file the table currently holds: the bound we need is
    // per file, and a file that is being scanned is by definition in some manifest.
    for (Snapshot snapshot : table.snapshots()) {
      if (bounds.size() == wanted.size()) {
        break;
      }
      try {
        for (DataFile file : addedDataFiles(table, snapshot)) {
          String location = file.location();
          if (!wanted.contains(location) || bounds.containsKey(location)) {
            continue;
          }
          Map<Integer, ByteBuffer> lower = file.lowerBounds();
          ByteBuffer buffer = lower == null ? null : lower.get(field.fieldId());
          bounds.put(location, buffer == null ? null : Conversions.fromByteBuffer(field.type(), buffer));
        }
      } catch (RuntimeException e) {
        // an unreadable manifest leaves those files unplaced, which sorts them last
      }
    }
    List<String> ordered = new ArrayList<>(paths);
    ordered.sort((a, b) -> {
      Comparable<Object> x = bounds.get(a);
      Comparable<Object> y = bounds.get(b);
      if (x == null && y == null) {
        return a.compareTo(b);
      }
      if (x == null) {
        return 1;
      }
      if (y == null) {
        return -1;
      }
      int cmp = x.compareTo((Object) y);
      return cmp != 0 ? cmp : a.compareTo(b);
    });
    return ordered;
  }

  private static boolean isAncestor(Table table, long snapshotId, long ancestorId) {
    try {
      return SnapshotUtil.isAncestorOf(snapshotId, ancestorId, table::snapshot);
    } catch (RuntimeException e) {
      // an expired snapshot answers null and the walk throws; first-sight is the safe answer
      return false;
    }
  }
}
