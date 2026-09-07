package io.kahshe.proxy.plan;

import io.kahshe.format.IndexPruner;
import io.kahshe.format.type.term.TermIndex;
import io.kahshe.common.BoundedCache;
import io.kahshe.common.WeighedCache;
import java.io.IOException;
import java.io.UncheckedIOException;
import java.nio.charset.StandardCharsets;
import java.security.MessageDigest;
import java.security.NoSuchAlgorithmException;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.HexFormat;
import java.util.List;
import java.util.Map;
import java.util.UUID;
import org.apache.iceberg.FileScanTask;
import org.apache.iceberg.Table;
import org.apache.iceberg.catalog.Catalog;
import org.apache.iceberg.catalog.TableIdentifier;
import org.apache.iceberg.expressions.InclusiveMetricsEvaluator;
import org.apache.iceberg.io.CloseableIterable;
import org.apache.iceberg.rest.PlanStatus;
import org.apache.iceberg.rest.requests.PlanTableScanRequest;
import org.apache.iceberg.rest.responses.PlanTableScanResponse;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import io.kahshe.proxy.ProxyConfig;
import io.kahshe.proxy.catalog.Mutations;

/**
 * kahshe's plan implementation.
 *
 * <p>Manifest planning runs once per (table, current snapshot); each request evaluates its filter
 * against the cached files' stats and the indexes. Two parallel task lists are cached: a
 * stats-laden one for evaluation and a stats-stripped one for responses, so per-file column
 * min/max never leaves the proxy unless a request asks for it and the table allows it.
 * Time-travel and incremental requests plan directly and bypass the cache.
 */
public final class PlanService {

  /**
   * Thrown when the requested snapshot carries delete files, which server-side planning cannot
   * describe correctly: the REST scan-task wire format loses {@code DeleteFile.dataSequenceNumber},
   * and a merge-on-read reader needs it to decide which deletes apply to which rows.
   *
   * <p>Surfaces as 422. A client that loaded the table while it was delete-free and plans after a
   * delete lands fails outright rather than falling back to local planning, because a plan
   * response has no way to say "plan this one yourself".
   */
  public static final class DeleteBearingSnapshotException extends RuntimeException {
    DeleteBearingSnapshotException(String message) {
      super(message);
    }
  }
  private static final Logger LOG = LoggerFactory.getLogger(PlanService.class);
  private static final int MAX_CACHED_TABLES = 64;

  private record SnapshotPlan(
      long snapshotId,
      List<FileScanTask> statsTasks,
      Map<String, FileScanTask> responseByPath,
      long weightBytes) {}

  private final IndexPruner pruner;
  /**
   * Prefix on the plan id of a plan answered inline, read by {@code PlanRoutes}.
   *
   * <p>A response carrying {@code COMPLETED} and all its tasks registers nothing server-side, so
   * the id names no plan a later request can fetch or cancel. {@code PlanRoutes} keys the no-op
   * cancel on this, so the two ends must agree — hence a shared constant.
   */
  static final String INLINE_PLAN_ID_PREFIX = "sync-";

  private final WeighedCache<String, SnapshotPlan> planCache;
  private final BoundedCache<String, Object> planLocks = new BoundedCache<>(MAX_CACHED_TABLES * 2);
  private final io.kahshe.common.Metrics metrics;
  private final boolean serveDeleteBearing;
  /** The deployment's plan-stats mode; a table's {@code kahshe.plan-stats} overrides it. */
  private final String planStatsDefault;

  public PlanService(
      io.kahshe.common.Metrics metrics,
      TermIndex termIndex,
      ProxyConfig config,
      io.kahshe.format.FormatConfig format) {
    this.metrics = metrics;
    this.serveDeleteBearing = config.serveDeleteBearing();
    this.planStatsDefault = config.planStats();
    this.pruner = new IndexPruner(termIndex, metrics, format);
    this.planCache =
        new WeighedCache<>(
            format.planCacheBytes(), SnapshotPlan::weightBytes, metrics.planCacheEvictions);
    metrics.planCacheWeightBytes = planCache::estimatedWeightBytes;
  }

  /** As below, from a caller with no bearer: the audit line says {@code caller=none}. */
  public PlanTableScanResponse plan(
      Catalog catalog,
      TableIdentifier ident,
      PlanTableScanRequest request,
      List<IndexPruner.ContainsHint> hints) {
    return plan(catalog, ident, request, hints, null);
  }

  /**
   * Plans one scan, always {@code COMPLETED} with every task in the response. A table with no
   * current snapshot, asked for without pinning one, gets an empty plan: an unwritten table has
   * nothing to scan, and that is not a failure.
   *
   * <p>Every plan served leaves one INFO line and moves the table's counters; {@code callerToken}
   * is the request's Authorization header, which appears in that line only as a hash.
   *
   * @throws DeleteBearingSnapshotException for a snapshot not provably delete-free, unless
   *     {@code KAHSHE_SERVE_DELETE_BEARING} is set
   */
  public PlanTableScanResponse plan(
      Catalog catalog,
      TableIdentifier ident,
      PlanTableScanRequest request,
      List<IndexPruner.ContainsHint> hints,
      String callerToken) {
    long start = System.nanoTime();
    Table table = catalog.loadTable(ident);

    if (table.currentSnapshot() == null && request.snapshotId() == null) {
      // freshly created table: an empty plan, not an NPE
      return served(table, ident, "none", callerToken, 0, List.of(), start);
    }

    long currentSnapshotId =
        table.currentSnapshot() == null ? -1 : table.currentSnapshot().snapshotId();
    long targetSnapshotId =
        request.snapshotId() != null
            ? request.snapshotId()
            : request.endSnapshotId() != null ? request.endSnapshotId() : currentSnapshotId;

    if (!serveDeleteBearing) {
      refuseIfDeleteBearing(table, targetSnapshotId);
    }

    SnapshotPlan plan;
    if (request.startSnapshotId() != null) {
      // Incremental, answered inline like every other scan: the response is COMPLETED with every
      // task, so no replica owns state a later fetch could miss. Never cached: a range is not a
      // snapshot, and ranges do not repeat.
      plan = buildIncrementalPlan(table, request.startSnapshotId(), targetSnapshotId);
    } else if (targetSnapshotId != currentSnapshotId) {
      // time travel: plan directly, never disturb the current-snapshot cache
      plan = buildPlan(table, targetSnapshotId);
    } else {
      String cacheKey = table.location();
      plan = planCache.get(cacheKey);
      if (plan == null || plan.snapshotId() != targetSnapshotId) {
        // single-flight: concurrent cold plans for the same table share one manifest read
        synchronized (planLocks.computeIfAbsent(cacheKey, k -> new Object())) {
          plan = planCache.get(cacheKey);
          if (plan == null || plan.snapshotId() != targetSnapshotId) {
            plan = buildPlan(table, targetSnapshotId);
            planCache.put(cacheKey, plan);
            metrics.planCacheMisses.increment();
            LOG.info(
                "planned + cached {} tasks for {} @ snapshot {}",
                plan.statsTasks().size(), ident, targetSnapshotId);
          }
        }
      } else {
        metrics.planCacheHits.increment();
      }
    }

    List<FileScanTask> tasks = plan.statsTasks();
    if (request.filter() != null) {
      InclusiveMetricsEvaluator evaluator =
          new InclusiveMetricsEvaluator(table.schema(), request.filter(), request.caseSensitive());
      List<FileScanTask> kept = new ArrayList<>(tasks.size());
      for (FileScanTask task : tasks) {
        if (evaluator.eval(task.file())) {
          kept.add(task);
        }
      }
      tasks = kept;
    }

    tasks = pruner.prune(table, request.filter(), hints, tasks);
    metrics.planFilesTotal.add(plan.statsTasks().size());
    metrics.planFilesKept.add(tasks.size());

    // The statistics a task may carry out: none (strip, the default), or those of the columns
    // the request named in stats-fields (requested). Resolved per table so a table whose bounds
    // are not sensitive can buy back the scan cost of a filter Trino cannot push without them.
    java.util.Set<Integer> statsFieldIds = requestedStatsFieldIds(table, request);
    List<FileScanTask> response = new ArrayList<>(tasks.size());
    for (FileScanTask task : tasks) {
      if (!statsFieldIds.isEmpty()) {
        metrics.planTasksWithStats.increment();
        response.add(new NoResidualTask(task, task.file().copyWithStats(statsFieldIds)));
        continue;
      }
      FileScanTask stripped = plan.responseByPath().get(task.file().location());
      if (stripped == null) {
        // The two pinned-snapshot scans disagreed about which files exist, which should be
        // impossible. Falling back to the stats-laden task keeps the answer correct -- dropping the
        // file would be a false negative -- but it discloses the per-file bounds the two-pass design
        // exists to withhold, so it is counted and logged rather than done quietly.
        metrics.errors.increment();
        LOG.warn(
            "no stats-stripped task for {}; responding with the stats-laden one. Per-file column "
                + "bounds are leaving the proxy for this task.",
            task.file().location());
      }
      // residual must be absent, not alwaysTrue: alwaysTrue tells spec-honoring clients no row
      // filtering is needed, which would surface bloom false positives as results
      response.add(new NoResidualTask(stripped != null ? stripped : task));
    }

    return served(
        table, ident, Long.toString(targetSnapshotId), callerToken, plan.statsTasks().size(),
        response, start);
  }

  /**
   * The audit line and the per-table counters, then the response. One line per plan in a fixed
   * key order, so it greps and parses; what it never carries is the token or a path.
   */
  private PlanTableScanResponse served(
      Table table,
      TableIdentifier ident,
      String snapshot,
      String callerToken,
      int filesIn,
      List<FileScanTask> response,
      long startNanos) {
    metrics.planServed(ident.toString(), response.size());
    LOG.info(
        "plan table={} snapshot={} caller={} files_in={} files_kept={} ms={}",
        ident, snapshot, callerId(callerToken), filesIn, response.size(),
        (System.nanoTime() - startNanos) / 1_000_000);
    return completed(response, table);
  }

  /**
   * Who asked, as the first eight hex digits of the SHA-256 of the bearer: enough to tie one
   * caller's plans together in a log, never enough to replay one. {@code none} without a bearer.
   */
  private static String callerId(String authorization) {
    if (authorization == null || authorization.isBlank()) {
      return "none";
    }
    String token = authorization.strip();
    if (token.regionMatches(true, 0, "Bearer ", 0, 7)) {
      token = token.substring(7).strip();
    }
    try {
      byte[] digest =
          MessageDigest.getInstance("SHA-256").digest(token.getBytes(StandardCharsets.UTF_8));
      return HexFormat.of().formatHex(digest, 0, 4);
    } catch (NoSuchAlgorithmException e) {
      throw new IllegalStateException("SHA-256 is mandatory in every JVM", e);
    }
  }

  /**
   * A completed inline plan carrying these tasks.
   *
   * <p>{@code withSpecsById} is deprecated in Iceberg 1.11 with "visibility will be reduced in
   * 1.12" and no public replacement: the builder is expected to derive the specs itself from
   * 1.12. Until then it is the only way to put them on the response, and a response without them
   * cannot serialise a partitioned task. Suppressed here, once, rather than at each caller.
   */
  @SuppressWarnings("deprecation")
  private static PlanTableScanResponse completed(List<FileScanTask> tasks, Table table) {
    return PlanTableScanResponse.builder()
        .withPlanStatus(PlanStatus.COMPLETED)
        .withPlanId(INLINE_PLAN_ID_PREFIX + UUID.randomUUID())
        .withFileScanTasks(tasks)
        .withSpecsById(table.specs())
        .build();
  }

  /**
   * Refuses a snapshot carrying delete files. Fails closed: a snapshot that cannot be found, or one
   * whose summary does not state the count, is refused too, because "could not prove it is
   * delete-free" and "is delete-free" are different claims and only one is safe to serve.
   *
   * <p>Skippable via {@code KAHSHE_SERVE_DELETE_BEARING}, off by default. The refusal is broader
   * than the one break it is known to prevent: Trino 483 unboxes the absent
   * {@code DeleteFile.dataSequenceNumber} with no null guard and throws, while a stock
   * iceberg-java reader never consults the field and returns the right rows. That is what the
   * escape hatch is for; the module README names the clients measured either way.
   */
  private static void refuseIfDeleteBearing(Table table, long snapshotId) {
    org.apache.iceberg.Snapshot snapshot = table.snapshot(snapshotId);
    String deleteFiles =
        snapshot == null || snapshot.summary() == null
            ? null
            : snapshot.summary().get("total-delete-files");
    if (deleteFiles == null || !"0".equals(deleteFiles)) {
      throw new DeleteBearingSnapshotException(
          "snapshot " + snapshotId + " carries delete files (or its summary does not prove it "
              + "does not). Server-side planning would lose DeleteFile.dataSequenceNumber, so the "
              + "rows a merge-on-read reader returns would be wrong rather than merely slow. "
              + "RESOLUTION: reload the table. kahshe stops advertising scan-planning-mode=server "
              + "for a delete-bearing table, so a client that re-reads its config plans locally "
              + "and succeeds. This error means the client is still holding config it fetched "
              + "while the table was delete-free. Retrying the same plan will not help; "
              + "reloading will.");
    }
  }

  /** Delegate that withholds the residual so clients re-apply their own filter. */
  private static final class NoResidualTask implements FileScanTask {
    private final FileScanTask delegate;
    private final org.apache.iceberg.DataFile file;

    NoResidualTask(FileScanTask delegate) {
      this(delegate, null);
    }

    /** With {@code file} in place of the delegate's: the stats-scoped copy the response carries. */
    NoResidualTask(FileScanTask delegate, org.apache.iceberg.DataFile file) {
      this.delegate = delegate;
      this.file = file;
    }

    @Override
    public org.apache.iceberg.DataFile file() {
      return file != null ? file : delegate.file();
    }

    @Override
    public java.util.List<org.apache.iceberg.DeleteFile> deletes() {
      return delegate.deletes();
    }

    @Override
    public org.apache.iceberg.PartitionSpec spec() {
      return delegate.spec();
    }

    @Override
    public org.apache.iceberg.StructLike partition() {
      return delegate.partition();
    }

    @Override
    public long start() {
      return delegate.start();
    }

    @Override
    public long length() {
      return delegate.length();
    }

    @Override
    public org.apache.iceberg.expressions.Expression residual() {
      return null;
    }

    @Override
    public Iterable<FileScanTask> split(long targetSplitSize) {
      return delegate.split(targetSplitSize);
    }
  }

  private static SnapshotPlan buildPlan(Table table, long snapshotId) {
    return build(
        snapshotId,
        table.newScan().useSnapshot(snapshotId).includeColumnStats(),
        table.newScan().useSnapshot(snapshotId));
  }

  /** Files appended in {@code (start, end]}: the same two passes over Iceberg's incremental scan. */
  private static SnapshotPlan buildIncrementalPlan(Table table, long start, long end) {
    return build(
        end,
        table.newIncrementalAppendScan().fromSnapshotExclusive(start).toSnapshot(end)
            .includeColumnStats(),
        table.newIncrementalAppendScan().fromSnapshotExclusive(start).toSnapshot(end));
  }

  private static SnapshotPlan build(
      long snapshotId,
      org.apache.iceberg.Scan<?, FileScanTask, ?> statsScan,
      org.apache.iceberg.Scan<?, FileScanTask, ?> plainScan) {
    // two passes: stats-laden tasks for evaluation, stats-stripped tasks for responses
    List<FileScanTask> statsTasks = planFiles(statsScan);
    long weightBytes = PLAN_BASE_BYTES;
    for (FileScanTask task : statsTasks) {
      org.apache.iceberg.DataFile file = task.file();
      weightBytes +=
          taskWeightBytes(
              file.location(),
              file.valueCounts(),
              file.nullValueCounts(),
              file.lowerBounds(),
              file.upperBounds(),
              task.deletes().size());
    }
    Map<String, FileScanTask> responseByPath = new HashMap<>();
    for (FileScanTask task : planFiles(plainScan)) {
      responseByPath.put(task.file().location(), task);
      weightBytes += PLAIN_TASK_BYTES + BYTES_PER_PATH_CHAR * task.file().location().length();
    }
    return new SnapshotPlan(snapshotId, statsTasks, responseByPath, weightBytes);
  }

  // The calibrated heap estimate of a cached plan, by part: the plan's own objects, a response
  // task's shell, a stats-laden task's shell, a path's UTF-16 chars, a column's four stats entries
  // (value and null counts, lower and upper bound), and a delete-file reference.
  private static final long PLAN_BASE_BYTES = 512;
  private static final long PLAIN_TASK_BYTES = 256;
  private static final long STATS_TASK_BYTES = 384;
  private static final long BYTES_PER_PATH_CHAR = 2;
  private static final long BYTES_PER_STAT_COLUMN = 320;
  private static final long BYTES_PER_DELETE_REF = 96;

  /** One stats-laden task's calibrated heap estimate; plain values so it is unit-testable. */
  static long taskWeightBytes(
      String location,
      Map<?, ?> valueCounts,
      Map<?, ?> nullValueCounts,
      Map<Integer, java.nio.ByteBuffer> lowerBounds,
      Map<Integer, java.nio.ByteBuffer> upperBounds,
      int deleteCount) {
    int statCols =
        Math.max(mapSize(valueCounts), Math.max(mapSize(nullValueCounts), mapSize(lowerBounds)));
    return STATS_TASK_BYTES
        + BYTES_PER_PATH_CHAR * location.length()
        + BYTES_PER_STAT_COLUMN * statCols
        + boundsBytes(lowerBounds)
        + boundsBytes(upperBounds)
        + BYTES_PER_DELETE_REF * deleteCount;
  }

  private static int mapSize(Map<?, ?> map) {
    return map == null ? 0 : map.size();
  }

  private static long boundsBytes(Map<Integer, java.nio.ByteBuffer> bounds) {
    if (bounds == null) {
      return 0;
    }
    long sum = 0;
    for (java.nio.ByteBuffer buffer : bounds.values()) {
      sum += buffer == null ? 0 : buffer.remaining();
    }
    return sum;
  }

  private static List<FileScanTask> planFiles(org.apache.iceberg.Scan<?, FileScanTask, ?> scan) {
    List<FileScanTask> tasks = new ArrayList<>();
    try (CloseableIterable<FileScanTask> planned = scan.planFiles()) {
      planned.forEach(tasks::add);
    } catch (IOException e) {
      throw new UncheckedIOException(e);
    }
    return tasks;
  }

  /**
   * The field ids whose statistics this plan's tasks may carry: empty under strip, or when the
   * request named nothing; under requested, the named columns that exist. A mode nobody defined
   * strips, loudly -- a wider disclosure is never the fallback.
   */
  private java.util.Set<Integer> requestedStatsFieldIds(Table table, PlanTableScanRequest request) {
    String mode = table.properties().getOrDefault("kahshe.plan-stats", planStatsDefault);
    mode = mode == null ? "strip" : mode.trim().toLowerCase(java.util.Locale.ROOT);
    if (mode.equals("strip")) {
      return java.util.Set.of();
    }
    if (!mode.equals("requested")) {
      LOG.warn("kahshe.plan-stats / KAHSHE_PLAN_STATS must be strip or requested, got '{}' for {}; stripping",
          mode, table.name());
      return java.util.Set.of();
    }
    if (request.statsFields() == null || request.statsFields().isEmpty()) {
      return java.util.Set.of();
    }
    java.util.Set<Integer> ids = new java.util.HashSet<>();
    for (String name : request.statsFields()) {
      org.apache.iceberg.types.Types.NestedField field =
          request.caseSensitive() ? table.schema().findField(name) : table.schema().caseInsensitiveFindField(name);
      if (field != null) {
        ids.add(field.fieldId());
      }
    }
    return ids;
  }
}
