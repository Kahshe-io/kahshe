package io.kahshe.indexer.maintain;

import java.io.IOException;
import io.kahshe.analysis.Canonical;
import io.kahshe.format.BuildLease;
import io.kahshe.format.IcebergKinds;
import io.kahshe.format.type.gram.Grams;
import io.kahshe.format.IndexPaths;
import io.kahshe.format.type.term.TermIndexWriter;
import io.kahshe.common.Metrics;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.LinkedBlockingQueue;
import org.apache.iceberg.Table;
import org.apache.iceberg.catalog.TableIdentifier;
import org.apache.iceberg.rest.RESTUtil;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import io.kahshe.indexer.BuildConfig;
import io.kahshe.indexer.TableSource;
import io.kahshe.indexer.build.IndexBuildListener;
import io.kahshe.indexer.build.IndexBuilder;
import io.kahshe.indexer.build.IndexSettings;

/**
 * Automatic index maintenance with no watcher of its own: the passthrough traffic drives it.
 * When a loadTable response carries the {@code kahshe.index} table property (comma-separated
 * string columns) and the index is behind the table's snapshot, the table is queued; one daemon
 * worker rebuilds with the service credential. Failures are logged, counted, and retried on the
 * next observation — per COLUMN, so one column's failure neither skips its siblings nor keeps the
 * table out of the handled set. A column kahshe can never index (the wrong type, or a path through
 * a list or map) is refused instead of retried: it is counted separately from a build failure and
 * left alone until the table's schema changes, because no retry can change its answer.
 * Stale indexes only ever cost performance, so this loop needs no durability.
 * What the loop cannot do is announce that it has stopped — {@link IndexFreshness} tracks
 * observed-versus-built per table so a scrape can.
 *
 * <p>As a FLEET member ({@code KAHSHE_INDEX_FLEET}) the same loop shares the work with other
 * replicas: a column another member holds the {@link io.kahshe.format.BuildLease} on is skipped
 * and counted rather than refused, the table stays unclaimed so a later poll revisits it, and
 * the claim order is rotated by this member's {@link Fleet#ordinal} so N members start on N
 * different columns.
 */
public final class IndexerService {
  private static final Logger LOG = LoggerFactory.getLogger(IndexerService.class);

  public record Job(
      String prefixRaw, String namespaceRaw, String tableRaw, List<String> columns, long snapshotId) {}

  private final TableSource catalogs;
  private final Metrics metrics;
  private final LinkedBlockingQueue<Job> queue = new LinkedBlockingQueue<>(256);
  // last snapshot we built (or verified current) per table key; commit observations clear entries
  private final Map<String, Long> handled = new ConcurrentHashMap<>();
  /**
   * Columns refused as unindexable: {@code table key|column} to the schema id the refusal was
   * decided under. Without it a bad column costs a build attempt, a lease and a WARN on every
   * commit, forever, to reach the same answer — the dedup above is per TABLE and a new snapshot
   * clears it for every column at once.
   *
   * <p>Keyed by schema id rather than cleared on commit because a schema change is the only thing
   * that can make a refusal wrong: an ALTER that turns the column into a string is picked up on
   * the next observation, with no restart. It gains an entry only where an operator's property
   * names a column kahshe cannot index, which is why it is not bounded like {@link IndexFreshness}.
   */
  private final Map<String, Integer> refused = new ConcurrentHashMap<>();
  private final BuildConfig config;
  private final IndexBuildListener listener;
  /**
   * Columns refused because kahshe can never index them; {@link Metrics#indexColumnsRefused} says
   * why that is deliberately not {@code indexBuildFailures}. The adder lives here for the reason
   * {@code responseRewriteFailures}'s does: the component that moves a counter holds it, and
   * installs its sum into the {@code Metrics} supplier.
   */
  public final java.util.concurrent.atomic.LongAdder columnsRefused =
      new java.util.concurrent.atomic.LongAdder();
  final IndexFreshness freshness;
  /** This member's claim order; 0 and unused when not in a fleet. */
  private final int ordinal;

  public IndexerService(TableSource catalogs, Metrics metrics, boolean enabled,
      BuildConfig config) {
    this(catalogs, metrics, enabled, config, IndexBuildListener.NONE);
  }

  public IndexerService(TableSource catalogs, Metrics metrics, boolean enabled,
      BuildConfig config, IndexBuildListener listener) {
    this.config = config;
    this.catalogs = catalogs;
    this.metrics = metrics;
    metrics.indexColumnsRefused = columnsRefused::sum;
    this.listener = listener;
    // a process that does not build reads nothing behind
    this.freshness = new IndexFreshness(metrics, config.indexStaleWarnMs(), enabled);
    this.enabled = enabled;
    this.ordinal = config.fleet() ? Fleet.ordinal(config.fleetOrdinal()) : 0;
    if (enabled && config.fleet()) {
      LOG.info("indexer fleet member, claim order rotated by ordinal {}: a column another member "
          + "holds the lease on is skipped and counted, not refused", ordinal);
    }
    if (enabled) {
      Thread worker = new Thread(this::runWorker, "kahshe-indexer");
      worker.setDaemon(true);
      worker.start();
    }
  }

  /** Whether this process drains the queue at all ({@code KAHSHE_INDEXER}). */
  private final boolean enabled;

  /** Called from the loadTable passthrough with the observed policy and snapshot. Cheap. */
  public void observe(String prefixRaw, String namespaceRaw, String tableRaw,
      List<String> columns, long snapshotId) {
    String key = prefixRaw + "|" + namespaceRaw + "|" + tableRaw;
    // ahead of the dedup check: a table the dedup skips because it is already current must still
    // read as tracked-and-current, not as untracked
    freshness.observed(key, snapshotId);
    if (!enabled) {
      // A proxy-only replica (KAHSHE_INDEXER=false) records what it saw so its freshness gauges
      // stay honest, and enqueues nothing: there is no worker to drain the queue, so anything
      // queued here would only fill it and then report drops.
      return;
    }
    Long last = handled.get(key);
    if (last != null && last == snapshotId) {
      return;
    }
    Job job = new Job(prefixRaw, namespaceRaw, tableRaw, columns, snapshotId);
    if (queue.offer(job)) {
      handled.put(key, snapshotId); // claim; cleared again if the build fails
    } else {
      // A drop is not a no-op: index maintenance stops for that table, and the only downstream
      // symptom is kahshe_index_max_behind_seconds rising for a reason nothing explains. The claim
      // is not recorded, so the next observation of the same snapshot retries.
      metrics.indexerJobsDropped.increment();
      LOG.warn(
          "indexer queue is full ({} waiting); dropped the build for {}.{} at snapshot {}. The "
              + "index will lag until a later observation is accepted; watch "
              + "kahshe_index_max_behind_seconds.",
          queue.size(), namespaceRaw, tableRaw, snapshotId);
    }
  }

  /** This member's claim order, for a poller that shares the rotation (0 outside a fleet). */
  public int fleetOrdinal() {
    return ordinal;
  }

  /** Called when a commit passes through: the next loadTable observation re-triggers. */
  public void invalidate(String prefixRaw, String namespaceRaw, String tableRaw) {
    handled.remove(prefixRaw + "|" + namespaceRaw + "|" + tableRaw);
  }

  private void runWorker() {
    while (true) {
      Job job;
      try {
        job = queue.take();
      } catch (InterruptedException e) {
        Thread.currentThread().interrupt();
        return;
      }
      String key = job.prefixRaw() + "|" + job.namespaceRaw() + "|" + job.tableRaw();
      try {
        TableIdentifier ident = decodeIdent(job.namespaceRaw(), job.tableRaw());
        String prefix =
            java.net.URLDecoder.decode(job.prefixRaw(), java.nio.charset.StandardCharsets.UTF_8);
        Table table = catalogs.load(prefix, ident);
        long current = table.currentSnapshot() == null ? -1 : table.currentSnapshot().snapshotId();
        if (current != job.snapshotId()) {
          // The observation carried a snapshot this cached table predates (an out-of-band commit
          // the proxy never saw, so nothing invalidated the entry). Evaluating staleness against
          // the older view would decide the index is current and then record that older snapshot
          // as handled, wedging maintenance until something else evicts the entry.
          catalogs.invalidate(prefix, ident);
          table = catalogs.load(prefix, ident);
          current = table.currentSnapshot() == null ? -1 : table.currentSnapshot().snapshotId();
        }
        boolean skipped = false;
        boolean failed = false;
        // Rotated so N fleet members start on N different columns; a lone builder's list is
        // returned unchanged.
        for (String column : Fleet.rotate(job.columns(), ordinal)) {
          if (refusedUnderThisSchema(table, key, column)) {
            continue;
          }
          if (indexCurrent(table, column, current)) {
            continue;
          }
          long start = System.nanoTime();
          try {
            IndexBuilder.buildColumn(table, column.trim(), config, listener, prefix,
                ident.namespace().toString(), ident.name(), metrics);
          } catch (BuildLease.HeldByAnother e) {
            if (!config.fleet()) {
              throw e; // one builder per column: nothing else is going to build this
            }
            // Another member is building it. Not a failure and not this member's column.
            skipped = true;
            metrics.indexLeaseSkips.increment();
            LOG.info("skipping {}.{}: another fleet member holds its build lease ({})", ident,
                column, e.holder());
            continue;
          } catch (RuntimeException | IOException e) {
            // One column's failure stays one column's. Let it reach the table-level catch and every
            // sibling after it in the rotation goes unbuilt, and that catch un-claims by TABLE key,
            // so the same job reruns on every later observation. An Error is deliberately not
            // caught here -- it is not this loop's to absorb and carry on past.
            if (unindexable(table, column)) {
              refuse(table, key, column, ident, e);
            } else {
              failed = true;
              metrics.indexBuildFailures.increment();
              LOG.warn("index build failed for {}.{} (will retry on next observation)", ident,
                  column, e);
            }
            continue;
          }
          metrics.indexBuilds.increment();
          LOG.info("indexed {}.{} @ snapshot {} in {}ms (root {})", ident, column, current,
              (System.nanoTime() - start) / 1_000_000, IndexPaths.root(table, config.format().indexRoot()));
        }
        if (skipped || failed) {
          // Left unclaimed so a later observation revisits what this pass did not cover: a column
          // another member holds (the member that builds it records its own) or one whose build
          // failed in a way another attempt may not hit. A REFUSED column is neither, and does not
          // land here: it is claimed like a built one, because no number of retries can change a
          // configuration error's answer.
          handled.remove(key);
          if (failed) {
            freshness.failed(key);
          }
          continue;
        }
        handled.put(key, current);
        freshness.built(key, job.snapshotId(), current);
      } catch (Throwable e) {
        // Throwable: an Error here would silently end the sole indexer thread
        metrics.indexBuildFailures.increment();
        handled.remove(key); // retry on next observation
        freshness.failed(key);
        LOG.warn("index build failed for {} (will retry on next observation)", key, e);
      }
    }
  }

  /**
   * Whether the table's schema says this column can never be indexed — what separates a
   * configuration error from a build that failed and may yet pass.
   *
   * <p>Asked of the SCHEMA rather than read off the exception type, because a build raises
   * {@code IllegalArgumentException} from its middle too — a prior bloom leaf that will not parse
   * is one — and remembering that as permanent would stop maintaining a column that needed
   * nothing but its next build.
   *
   * <p>The negation of the admission test in {@code IndexBuilder.resolveTarget}, down to
   * {@code shapeOf} (a list or map is admitted on the kind of what it holds) and the struct-path
   * rule, and it has to move when that one does. The error is one-sided on purpose: a column this
   * answers false for is merely retried.
   */
  private static boolean unindexable(Table table, String column) {
    var field = IndexSettings.resolveField(table, column);
    return field == null
        || !Canonical.indexable(IcebergKinds.shapeOf(field.type()).kind())
        || IcebergKinds.repeatedPath(table.schema(), field.fieldId());
  }

  private boolean refusedUnderThisSchema(Table table, String key, String column) {
    Integer at = refused.get(key + "|" + column);
    return at != null && at == table.schema().schemaId();
  }

  private void refuse(
      Table table, String key, String column, TableIdentifier ident, Exception cause) {
    refused.put(key + "|" + column, table.schema().schemaId());
    columnsRefused.increment();
    LOG.warn(
        "{}.{} cannot be indexed: {}. That is a kahshe.index property to fix rather than a build "
            + "to retry, so it is not attempted again until the table's schema changes; the "
            + "table's other columns go on being maintained.",
        ident, column, cause.getMessage());
  }

  /**
   * The separator between namespace levels in a REST path segment: U+001F, URL-encoded. Iceberg
   * keeps its own copy of this constant package-private and deprecated the one-argument decoder
   * that supplied it, so the value lives here, once, for every site that decodes a raw path.
   */
  public static final String NAMESPACE_SEPARATOR = "%1F";

  /**
   * The encoding contract for {@link #observe}: it takes RAW url-encoded segments (the HTTP
   * handler passes raw path groups); this is the exact inverse applied by the worker. Callers
   * that start from decoded names (TableDiscovery) must encode before observing.
   */
  public static TableIdentifier decodeIdent(String namespaceRaw, String tableRaw) {
    return TableIdentifier.of(
        RESTUtil.decodeNamespace(namespaceRaw, NAMESPACE_SEPARATOR),
        java.net.URLDecoder.decode(tableRaw, java.nio.charset.StandardCharsets.UTF_8));
  }

  public boolean indexCurrent(Table table, String column, long currentSnapshot) {
    // Follows a rename by field id (IndexSettings.resolveField); only a name no schema ever had
    // is skipped, quietly, as a misconfigured property.
    var field = IndexSettings.resolveField(table, column);
    if (field == null) {
      return true; // misconfigured property; skip quietly, visible via logs on build attempts
    }
    try {
      var metaFile = IndexPaths.io(table, config.format()).newInputFile(
          TermIndexWriter.dir(IndexPaths.root(table, config.format().indexRoot()), field.fieldId()) + "/index-metadata.json");
      if (!metaFile.exists()) {
        return false;
      }
      try (var in = metaFile.newStream()) {
        var meta = new com.fasterxml.jackson.databind.ObjectMapper().readTree(in);
        var snapshot = TermIndexWriter.snapshotNode(meta);
        // A checkpoint is not current, whatever snapshot it names: the build died or is still
        // running, and the next observation must resume it through the incremental path.
        boolean partial = "true".equals(TermIndexWriter.propertiesNode(meta).path("partial").asText(null));
        // Nor is an index built under another analyzer: the reader serves it under its own rule
        // (N-1) while the rebuild under the current one runs, and this is what starts that
        // rebuild rather than waiting for the table to change.
        String expectedAnalyzer = IndexSettings.contract(table, column, config).id();
        boolean analyzerCurrent =
            expectedAnalyzer.equals(TermIndexWriter.propertiesNode(meta).path("analyzer").asText(null));
        // The gram rule and size too: an index cut under another is served under its own rule
        // (N-1) while the rebuild under the configured one runs, and this is what starts it.
        String expectedGrams = IndexSettings.grams(table, column, config).id();
        boolean gramsCurrent = expectedGrams.equals(
            Grams.Contract.of(TermIndexWriter.propertiesNode(meta).path("grams").asText(null)).id());
        return !partial
            && analyzerCurrent
            && gramsCurrent
            && snapshot.path("source-table-snapshot-id").asLong(snapshot.path("snapshotId").asLong())
                == currentSnapshot;
      }
    } catch (IOException | RuntimeException e) {
      return false; // unreadable metadata: rebuild
    }
  }
}
