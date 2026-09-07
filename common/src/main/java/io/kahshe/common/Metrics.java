package io.kahshe.common;

import java.util.concurrent.atomic.LongAdder;
import java.util.function.LongSupplier;

/**
 * Hand-rolled Prometheus-text metrics, with no client library. Everything kahshe knows about
 * itself in one scrape: request volume, planning behavior, pruning effectiveness, caches.
 *
 * <p>Counters are {@link LongAdder} fields a component increments directly. Gauges are
 * {@code volatile LongSupplier} fields the measured component installs at construction, so this
 * class reports on index tiers, watch windows and proxy rewrites while depending on none of them.
 * A gauge must keep its {@code () -> 0} default: that is what makes a scrape valid before wiring
 * and truthful in a process that never runs the subsystem at all.
 *
 * <p>A field carries a doc comment when its presence or its absence tells an operator something
 * no other signal does; the rest are named for the metric {@link #scrape} emits.
 */
public final class Metrics {
  public final LongAdder passthroughRequests = new LongAdder();
  public final LongAdder planRequests = new LongAdder();
  public final LongAdder countRequests = new LongAdder();
  public final LongAdder planDurationUsTotal = new LongAdder();
  public final LongAdder planFilesTotal = new LongAdder();
  public final LongAdder planFilesKept = new LongAdder();
  /** Tasks that left carrying per-file column statistics: always 0 under the default strip mode. */
  public final LongAdder planTasksWithStats = new LongAdder();
  public final LongAdder planCacheHits = new LongAdder();
  public final LongAdder planCacheMisses = new LongAdder();
  public final LongAdder authCacheHits = new LongAdder();
  public final LongAdder authCacheMisses = new LongAdder();
  public final LongAdder authRejected = new LongAdder();
  public final LongAdder errors = new LongAdder();
  public final LongAdder indexBuilds = new LongAdder();
  public final LongAdder indexBuildFailures = new LongAdder();
  /**
   * Index builds dropped because the indexer queue was full, meaning maintenance skipped a table's
   * snapshot entirely. Nothing else records it: a drop is not a failure, so no build counter moves,
   * and the only other symptom is {@code kahshe_index_max_behind_seconds} rising with no cause.
   */
  public final LongAdder indexerJobsDropped = new LongAdder();
  /** Columns another fleet member already held the lease on: work taken, not work lost. */
  public final LongAdder indexLeaseSkips = new LongAdder();
  /**
   * Window rules whose "N events within T" threshold was met. A key's slots clear on a trip, so a
   * sustained match counts once per N events rather than once per event thereafter.
   */
  public final LongAdder watchWindowTrips = new LongAdder();
  /**
   * Keys dropped by the per-shard LRU under the key cap, each losing its partial window. That is a
   * miss rather than a delay: a trip those events would have caused never fires, and nothing else
   * records it.
   */
  public final LongAdder watchWindowKeyEvictions = new LongAdder();
  /**
   * Events older than every event still retained for their key, which cannot be placed without
   * discarding something newer and are dropped instead. The other kind of miss: non-zero means
   * events are arriving out of order by more than the N most recent already seen for that key.
   */
  public final LongAdder watchWindowLateDrops = new LongAdder();

  /**
   * Data files the row scan read from a snapshot that carries delete files.
   *
   * <p>The scan reads a data file's raw rows and applies no delete file, so on a merge-on-read
   * table it sees rows the engine no longer returns. Pruning tolerates that direction; a
   * DETECTION does not — an alert would claim a match the engine cannot reproduce, and its
   * confirmation SQL, which does apply deletes, would come back lower than the alert claimed. An
   * empty or short confirmation reads as a false positive rather than as a caveat.
   *
   * <p>So the evidence says {@code advisory} instead of {@code exact} for those files, and this
   * counts them. A non-zero value means some alerts are over-counting, not that any were lost.
   */
  public final LongAdder watchScanDeleteBearingFiles = new LongAdder();
  /** Files re-read on first sight to rebuild the cross-file state a restart lost. */
  public final LongAdder watchReplayFilesRead = new LongAdder();
  /** Replays that hit the file budget: state left unrebuilt, so windows may not fire. */
  public final LongAdder watchReplayTruncated = new LongAdder();
  public volatile LongSupplier watchWindowKeys = () -> 0;
  public final LongAdder indexDataFilesRead = new LongAdder();
  public final LongAdder termBuildSpills = new LongAdder();
  /**
   * Token-set lookups against the term index: one per call, whether or not a leaf had to be read.
   * {@link #termEntryCacheHits} covers the tokens answered from the resolved-entry cache, and
   * prefix and range lookups are counted by {@link #termPrefixLookups} instead.
   */
  public final LongAdder termLookups = new LongAdder();
  public final LongAdder termPrefixLookups = new LongAdder();
  public final LongAdder termPrefixCapped = new LongAdder();
  public final LongAdder termBuildRunMerges = new LongAdder();
  public final LongAdder indexCacheEvictions = new LongAdder();
  public final LongAdder termCacheEvictions = new LongAdder();
  public final LongAdder termEntryCacheHits = new LongAdder();
  public final LongAdder planCacheEvictions = new LongAdder();
  public final LongAdder gramCacheEvictions = new LongAdder();
  public final LongAdder gramBuildsSkipped = new LongAdder();
  public final LongAdder gramTooLarge = new LongAdder();
  /**
   * Data files whose gram set saturated its own alphabet's n-gram space during a build: for those
   * files the gram and bloom tiers keep everything a probe in that alphabet asks about, which is
   * correct but not selective. The build is the only place it can be said — on the query side
   * saturation is indistinguishable from data that simply does not prune.
   */
  public final LongAdder gramSaturatedFiles = new LongAdder();
  /**
   * Bloom tiers refused at load because they exceed the cache budget. Non-zero means a column is
   * serving without its bloom tier: correct, since an absent bloom keeps files, but slower, and
   * nothing else surfaces it.
   */
  public final LongAdder indexTooLarge = new LongAdder();
  /**
   * loadTable responses where server planning was not advertised, which happens when the snapshot
   * is not provably delete-free. It moves per loadTable rather than per table, so read it as
   * traffic against unservable snapshots, not as a table count.
   */
  public final LongAdder serverPlanningDeclined = new LongAdder();

  /**
   * Plans that would have been built from a view older than the one this proxy forwarded. Each is
   * a query that would otherwise have silently missed rows, so this measures out-of-band write
   * pressure -- commits reaching the catalog without passing through kahshe. Zero on a fleet that
   * writes only through the proxy.
   */
  public final LongAdder planStaleViewReloads = new LongAdder();

  /**
   * Cached tables dropped because a commit passed through the proxy. Counts the decision, not the
   * eviction: a prefix with no catalog client yet has nothing to drop. Useful mainly for its
   * absence -- a fleet committing steadily while this stays flat means commit observation has
   * stopped, which otherwise shows up only as occasional stale plans.
   */
  public final LongAdder tableInvalidations = new LongAdder();

  /**
   * Mutating requests whose body kahshe could not read, so it learned no table names from them;
   * those tables stay cached until a plan reconciles them against a forwarded snapshot or the TTL
   * expires. Sustained non-zero means the request format has moved and proactive invalidation is
   * gone, leaving only those two.
   */
  public final LongAdder invalidationParseFailures = new LongAdder();

  public final LongAdder watchAlerts = new LongAdder();
  /** Build reports the poller acted on: one per build it had not seen, however often it polls. */
  public final LongAdder watchReportsSeen = new LongAdder();
  /**
   * Reports whose predecessor is not the last build this watcher recorded, meaning a build in
   * between was never polled. The poller only ever reads the newest report, so that build's alerts
   * were not delivered from here and never will be.
   */
  public final LongAdder watchReportsMissed = new LongAdder();
  /**
   * Alerts delivered out of those reports. Deduplicated per (rule, file) within this process, so
   * an alert this watcher already raised from its own scan is not counted again.
   */
  public final LongAdder watchReportAlertsDelivered = new LongAdder();
  public final LongAdder watchSuppressed = new LongAdder();
  public final LongAdder watchWebhookDropped = new LongAdder();
  public final LongAdder watchWebhookFailures = new LongAdder();
  public final LongAdder watchRulesSkipped = new LongAdder();
  /**
   * Builds whose snapshot could not be proven delete-free while a token rule was loaded, so the
   * counts that build produced are upper bounds. The row scan has its own counter for the same
   * hazard on its own path; this is the index-riding half, and without it a delete-bearing build
   * is visible only in a log line.
   */
  public final LongAdder watchBuildDeleteBearing = new LongAdder();
  /**
   * Files whose per-file term map hit its cap, so token rules were skipped for them. Non-zero
   * means alerting has a hole: those files were indexed but their match rules did not run.
   */
  public final LongAdder watchCountsTruncated = new LongAdder();

  public volatile LongSupplier indexCacheWeightBytes = () -> 0;
  public volatile LongSupplier termCacheWeightBytes = () -> 0;
  public volatile LongSupplier planCacheWeightBytes = () -> 0;
  public volatile LongSupplier gramCacheWeightBytes = () -> 0;
  /**
   * The denominator for the four weight gauges above: the sum of the cache budgets this process
   * configured, counting the gram budget only when the gram tier is enabled. Fixed at wiring, so
   * it is a plain value rather than a supplier.
   */
  public volatile long cacheBudgetBytes;
  public volatile LongSupplier watchRulesLoaded = () -> 0;
  public volatile LongSupplier watchRulesUncovered = () -> 0;
  /**
   * Tables this process has observed declaring an index column. Per-process and LRU-bounded, so a
   * restart resets it and a replica that never sees a table's traffic never tracks that table.
   */
  public volatile LongSupplier indexTablesTracked = () -> 0;
  /**
   * Of those, the ones whose observed snapshot no completed build has covered — the difference
   * between an index that is current and one that has quietly stopped. Reads 0 on a process whose
   * indexer is off, whatever it observed, since it cannot close the gap.
   */
  public volatile LongSupplier indexTablesBehind = () -> 0;
  /**
   * Columns {@code kahshe.index} names that the build refuses outright — a wrong type, or a leaf
   * under a list or map. Deliberately NOT folded into {@code index_build_failures}: that counter
   * reads as "builds are failing, look at the cluster", and a configuration error paging like an
   * outage is as bad as an outage hiding inside one. Non-zero means somebody's property names a
   * column that will never build, and the WARN says which.
   */
  public volatile LongSupplier indexColumnsRefused = () -> 0;
  /**
   * Response rewrites that fell back to the untouched backend body. The adder itself lives beside
   * the rewrite code in the proxy and its handler installs the sum here, so this registry stays
   * free of the proxy; an index or watch process reports 0, which is true.
   */
  public volatile LongSupplier responseRewriteFailures = () -> 0;
  public volatile LongSupplier indexMaxBehindSeconds = () -> 0;
  public volatile LongSupplier indexLastBuildAgeSeconds = () -> 0;

  /**
   * Dead ordinals as a percentage, at the worst of the (table, field) pairs this process has
   * loaded. It and {@link #indexBloomLeaves} report growth that nothing reclaims: such an index
   * answers correctly at a rising cold-load cost and moves no other signal. Both read 0 on a
   * fresh index and are per-process and not durable, like the freshness gauges.
   */
  public volatile LongSupplier indexDeadOrdinalPercent = () -> 0;

  /** Bloom leaves at the worst pair loaded: one per build, and one cold-read round trip each. */
  public volatile LongSupplier indexBloomLeaves = () -> 0;

  /**
   * Per-tier file counts across one plan's pruning cascade: how many file-scan tasks each index
   * type was handed, and how many it kept.
   *
   * <p>Labelled because the types are a ServiceLoader seam — a deployment can carry one nobody
   * here has named — so a fixed field per tier would be wrong the day someone adds one. These are
   * the only labelled counters in this class; the label is the type's own key.
   *
   * <p>{@code planFilesTotal} and {@code planFilesKept} are recorded ONCE per plan, so they cannot
   * say which tier did the pruning or whether a second predicate narrowed anything; these can.
   */
  public final java.util.concurrent.ConcurrentHashMap<String, LongAdder> pruneFilesIn =
      new java.util.concurrent.ConcurrentHashMap<>();

  public final java.util.concurrent.ConcurrentHashMap<String, LongAdder> pruneFilesKept =
      new java.util.concurrent.ConcurrentHashMap<>();

  /** Records one tier's turn: what it was handed and what it returned. */
  public void prunePass(String tier, int in, int kept) {
    pruneFilesIn.computeIfAbsent(tier, k -> new LongAdder()).add(in);
    pruneFilesKept.computeIfAbsent(tier, k -> new LongAdder()).add(kept);
  }

  public String scrape() {
    StringBuilder sb = new StringBuilder();
    counter(sb, "kahshe_passthrough_requests_total", passthroughRequests);
    counter(sb, "kahshe_plan_requests_total", planRequests);
    counter(sb, "kahshe_count_requests_total", countRequests);
    counter(sb, "kahshe_plan_duration_us_total", planDurationUsTotal);
    counter(sb, "kahshe_plan_files_total", planFilesTotal);
    counter(sb, "kahshe_plan_files_kept_total", planFilesKept);
    labelled(sb, "kahshe_prune_files_in_total", "tier", pruneFilesIn);
    labelled(sb, "kahshe_prune_files_kept_total", "tier", pruneFilesKept);
    counter(sb, "kahshe_plan_tasks_with_stats_total", planTasksWithStats);
    counter(sb, "kahshe_plan_cache_hits_total", planCacheHits);
    counter(sb, "kahshe_plan_cache_misses_total", planCacheMisses);
    counter(sb, "kahshe_auth_cache_hits_total", authCacheHits);
    counter(sb, "kahshe_auth_cache_misses_total", authCacheMisses);
    counter(sb, "kahshe_auth_rejected_total", authRejected);
    counter(sb, "kahshe_errors_total", errors);
    counter(sb, "kahshe_index_builds_total", indexBuilds);
    counter(sb, "kahshe_index_build_failures_total", indexBuildFailures);
    counter(sb, "kahshe_indexer_jobs_dropped_total", indexerJobsDropped);
    counter(sb, "kahshe_index_lease_skips_total", indexLeaseSkips);
    counter(sb, "kahshe_watch_window_trips_total", watchWindowTrips);
    counter(sb, "kahshe_watch_window_key_evictions_total", watchWindowKeyEvictions);
    counter(sb, "kahshe_watch_window_late_drops_total", watchWindowLateDrops);
    counter(sb, "kahshe_watch_scan_delete_bearing_files_total", watchScanDeleteBearingFiles);
    counter(sb, "kahshe_watch_replay_files_read_total", watchReplayFilesRead);
    counter(sb, "kahshe_watch_replay_truncated_total", watchReplayTruncated);
    gauge(sb, "kahshe_watch_window_keys", watchWindowKeys.getAsLong());
    counter(sb, "kahshe_index_columns_refused_total", indexColumnsRefused.getAsLong());
    counter(sb, "kahshe_response_rewrite_failures_total", responseRewriteFailures.getAsLong());
    counter(sb, "kahshe_index_data_files_read_total", indexDataFilesRead);
    counter(sb, "kahshe_term_build_spills_total", termBuildSpills);
    counter(sb, "kahshe_term_lookups_total", termLookups);
    counter(sb, "kahshe_term_prefix_lookups_total", termPrefixLookups);
    counter(sb, "kahshe_term_prefix_capped_total", termPrefixCapped);
    counter(sb, "kahshe_term_build_run_merges_total", termBuildRunMerges);
    counter(sb, "kahshe_index_cache_evictions_total", indexCacheEvictions);
    counter(sb, "kahshe_term_cache_evictions_total", termCacheEvictions);
    counter(sb, "kahshe_term_entry_cache_hits_total", termEntryCacheHits);
    counter(sb, "kahshe_plan_cache_evictions_total", planCacheEvictions);
    counter(sb, "kahshe_gram_cache_evictions_total", gramCacheEvictions);
    counter(sb, "kahshe_gram_builds_skipped_total", gramBuildsSkipped);
    counter(sb, "kahshe_gram_too_large_total", gramTooLarge);
    counter(sb, "kahshe_gram_saturated_files_total", gramSaturatedFiles);
    counter(sb, "kahshe_index_too_large_total", indexTooLarge);
    counter(sb, "kahshe_server_planning_declined_total", serverPlanningDeclined);
    counter(sb, "kahshe_plan_stale_view_reloads_total", planStaleViewReloads);
    counter(sb, "kahshe_table_invalidations_total", tableInvalidations);
    counter(sb, "kahshe_invalidation_parse_failures_total", invalidationParseFailures);
    counter(sb, "kahshe_watch_alerts_total", watchAlerts);
    counter(sb, "kahshe_watch_reports_seen_total", watchReportsSeen);
    counter(sb, "kahshe_watch_reports_missed_total", watchReportsMissed);
    counter(sb, "kahshe_watch_report_alerts_delivered_total", watchReportAlertsDelivered);
    counter(sb, "kahshe_watch_suppressed_total", watchSuppressed);
    counter(sb, "kahshe_watch_webhook_dropped_total", watchWebhookDropped);
    counter(sb, "kahshe_watch_webhook_failures_total", watchWebhookFailures);
    counter(sb, "kahshe_watch_rules_skipped_total", watchRulesSkipped);
    counter(sb, "kahshe_watch_build_delete_bearing_total", watchBuildDeleteBearing);
    counter(sb, "kahshe_watch_counts_truncated_total", watchCountsTruncated);
    gauge(sb, "kahshe_watch_rules_loaded", watchRulesLoaded.getAsLong());
    gauge(sb, "kahshe_watch_rules_uncovered", watchRulesUncovered.getAsLong());
    gauge(sb, "kahshe_index_cache_weight_bytes", indexCacheWeightBytes.getAsLong());
    gauge(sb, "kahshe_term_cache_weight_bytes", termCacheWeightBytes.getAsLong());
    gauge(sb, "kahshe_plan_cache_weight_bytes", planCacheWeightBytes.getAsLong());
    gauge(sb, "kahshe_gram_cache_weight_bytes", gramCacheWeightBytes.getAsLong());
    gauge(sb, "kahshe_cache_budget_bytes", cacheBudgetBytes);
    gauge(sb, "kahshe_index_tables_tracked", indexTablesTracked.getAsLong());
    gauge(sb, "kahshe_index_tables_behind", indexTablesBehind.getAsLong());
    gauge(sb, "kahshe_index_max_behind_seconds", indexMaxBehindSeconds.getAsLong());
    gauge(sb, "kahshe_index_last_build_age_seconds", indexLastBuildAgeSeconds.getAsLong());
    gauge(sb, "kahshe_index_dead_ordinal_percent", indexDeadOrdinalPercent.getAsLong());
    gauge(sb, "kahshe_index_bloom_leaves", indexBloomLeaves.getAsLong());
    return sb.toString();
  }

  private static void counter(StringBuilder sb, String name, LongAdder value) {
    sb.append("# TYPE ").append(name).append(" counter\n");
    sb.append(name).append(' ').append(value.sum()).append('\n');
  }

  private static void counter(StringBuilder sb, String name, long value) {
    sb.append("# TYPE ").append(name).append(" counter\n");
    sb.append(name).append(' ').append(value).append('\n');
  }

  /**
   * One counter per label value, sorted so a scrape is stable to diff.
   *
   * <p>Emits the TYPE line once and nothing at all when the map is empty, which is what a
   * Prometheus parser expects of a metric family with no series yet.
   */
  private static void labelled(StringBuilder sb, String name, String label,
      java.util.Map<String, LongAdder> values) {
    if (values.isEmpty()) {
      return;
    }
    sb.append("# TYPE ").append(name).append(" counter\n");
    for (String key : new java.util.TreeSet<>(values.keySet())) {
      sb.append(name).append('{').append(label).append("=\"").append(key).append("\"} ")
          .append(values.get(key).sum()).append('\n');
    }
  }

  private static void gauge(StringBuilder sb, String name, long value) {
    sb.append("# TYPE ").append(name).append(" gauge\n");
    sb.append(name).append(' ').append(value).append('\n');
  }
}
