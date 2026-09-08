package io.kahshe.proxy;

import io.kahshe.proxy.plan.PlanService;

/**
 * What the serving side needs: the backing catalog and how to reach it as the service, the
 * planning identity, request bounds, and the serving guards. Nothing here is read by a build or
 * by a reader of the index artifact.
 *
 * @param serveDeleteBearing advertise and serve server-side planning even for a snapshot that is
 *     not provably delete-free. Off by default; it gates {@code Mutations} and
 *     {@code PlanService} together, because gating only one half leaves it inert.
 * @param tableCacheTtlMs how long a loaded table may be reused before it is re-read; 0 disables
 *     table caching entirely, which is what a multi-replica deployment needs
 * @param advertiseServerMode which tables are told to plan server-side: {@code all} (the
 *     default), {@code indexed} (only a table declaring {@code kahshe.index}) or {@code none}.
 *     A table not advertised to plans locally — correct, and it reads the same files, but it
 *     fetches the manifests itself and gets no index pruning. Narrow it when a client cannot do
 *     something while a table says planning MUST be server-side: DuckDB, for one, cannot run the
 *     first DELETE or UPDATE against such a table.
 * @param planStats per-file column statistics in plan responses: {@code strip} (the default: none
 *     leave the proxy) or {@code requested} (those of the columns a request names in
 *     {@code stats-fields}). A table overrides it with {@code kahshe.plan-stats}. Disclosure
 *     traded for scan speed.
 * @param planningIdentity whose credentials read a table's metadata on the served endpoints:
 *     {@code caller} (the default) plans as the bearer the request carried, so the catalog's own
 *     rules for that principal apply to every metadata read and one caller's plan is never
 *     served to another; {@code service} plans as {@code KAHSHE_CREDENTIAL} once the caller's
 *     token has loaded the table, one plan per table shared by everyone the backend admits.
 *     Right only where table-level read means see-everything.
 */
public record ProxyConfig(
    String backendBase,
    String credential,
    String scope,
    boolean injectPlanning,
    long authCacheTtlMs,
    long backendTimeoutMs,
    int maxBodyBytes,
    String planningIdentity,
    String backendWarehouse,
    boolean serveDeleteBearing,
    String advertiseServerMode,
    long tableCacheTtlMs,
    String planStats,
    /** PEM CA bundle to trust the backend's certificate; empty for the JVM default. */
    String backendCa) {

  /** {@code KAHSHE_PLANNING_IDENTITY}: the two identities, and the one a deployment gets unasked. */
  public static final String PLANNING_CALLER = "caller";

  public static final String PLANNING_SERVICE = "service";
  public static final String DEFAULT_PLANNING_IDENTITY = PLANNING_CALLER;

  /**
   * Exact, like the other mode knobs: the app refuses any other spelling at startup, so a typo
   * cannot fall through to whichever branch is the {@code else}.
   */
  public boolean callerIdentityPlanning() {
    return PLANNING_CALLER.equals(planningIdentity);
  }

  public String oauthTokenUri() {
    return backendBase + "/v1/oauth/tokens";
  }
}
