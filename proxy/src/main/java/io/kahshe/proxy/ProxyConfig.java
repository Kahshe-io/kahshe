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
 * @param planStats per-file column statistics in plan responses: {@code strip} (the default: none
 *     leave the proxy) or {@code requested} (those of the columns a request names in
 *     {@code stats-fields}). A table overrides it with {@code kahshe.plan-stats}. Disclosure
 *     traded for scan speed.
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
    long tableCacheTtlMs,
    String planStats,
    /** PEM CA bundle to trust the backend's certificate; empty for the JVM default. */
    String backendCa) {

  public boolean callerIdentityPlanning() {
    return "caller".equalsIgnoreCase(planningIdentity);
  }

  public String oauthTokenUri() {
    return backendBase + "/v1/oauth/tokens";
  }
}
