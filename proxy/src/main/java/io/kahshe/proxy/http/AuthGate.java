package io.kahshe.proxy.http;

import io.kahshe.common.Hashing;
import io.kahshe.common.Metrics;
import java.nio.charset.StandardCharsets;
import java.util.Base64;
import java.util.List;
import java.util.Map;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import io.kahshe.proxy.catalog.Mutations;

/**
 * Caller authorization for kahshe-served endpoints (/plan, /_count): the caller's own bearer
 * token must be able to load the table from the backing catalog. The backend stays the sole
 * authority; kahshe only memoizes positive verdicts.
 *
 * <p>Cache key is SHA-256(token)+table — raw credentials are never stored. Entries live for
 * {@code KAHSHE_AUTH_CACHE_TTL_MS} (default 60s), capped at the token's own JWT expiry when one
 * is parseable. Denials are negative-cached briefly ({@code NEGATIVE_TTL_MS}, 2 s) so a
 * bad-token storm does not amplify 1:1 into the backend. Revocation lag is bounded by the TTL.
 */
final class AuthGate {
  private static final Logger LOG = LoggerFactory.getLogger(AuthGate.class);

  private record Verdict(long expiresAtMs) {}

  private static final long NEGATIVE_TTL_MS = 2_000;

  private final Forwarder forwarder;
  private final long ttlMs;
  private final io.kahshe.common.BoundedCache<String, Verdict> cache =
      new io.kahshe.common.BoundedCache<>(10_000);
  /**
   * Short-lived denial cache: a bad-token storm must not amplify 1:1 into the backend.
   *
   * <p>Carries the status the backend actually gave, not just an expiry, because the status is what
   * a client acts on: engines treat 401 as "your token is bad" and abort or re-authenticate, while
   * a 5xx is something they retry.
   */
  private record Denial(int status, long untilMs) {}

  private final io.kahshe.common.BoundedCache<String, Denial> denied =
      new io.kahshe.common.BoundedCache<>(10_000);
  private final Metrics metrics;

  AuthGate(Forwarder forwarder, long ttlMs, Metrics metrics) {
    this.forwarder = forwarder;
    this.ttlMs = ttlMs;
    this.metrics = metrics;
  }

  /**
   * Returns null when authorized; otherwise the backend's response to relay (401/403/404...).
   */
  Forwarder.Response check(
      String prefixRaw, String namespaceRaw, String tableRaw, Map<String, List<String>> headers)
      throws Exception {
    List<String> auth = headers.get("Authorization");
    String token = auth == null || auth.isEmpty() ? null : auth.get(0);
    if (token == null) {
      metrics.authRejected.increment();
      return new Forwarder.Response(
          401,
          Map.of("Content-Type", List.of("application/json")),
          Mutations.errorBody(401, "NotAuthorizedException", "Authorization header required"));
    }

    String key = Hashing.sha256Base64(token) + "|" + prefixRaw + "|" + namespaceRaw + "|" + tableRaw;
    long now = System.currentTimeMillis();
    Verdict cached = cache.get(key);
    if (cached != null && cached.expiresAtMs() > now) {
      metrics.authCacheHits.increment();
      return null;
    }
    Denial deniedEntry = denied.get(key);
    if (deniedEntry != null && deniedEntry.untilMs() > now) {
      metrics.authRejected.increment();
      return new Forwarder.Response(
          deniedEntry.status(),
          Map.of("Content-Type", List.of("application/json")),
          Mutations.errorBody(
              deniedEntry.status(), "NotAuthorizedException", "not authorized for table"));
    }

    Forwarder.Response response =
        forwarder.forward(
            "GET",
            "/v1/" + prefixRaw + "/namespaces/" + namespaceRaw + "/tables/" + tableRaw,
            headers,
            new byte[0]);
    metrics.authCacheMisses.increment();
    if (response.status() / 100 != 2) {
      metrics.authRejected.increment();
      cache.remove(key);
      // Only genuine authorization answers are cached. A 5xx means the backend could not answer,
      // not that the caller may not read the table, so caching one would turn a brief backend
      // brownout into a credential failure lasting NEGATIVE_TTL_MS for every caller that hit it.
      // Anything outside this set is relayed once and re-asked next time.
      if (response.status() == 401 || response.status() == 403 || response.status() == 404) {
        denied.put(key, new Denial(response.status(), now + NEGATIVE_TTL_MS));
      }
      return response;
    }
    long expiry = Math.min(now + ttlMs, jwtExpiryMs(token));
    if (expiry > now) {
      cache.put(key, new Verdict(expiry));
    }
    return null;
  }

  /** Best-effort JWT exp (ms). Long.MAX_VALUE when the token is opaque or unparseable. */
  private static long jwtExpiryMs(String bearerToken) {
    try {
      String token = bearerToken.startsWith("Bearer ") ? bearerToken.substring(7) : bearerToken;
      String[] parts = token.split("\\.");
      if (parts.length != 3) {
        return Long.MAX_VALUE;
      }
      String payload = new String(Base64.getUrlDecoder().decode(parts[1]), StandardCharsets.UTF_8);
      java.util.regex.Matcher m =
          java.util.regex.Pattern.compile("\"exp\"\\s*:\\s*(\\d+)").matcher(payload);
      return m.find() ? Long.parseLong(m.group(1)) * 1000L : Long.MAX_VALUE;
    } catch (RuntimeException e) {
      return Long.MAX_VALUE;
    }
  }

}
