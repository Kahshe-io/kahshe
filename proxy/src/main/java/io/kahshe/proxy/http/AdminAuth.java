package io.kahshe.proxy.http;

import io.kahshe.common.Hashing;
import io.kahshe.common.Metrics;
import java.nio.charset.StandardCharsets;
import java.security.MessageDigest;
import java.util.Set;

/**
 * The bearer check on the admin port, when {@code KAHSHE_ADMIN_TOKEN} is set.
 *
 * <p>Not {@link AuthGate}: that one asks the backend whether the caller may load a table, and the
 * admin port has no table to ask about. This is a shared secret compared locally, which is the
 * right shape for a scrape target — one token in the collector's configuration and no round trip
 * per scrape.
 *
 * <p>{@code /healthz} and {@code /readyz} answer without a token whatever is configured. A kubelet
 * cannot easily carry a bearer, and a health endpoint that needs a secret fails closed for the
 * wrong reason: a rotated token would restart the pod or pull it from the Service with nothing
 * unhealthy. Those two say only "up" and "ready".
 */
public final class AdminAuth {
  /** The challenge a refusal carries, so a curl or a collector's log says what was missing. */
  static final String CHALLENGE = "Bearer realm=\"kahshe-admin\"";

  private static final Set<String> PROBES = Set.of("/healthz", "/readyz");

  /** A digest of the configured token, so the heap holds a hash of the secret and not the secret. */
  private final byte[] expected; // null when the port is open
  private final Metrics metrics;

  private AdminAuth(byte[] expected, Metrics metrics) {
    this.expected = expected;
    this.metrics = metrics;
  }

  /**
   * Open when {@code token} is null or blank: an empty variable is not a token. Surrounding
   * whitespace is dropped because a Secret written with {@code echo} carries a trailing newline
   * that no client sends, and every scrape would fail against it.
   */
  public static AdminAuth fromToken(String token, Metrics metrics) {
    return new AdminAuth(token == null || token.isBlank() ? null : digest(token.strip()), metrics);
  }

  public boolean enabled() {
    return expected != null;
  }

  /**
   * Whether a request to {@code path} carrying {@code authorization} (the header's value, or null)
   * may be served. A refusal is counted here, so the caller only has to answer it.
   */
  public boolean permits(String path, String authorization) {
    if (expected == null || PROBES.contains(path)) {
      return true;
    }
    // Digests rather than the strings: MessageDigest.isEqual takes the same time wherever the
    // inputs differ, and two fixed-length digests keep the token's length out of the timing too.
    if (authorization != null
        && authorization.regionMatches(true, 0, "Bearer ", 0, 7)
        && MessageDigest.isEqual(expected, digest(authorization.substring(7).strip()))) {
      return true;
    }
    metrics.adminAuthRejected.increment();
    return false;
  }

  private static byte[] digest(String value) {
    return Hashing.sha256Base64(value).getBytes(StandardCharsets.US_ASCII);
  }
}
