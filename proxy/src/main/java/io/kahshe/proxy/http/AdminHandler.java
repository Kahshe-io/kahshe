package io.kahshe.proxy.http;

import io.kahshe.common.Metrics;
import io.kahshe.indexer.maintain.IndexStatus;
import com.fasterxml.jackson.databind.node.JsonNodeFactory;
import com.sun.net.httpserver.HttpExchange;
import com.sun.net.httpserver.HttpHandler;
import java.io.IOException;
import java.io.OutputStream;
import java.nio.charset.StandardCharsets;
import java.util.Map;
import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.function.LongSupplier;
import java.util.regex.Matcher;
import java.util.regex.Pattern;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Health/readiness/metrics on a dedicated port with its own executor, so probes keep answering
 * while the data-plane pool is saturated by a slow backend. Readiness = backend answered with any
 * status &lt; 500 recently; flips false on shutdown.
 *
 * <p>No request thread here ever touches the backend: the backend call runs on the prober's own
 * thread and {@code /readyz} answers from the last verdict it published, so every admin endpoint
 * costs one volatile read whatever the server's thread count and however many probes arrive at
 * once. Probing inline instead would let a slow backend delay {@code /healthz} by up to the
 * backend timeout, which is longer than a liveness probe usually waits.
 *
 * <p>Everything but the two probes passes {@link AdminAuth} first, so a 401 is answered before an
 * unknown path is a 404: an unauthenticated caller learns nothing about what the port serves.
 * That includes {@code /index}, the per-table index status, which names tables and snapshots.
 */
public final class AdminHandler implements HttpHandler, AutoCloseable {
  private static final Logger LOG = LoggerFactory.getLogger(AdminHandler.class);

  /** How often the prober re-observes the backend, on a schedule, whether or not anyone asks. */
  private static final long READY_REFRESH_MS = 5_000;

  /**
   * Age past which a verdict is not reported at all, so a wedged prober and a working one cannot
   * look the same: a prober stuck inside {@code forward} publishes nothing further, and a stale
   * {@code ready} would otherwise be served forever. Ageing out fails the same way an unreachable
   * backend does — not ready.
   *
   * <p>Only a scheduled refresh lets this age mean "the prober stopped". A refresh kicked by
   * {@code /readyz} could not tell a wedged prober from nobody asking, and a readiness probe with
   * a period above this bound would find the verdict stale on every call and never report ready.
   */
  private static final long READY_STALE_MS = 30_000;

  private static final String JSON = "application/json";
  private static final Pattern ONE_TABLE = Pattern.compile("/index/([^/]+)/([^/]+)/([^/]+)");

  /** One reachability observation and when it was made, published as a unit so it cannot tear. */
  private record Verdict(boolean reachable, long observedAt) {}

  private final Metrics metrics;
  private final Forwarder forwarder;
  private final AtomicBoolean shuttingDown;
  private final AdminAuth auth;
  private final IndexStatus.Source status;
  private final LongSupplier clock;
  private final ScheduledExecutorService prober;
  private volatile Verdict verdict; // null until the backend has been observed even once

  /** A port with no indexer behind it: {@code /index} answers an empty listing that says so. */
  public AdminHandler(
      Metrics metrics, Forwarder forwarder, AtomicBoolean shuttingDown, AdminAuth auth) {
    this(metrics, forwarder, shuttingDown, auth, IndexStatus.Source.NONE);
  }

  public AdminHandler(
      Metrics metrics,
      Forwarder forwarder,
      AtomicBoolean shuttingDown,
      AdminAuth auth,
      IndexStatus.Source status) {
    this(metrics, forwarder, shuttingDown, auth, status, System::currentTimeMillis,
        READY_REFRESH_MS);
  }

  /** An open port, for tests of the probes themselves. */
  AdminHandler(
      Metrics metrics,
      Forwarder forwarder,
      AtomicBoolean shuttingDown,
      LongSupplier clock,
      long refreshMs) {
    this(metrics, forwarder, shuttingDown, AdminAuth.fromToken(null, metrics),
        IndexStatus.Source.NONE, clock, refreshMs);
  }

  /**
   * @param clock injected so a caller can age a verdict past {@code READY_STALE_MS} without
   *     sleeping for it
   * @param refreshMs likewise, so the prober's own republishing can be observed without waiting
   *     {@code READY_REFRESH_MS} of real time
   */
  AdminHandler(
      Metrics metrics,
      Forwarder forwarder,
      AtomicBoolean shuttingDown,
      AdminAuth auth,
      IndexStatus.Source status,
      LongSupplier clock,
      long refreshMs) {
    this.metrics = metrics;
    this.forwarder = forwarder;
    this.shuttingDown = shuttingDown;
    this.auth = auth;
    this.status = status;
    this.clock = clock;
    this.prober =
        Executors.newSingleThreadScheduledExecutor(
            runnable -> {
              Thread thread = new Thread(runnable, "kahshe-readyz-probe");
              thread.setDaemon(true);
              return thread;
            });
    // Fixed delay, not rate: a probe that takes its full timeout must not have the next one queued
    // up behind it. Zero initial delay, so the first observation starts with the process and the
    // pod is not held out of service for a readiness period waiting to be asked.
    prober.scheduleWithFixedDelay(this::probe, 0, refreshMs, TimeUnit.MILLISECONDS);
  }

  @Override
  public void handle(HttpExchange exchange) throws IOException {
    String path = exchange.getRequestURI().getPath();
    try {
      if (!auth.permits(path, exchange.getRequestHeaders().getFirst("Authorization"))) {
        exchange.getResponseHeaders().set("WWW-Authenticate", AdminAuth.CHALLENGE);
        respond(exchange, 401, "unauthorized: send Authorization: Bearer <KAHSHE_ADMIN_TOKEN>",
            "text/plain");
        return;
      }
      switch (path) {
        case "/healthz" -> respond(exchange, 200, "ok", "text/plain");
        case "/readyz" -> {
          boolean ready = !shuttingDown.get() && backendReachable();
          respond(exchange, ready ? 200 : 503, ready ? "ok" : "not ready", "text/plain");
        }
        case "/metrics" -> respond(exchange, 200, metrics.scrape(), "text/plain; version=0.0.4");
        case "/index" -> respond(exchange, 200, IndexStatus.listing(status).toString(), JSON);
        default -> {
          // The RAW path: a multi-level namespace travels as %1F-joined levels, exactly as it
          // does on the data port, and the decoded form would have lost where one level ends.
          Matcher table = ONE_TABLE.matcher(exchange.getRequestURI().getRawPath());
          if (table.matches()) {
            oneTable(exchange, table.group(1), table.group(2), table.group(3));
          } else {
            respond(exchange, 404, "not found", "text/plain");
          }
        }
      }
    } finally {
      exchange.close();
    }
  }

  private void oneTable(HttpExchange exchange, String prefixRaw, String namespaceRaw, String tableRaw)
      throws IOException {
    IndexStatus found;
    try {
      found = IndexStatus.find(status, prefixRaw, namespaceRaw, tableRaw);
    } catch (IllegalArgumentException e) {
      respond(exchange, 400, note("a path segment is not valid percent-encoding"), JSON);
      return;
    }
    if (found != null) {
      respond(exchange, 200, found.toJson().toString(), JSON);
      return;
    }
    // Says why, because "not tracked" has three causes an operator must tell apart: no indexer
    // here, no loadTable for the table has passed this replica since it started, or the table
    // has no kahshe.index property at all.
    String note = status.note();
    respond(exchange, 404,
        note(note != null ? note : "not tracked by this process: no loadTable response declaring "
            + "kahshe.index for this table has passed it since startup"),
        JSON);
  }

  private static String note(String text) {
    return JsonNodeFactory.instance.objectNode().put("note", text).toString();
  }

  /** A read of the last published verdict. Starts nothing and waits for nothing. */
  private boolean backendReachable() {
    Verdict current = verdict;
    return current != null
        && clock.getAsLong() - current.observedAt() < READY_STALE_MS
        && current.reachable();
  }

  /**
   * One observation, published whatever happens.
   *
   * <p>This must not throw: {@code scheduleWithFixedDelay} cancels a task that does, permanently
   * and silently, which would stop readiness being refreshed for the life of the process.
   */
  private void probe() {
    boolean reachable = false;
    try {
      reachable = forwarder.forward("GET", "/v1/config", Map.of(), new byte[0]).status() < 500;
    } catch (InterruptedException e) {
      Thread.currentThread().interrupt();
    } catch (Exception e) {
      // Refused, unresolvable, or timed out: all of them mean not ready.
    }
    try {
      verdict = new Verdict(reachable, clock.getAsLong());
    } catch (RuntimeException e) {
      LOG.warn("readiness probe could not publish its verdict", e);
    }
  }

  /** Stops the scheduled prober. The thread is a daemon, so this is tidiness, not a shutdown gate. */
  @Override
  public void close() {
    prober.shutdownNow();
  }

  private static void respond(HttpExchange exchange, int status, String body, String contentType)
      throws IOException {
    byte[] bytes = body.getBytes(StandardCharsets.UTF_8);
    exchange.getResponseHeaders().set("Content-Type", contentType);
    exchange.sendResponseHeaders(status, bytes.length);
    try (OutputStream out = exchange.getResponseBody()) {
      out.write(bytes);
    }
  }
}
