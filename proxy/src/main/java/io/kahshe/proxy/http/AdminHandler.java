package io.kahshe.proxy.http;

import io.kahshe.common.Metrics;
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

  /** One reachability observation and when it was made, published as a unit so it cannot tear. */
  private record Verdict(boolean reachable, long observedAt) {}

  private final Metrics metrics;
  private final Forwarder forwarder;
  private final AtomicBoolean shuttingDown;
  private final LongSupplier clock;
  private final ScheduledExecutorService prober;
  private volatile Verdict verdict; // null until the backend has been observed even once

  public AdminHandler(Metrics metrics, Forwarder forwarder, AtomicBoolean shuttingDown) {
    this(metrics, forwarder, shuttingDown, System::currentTimeMillis);
  }

  AdminHandler(
      Metrics metrics, Forwarder forwarder, AtomicBoolean shuttingDown, LongSupplier clock) {
    this(metrics, forwarder, shuttingDown, clock, READY_REFRESH_MS);
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
      LongSupplier clock,
      long refreshMs) {
    this.metrics = metrics;
    this.forwarder = forwarder;
    this.shuttingDown = shuttingDown;
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
      switch (path) {
        case "/healthz" -> respond(exchange, 200, "ok", "text/plain");
        case "/readyz" -> {
          boolean ready = !shuttingDown.get() && backendReachable();
          respond(exchange, ready ? 200 : 503, ready ? "ok" : "not ready", "text/plain");
        }
        case "/metrics" -> respond(exchange, 200, metrics.scrape(), "text/plain; version=0.0.4");
        default -> respond(exchange, 404, "not found", "text/plain");
      }
    } finally {
      exchange.close();
    }
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
