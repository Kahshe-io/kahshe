package io.kahshe.watch.sink;

import io.kahshe.common.Metrics;
import java.io.IOException;
import java.net.URI;
import java.net.http.HttpClient;
import java.net.http.HttpRequest;
import java.net.http.HttpResponse;
import java.time.Duration;
import java.util.concurrent.LinkedBlockingQueue;
import java.util.concurrent.atomic.AtomicLong;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import io.kahshe.watch.WatchConfig;

/**
 * Delivers alert payloads to KAHSHE_WATCH_WEBHOOK on a single daemon drain thread: bounded queue
 * (full = drop + count), 3 attempts with 1s/5s backoff per payload. Config is validated at
 * startup — a malformed URL or header value disables the webhook with an ERROR log instead of
 * throwing later on the drain thread. Alerts always log and count regardless.
 *
 * <p>The default {@link AlertSink}, selected by KAHSHE_WATCH_SINK=webhook.
 */
public final class WebhookSink implements AlertSink {
  private static final Logger LOG = LoggerFactory.getLogger(WebhookSink.class);

  /** The name this sink is selected by. */
  public static final String NAME = "webhook";

  private final LinkedBlockingQueue<byte[]> queue = new LinkedBlockingQueue<>(1024);
  // queued + in-flight deliveries; incremented before offer so awaitDrain never misses a payload
  private final AtomicLong pending = new AtomicLong();
  private final HttpClient client;
  private final URI uri; // null = disabled
  private final String auth;
  private final long timeoutMs;
  private final long[] backoffMs;
  private final Metrics metrics;

  /** Announces the webhook sink to {@link java.util.ServiceLoader}. */
  public static final class Provider implements AlertSinkProvider {
    @Override
    public String name() {
      return NAME;
    }

    @Override
    public AlertSink create(WatchConfig config, Metrics metrics) {
      return fromEnv(config, metrics);
    }
  }

  /**
   * Builds the sink from the watch config, disabling it (with an ERROR log) rather than failing
   * startup when the URL or the Authorization value is malformed.
   *
   * @param config the watch configuration
   * @param metrics the process metrics
   * @return the sink, enabled only if KAHSHE_WATCH_WEBHOOK is set and valid
   */
  public static WebhookSink fromEnv(WatchConfig config, Metrics metrics) {
    if (config.watchWebhook().isBlank()) {
      return new WebhookSink(null, "", 0, new long[0], metrics);
    }
    try {
      URI uri = new URI(config.watchWebhook());
      String scheme = uri.getScheme();
      if (!"http".equals(scheme) && !"https".equals(scheme)) {
        throw new IllegalArgumentException("scheme must be http or https");
      }
      // builds a request now so an invalid Authorization value fails here, not on the drain thread
      HttpRequest.Builder probe = HttpRequest.newBuilder(uri);
      if (!config.watchWebhookAuth().isBlank()) {
        probe.header("Authorization", config.watchWebhookAuth());
      }
      probe.build();
      return new WebhookSink(uri, config.watchWebhookAuth(), config.watchWebhookTimeoutMs(),
          new long[] {1_000, 5_000}, metrics);
    } catch (Exception e) {
      LOG.error(
          "KAHSHE_WATCH_WEBHOOK config invalid; webhook DISABLED (alerts still log and count): {}",
          e.toString());
      return new WebhookSink(null, "", 0, new long[0], metrics);
    }
  }

  public WebhookSink(URI uri, String auth, long timeoutMs, long[] backoffMs, Metrics metrics) {
    this.uri = uri;
    this.auth = auth;
    this.timeoutMs = timeoutMs;
    this.backoffMs = backoffMs;
    this.metrics = metrics;
    if (uri == null) {
      this.client = null;
    } else {
      this.client = HttpClient.newBuilder().connectTimeout(Duration.ofMillis(timeoutMs)).build();
      Thread drain = new Thread(this::drain, "kahshe-watch-webhook");
      drain.setDaemon(true);
      drain.start();
    }
  }

  @Override
  public String name() {
    return NAME;
  }

  @Override
  public boolean enabled() {
    return uri != null;
  }

  @Override
  public void deliver(byte[] alertJson) {
    if (uri == null) {
      return;
    }
    pending.incrementAndGet();
    if (!queue.offer(alertJson)) {
      pending.decrementAndGet();
      metrics.watchWebhookDropped.increment();
    }
  }

  /**
   * Blocks until every enqueued alert has finished delivery (success or exhausted retries) or the
   * deadline passes. CLI builds call this before JVM exit so queued alerts are not dropped.
   */
  @Override
  public void awaitDrain(long timeoutMs) {
    if (uri == null) {
      return;
    }
    long deadline = System.currentTimeMillis() + timeoutMs;
    while (pending.get() > 0 && System.currentTimeMillis() < deadline) {
      try {
        Thread.sleep(20);
      } catch (InterruptedException e) {
        Thread.currentThread().interrupt();
        return;
      }
    }
  }

  private void drain() {
    while (true) {
      byte[] payload;
      try {
        payload = queue.take();
      } catch (InterruptedException e) {
        Thread.currentThread().interrupt();
        return;
      }
      try {
        post(payload);
      } catch (Throwable t) {
        // a poison alert must never kill the drain thread
        metrics.watchWebhookFailures.increment();
        LOG.warn("webhook delivery failed after retries", t);
      } finally {
        pending.decrementAndGet();
      }
    }
  }

  private void post(byte[] payload) throws Exception {
    HttpRequest.Builder builder =
        HttpRequest.newBuilder(uri)
            .timeout(Duration.ofMillis(timeoutMs))
            .header("Content-Type", "application/json")
            .POST(HttpRequest.BodyPublishers.ofByteArray(payload));
    if (!auth.isBlank()) {
      builder.header("Authorization", auth);
    }
    HttpRequest request = builder.build();
    Exception last = null;
    for (int attempt = 0; attempt < 3; attempt++) {
      if (attempt > 0) {
        Thread.sleep(backoffMs[attempt - 1]);
      }
      try {
        HttpResponse<Void> response = client.send(request, HttpResponse.BodyHandlers.discarding());
        if (response.statusCode() / 100 == 2) {
          return;
        }
        last = new IOException("webhook returned " + response.statusCode());
      } catch (IOException e) {
        last = e;
      }
    }
    throw last;
  }
}
