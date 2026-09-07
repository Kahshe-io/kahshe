package io.kahshe.watch.sink;

import io.kahshe.common.Metrics;
import java.nio.charset.StandardCharsets;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import io.kahshe.watch.WatchConfig;

/**
 * Writes each alert to the log as one INFO line carrying the payload JSON. Always enabled,
 * nothing queued and so nothing to drain.
 *
 * <p>For a dev stack that wants to see the alerts it raises without standing up a webhook
 * receiver. Not a production sink: the log is the only record, so an alert lives exactly as long
 * as the log does.
 */
public final class LogSink implements AlertSink {
  private static final Logger LOG = LoggerFactory.getLogger(LogSink.class);

  /** The name this sink is selected by. */
  public static final String NAME = "log";

  /** Announces the log sink to {@link java.util.ServiceLoader}. */
  public static final class Provider implements AlertSinkProvider {
    @Override
    public String name() {
      return NAME;
    }

    @Override
    public AlertSink create(WatchConfig config, Metrics metrics) {
      return new LogSink();
    }
  }

  @Override
  public String name() {
    return NAME;
  }

  @Override
  public boolean enabled() {
    return true;
  }

  @Override
  public void deliver(byte[] alertJson) {
    LOG.info("WATCH ALERT PAYLOAD {}", new String(alertJson, StandardCharsets.UTF_8));
  }

  /** Nothing is queued, so there is nothing to wait for. */
  @Override
  public void awaitDrain(long timeoutMs) {}
}
