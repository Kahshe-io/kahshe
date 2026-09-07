package io.kahshe.watch.sink;

import io.kahshe.common.Metrics;
import io.kahshe.watch.WatchConfig;

/**
 * Delivers nothing, deliberately.
 *
 * <p>For a process that EVALUATES but must not deliver: an indexer fleet member rides its own
 * builds so the alerts land in the build report it writes, and one watcher elsewhere delivers
 * from those reports, keeping alert dedup in a single process. Alerts still count on
 * {@code kahshe_watch_alerts_total} and still log, so a fleet member that is evaluating is not
 * silent — only its delivery is.
 *
 * <p>Not the same as an unset sink: {@code KAHSHE_WATCH_SINK} unset means the webhook, and a
 * webhook with no URL logs an error. This is the way to say "no delivery from here" and be read
 * as having meant it.
 */
public final class NoneSink implements AlertSink {

  /** The name this sink is selected by. */
  public static final String NAME = "none";

  /** Announces the none sink to {@link java.util.ServiceLoader}. */
  public static final class Provider implements AlertSinkProvider {
    @Override
    public String name() {
      return NAME;
    }

    @Override
    public AlertSink create(WatchConfig config, Metrics metrics) {
      return new NoneSink();
    }
  }

  @Override
  public String name() {
    return NAME;
  }

  /** False: nothing is delivered, and the startup log says so. */
  @Override
  public boolean enabled() {
    return false;
  }

  @Override
  public void deliver(byte[] alertJson) {}

  /** Nothing is queued, so there is nothing to wait for. */
  @Override
  public void awaitDrain(long timeoutMs) {}
}
