package io.kahshe.watch.sink;

import io.kahshe.common.Metrics;
import io.kahshe.watch.WatchConfig;

/**
 * How an {@link AlertSink} implementation announces itself. Implementations are found through
 * {@link java.util.ServiceLoader}, so a sink written outside this module joins by putting its
 * provider on the classpath with a META-INF/services/io.kahshe.watch.sink.AlertSinkProvider
 * entry — no edit here, and none in the app.
 *
 * <p>Every provider on the classpath is constructed just to be asked its {@link #name()}, so a
 * constructor must do nothing but exist; {@link #create} is called once, and only for the name
 * that was selected.
 */
public interface AlertSinkProvider {
  /** The KAHSHE_WATCH_SINK value this provider answers to. Unique across the classpath. */
  String name();

  /**
   * Builds the sink. Called once at startup, on the selected provider only.
   *
   * @param config the watch configuration, including whatever this sink reads from it
   * @param metrics the process metrics, for a sink that counts drops or failures
   * @return the sink, never null
   */
  AlertSink create(WatchConfig config, Metrics metrics);
}
