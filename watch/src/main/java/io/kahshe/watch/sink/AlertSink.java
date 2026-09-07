package io.kahshe.watch.sink;

/**
 * The one way an alert leaves the process. A watcher holds a sink and knows nothing else about
 * delivery: {@link io.kahshe.watch.WatchEngine} and {@link io.kahshe.watch.ReportPoller} hand it
 * bytes and are done.
 *
 * <p>Which implementation runs is chosen once at startup by {@link AlertSinks#fromEnv} from
 * KAHSHE_WATCH_SINK, over the providers {@link java.util.ServiceLoader} finds. Delivery is
 * best-effort: a sink may drop under back-pressure, and says so by counting, never by throwing
 * at its caller — the caller is an index build, and alerting is advisory.
 */
public interface AlertSink {
  /** The name this sink is selected by, matching its provider's. */
  String name();

  /**
   * Whether alerts actually go anywhere. A disabled sink still accepts deliveries and discards
   * them, because alerts log and count in the engine regardless of where they are sent.
   */
  boolean enabled();

  /**
   * Hands one alert payload over for delivery. Must not throw, and must not block the caller on
   * the network.
   *
   * @param alertJson the serialized alert, UTF-8 JSON
   */
  void deliver(byte[] alertJson);

  /**
   * Blocks until everything already handed over has been delivered (or given up on), or the
   * deadline passes. CLI builds call this before JVM exit; a synchronous sink returns at once.
   *
   * @param timeoutMs how long to wait, in milliseconds
   */
  void awaitDrain(long timeoutMs);
}
