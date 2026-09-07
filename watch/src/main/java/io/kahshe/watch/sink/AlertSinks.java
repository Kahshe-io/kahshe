package io.kahshe.watch.sink;

import io.kahshe.common.Metrics;
import java.util.Map;
import java.util.ServiceLoader;
import java.util.TreeMap;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import io.kahshe.watch.WatchConfig;

/**
 * Selects the one {@link AlertSink} this process delivers through, by name, over the providers
 * {@link ServiceLoader} finds on the classpath.
 *
 * <p>An unrecognised name fails startup rather than falling back to the default: a typo in
 * KAHSHE_WATCH_SINK would otherwise leave a watcher running and quietly delivering somewhere the
 * operator did not ask for. The failure message lists the names that were discovered.
 */
public final class AlertSinks {
  private static final Logger LOG = LoggerFactory.getLogger(AlertSinks.class);

  /** The sink used when KAHSHE_WATCH_SINK is unset. */
  public static final String DEFAULT = WebhookSink.NAME;

  private AlertSinks() {}

  /**
   * Builds the sink named by {@link WatchConfig#sink()}.
   *
   * @param config the watch configuration
   * @param metrics the process metrics, handed to whichever sink is built
   * @return the selected sink
   * @throws IllegalArgumentException if no discovered provider claims that name
   */
  public static AlertSink fromEnv(WatchConfig config, Metrics metrics) {
    String requested = config.sink() == null || config.sink().isBlank()
        ? DEFAULT
        : config.sink().trim();
    Map<String, AlertSinkProvider> discovered = discover();
    AlertSinkProvider provider = discovered.get(requested);
    if (provider == null) {
      throw new IllegalArgumentException("KAHSHE_WATCH_SINK=" + requested
          + " is not a known alert sink; discovered: " + discovered.keySet());
    }
    AlertSink sink = provider.create(config, metrics);
    LOG.info("alert sink: {} (enabled={}); discovered: {}", sink.name(), sink.enabled(),
        discovered.keySet());
    return sink;
  }

  /**
   * Every provider on the classpath, by name, sorted so the failure message and the startup log
   * read the same way twice. Package-private: tests assert on what discovery finds.
   *
   * @return the discovered providers keyed by {@link AlertSinkProvider#name()}
   */
  static Map<String, AlertSinkProvider> discover() {
    Map<String, AlertSinkProvider> byName = new TreeMap<>();
    for (AlertSinkProvider provider :
        ServiceLoader.load(AlertSinkProvider.class, AlertSinks.class.getClassLoader())) {
      AlertSinkProvider previous = byName.putIfAbsent(provider.name(), provider);
      if (previous != null) {
        LOG.warn("two providers claim the alert sink name {}: keeping {}, ignoring {}",
            provider.name(), previous.getClass().getName(), provider.getClass().getName());
      }
    }
    return byName;
  }
}
