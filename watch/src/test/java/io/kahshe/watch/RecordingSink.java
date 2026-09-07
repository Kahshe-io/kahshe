package io.kahshe.watch;

import io.kahshe.common.Metrics;
import java.nio.charset.StandardCharsets;
import java.util.List;
import java.util.concurrent.CopyOnWriteArrayList;
import io.kahshe.watch.sink.AlertSink;
import io.kahshe.watch.sink.AlertSinkProvider;
import io.kahshe.watch.sink.AlertSinks;

/**
 * A third {@link AlertSink}, in tests only: it keeps what it was handed. WatchEngine and
 * ReportPoller are exercised against it, so those tests hold the interface and nothing else --
 * were the seam a concrete webhook, they would not compile.
 *
 * <p>Its {@link Provider} is registered in watch/src/test/resources/META-INF/services and exists
 * to prove the discovery claim: a provider from a source set the main services file knows nothing
 * about is found and selectable with no edit to AlertSinks and none to the app.
 */
public final class RecordingSink implements AlertSink {
  static final String NAME = "recording";

  private final List<byte[]> delivered = new CopyOnWriteArrayList<>();

  /** Announces the recording sink through the test classpath's services file. */
  public static final class Provider implements AlertSinkProvider {
    @Override
    public String name() {
      return NAME;
    }

    @Override
    public AlertSink create(WatchConfig config, Metrics metrics) {
      return new RecordingSink();
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
    delivered.add(alertJson);
  }

  /** Nothing is queued: every delivery already landed in the list. */
  @Override
  public void awaitDrain(long timeoutMs) {}

  public int count() {
    return delivered.size();
  }

  public String json(int index) {
    return new String(delivered.get(index), StandardCharsets.UTF_8);
  }
}
