package io.kahshe.watch.sink;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertInstanceOf;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

import io.kahshe.common.Metrics;
import org.junit.jupiter.api.Test;
import io.kahshe.watch.RecordingSink;
import io.kahshe.watch.WatchConfig;

/**
 * The seam is a seam: two built-in sinks are discovered through {@link java.util.ServiceLoader},
 * a third registered only on the test classpath is discovered the same way, and a name nobody
 * claims stops the process instead of quietly delivering somewhere else.
 *
 * <p>Verified red by deleting the null-provider check in {@link AlertSinks#fromEnv}: the unknown
 * name then reached {@code provider.create} and the test failed with a NullPointerException in
 * place of the message naming what was discovered.
 */
class AlertSinksTest {

  private static WatchConfig config(String sink) {
    return new WatchConfig("", sink, "", "", 1_000, 60_000, "iceberg", false, true, 2);
  }

  @Test
  void bothBuiltInsAreDiscovered() {
    assertTrue(AlertSinks.discover().containsKey("webhook"), "the default sink must be findable");
    assertTrue(AlertSinks.discover().containsKey("log"), "the second built-in must be findable");
    Metrics metrics = new Metrics();
    assertInstanceOf(WebhookSink.class, AlertSinks.fromEnv(config("webhook"), metrics));
    assertInstanceOf(LogSink.class, AlertSinks.fromEnv(config("log"), metrics));
  }

  @Test
  void blankNameIsTheWebhookDefault() {
    AlertSink sink = AlertSinks.fromEnv(config(""), new Metrics());
    assertEquals("webhook", sink.name());
    assertInstanceOf(WebhookSink.class, sink);
  }

  /**
   * A sink this module's main services file has never heard of. Registered in
   * watch/src/test/resources/META-INF/services, so selecting it exercises exactly the path a
   * third-party sink on the classpath would take.
   */
  @Test
  void aProviderRegisteredElsewhereOnTheClasspathIsDiscoveredToo() {
    assertTrue(AlertSinks.discover().containsKey("recording"),
        "discovery reads every services file on the classpath, not a list in AlertSinks");
    AlertSink sink = AlertSinks.fromEnv(config("recording"), new Metrics());
    assertInstanceOf(RecordingSink.class, sink);
    assertEquals("recording", sink.name());
  }

  @Test
  void unknownNameFailsStartupAndListsWhatWasFound() {
    IllegalArgumentException e = assertThrows(IllegalArgumentException.class,
        () -> AlertSinks.fromEnv(config("webook"), new Metrics()));
    assertTrue(e.getMessage().contains("webook"), "say which name was asked for: " + e.getMessage());
    assertTrue(e.getMessage().contains("webhook") && e.getMessage().contains("log"),
        "the answer to an unknown sink is the set of known ones: " + e.getMessage());
  }

  @Test
  void theLogSinkIsAlwaysOnAndHasNothingToDrain() {
    AlertSink sink = AlertSinks.fromEnv(config("log"), new Metrics());
    assertTrue(sink.enabled(), "a dev stack asked for the log sink; it cannot silently be off");
    sink.deliver("{\"rule\":{\"id\":\"r0\"}}".getBytes(java.nio.charset.StandardCharsets.UTF_8));
    sink.awaitDrain(0); // returns immediately: no queue, no thread
  }
}
