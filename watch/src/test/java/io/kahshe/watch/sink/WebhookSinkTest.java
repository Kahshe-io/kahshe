package io.kahshe.watch.sink;

import static org.junit.jupiter.api.Assertions.assertEquals;

import com.sun.net.httpserver.HttpServer;
import io.kahshe.common.Metrics;
import java.net.InetSocketAddress;
import java.net.URI;
import java.nio.charset.StandardCharsets;
import java.util.concurrent.atomic.AtomicInteger;
import org.junit.jupiter.api.Test;

class WebhookSinkTest {
  @Test
  void drainThreadSurvivesUnresolvableEndpoint() throws Exception {
    Metrics metrics = new Metrics();
    // .invalid TLD: guaranteed unresolvable; tiny backoffs keep the test fast
    AlertSink sender = new WebhookSink(
        URI.create("http://kahshe-nowhere.invalid:9/hook"), "Bearer x", 500,
        new long[] {10, 10}, metrics);
    sender.deliver("{\"n\":1}".getBytes(java.nio.charset.StandardCharsets.UTF_8));
    sender.deliver("{\"n\":2}".getBytes(java.nio.charset.StandardCharsets.UTF_8));
    // both payloads fail all attempts; two failure counts prove the thread survived the first
    long deadline = System.currentTimeMillis() + 30_000;
    while (metrics.watchWebhookFailures.sum() < 2 && System.currentTimeMillis() < deadline) {
      Thread.sleep(20);
    }
    assertEquals(2, metrics.watchWebhookFailures.sum());
  }

  @Test
  void retryRecoversFromTransient5xx() throws Exception {
    Metrics metrics = new Metrics();
    AtomicInteger requests = new AtomicInteger();
    HttpServer server = HttpServer.create(new InetSocketAddress("127.0.0.1", 0), 0);
    server.createContext("/hook", exchange -> {
      int n = requests.incrementAndGet();
      exchange.getRequestBody().readAllBytes();
      exchange.sendResponseHeaders(n == 1 ? 503 : 200, -1);
      exchange.close();
    });
    server.start();
    try {
      AlertSink sender = new WebhookSink(
          URI.create("http://127.0.0.1:" + server.getAddress().getPort() + "/hook"), "", 2_000,
          new long[] {10, 10}, metrics);
      sender.deliver("{\"n\":1}".getBytes(StandardCharsets.UTF_8));
      sender.awaitDrain(20_000);
      assertEquals(2, requests.get());
      assertEquals(0, metrics.watchWebhookFailures.sum());
    } finally {
      server.stop(0);
    }
  }

  @Test
  void awaitDrainFlushesPendingAlert() throws Exception {
    Metrics metrics = new Metrics();
    AtomicInteger requests = new AtomicInteger();
    HttpServer server = HttpServer.create(new InetSocketAddress("127.0.0.1", 0), 0);
    server.createContext("/hook", exchange -> {
      requests.incrementAndGet();
      exchange.getRequestBody().readAllBytes();
      exchange.sendResponseHeaders(200, -1);
      exchange.close();
    });
    server.start();
    try {
      AlertSink sender = new WebhookSink(
          URI.create("http://127.0.0.1:" + server.getAddress().getPort() + "/hook"), "", 2_000,
          new long[] {10, 10}, metrics);
      sender.deliver("{\"n\":1}".getBytes(StandardCharsets.UTF_8));
      sender.awaitDrain(20_000);
      // the delivery must have happened-before awaitDrain returned
      assertEquals(1, requests.get());
      assertEquals(0, metrics.watchWebhookFailures.sum());
    } finally {
      server.stop(0);
    }
  }
}
