package io.kahshe.proxy.http;

import io.kahshe.common.Metrics;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertTrue;

import com.sun.net.httpserver.HttpServer;
import java.io.OutputStream;
import java.net.InetSocketAddress;
import java.net.URI;
import java.net.http.HttpClient;
import java.net.http.HttpRequest;
import java.net.http.HttpResponse;
import java.nio.charset.StandardCharsets;
import java.time.Duration;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicLong;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

/**
 * The admin port: {@code /healthz}, {@code /readyz} and {@code /metrics}.
 *
 * <p>The starvation these guard against is invisible on reading the handler: nothing there says
 * the endpoints share one thread, so a backend call on any of them would hold up the rest.
 *
 * <p>Everything here runs against a real socket with the executor production uses — a single
 * thread. That is deliberate: widening the pool would make the starvation test pass without the
 * handler changing, and the point is not that {@code /healthz} is fast when threads are plentiful,
 * it is that no admin request waits on the backend at all.
 */
class AdminHandlerTest {
  private HttpServer backend;
  private HttpServer admin;
  private AdminHandler handler;
  private String base;

  /** True while the stub backend must hang rather than answer, holding a probe in flight. */
  private final AtomicBoolean backendStalls = new AtomicBoolean(true);

  /** Released to let a stalled backend request through; also released on teardown. */
  private final CountDownLatch backendMayAnswer = new CountDownLatch(1);

  /** Counted down once the backend has actually received a probe, so tests need not guess. */
  private final CountDownLatch backendWasProbed = new CountDownLatch(1);

  /** Frozen, so the staleness bound can be crossed without a test that sleeps for 30 s. */
  private final AtomicLong now = new AtomicLong(1_700_000_000_000L);

  private final HttpClient client =
      HttpClient.newBuilder().version(HttpClient.Version.HTTP_1_1).build();

  @BeforeEach
  void setUp() throws Exception {
    backend = HttpServer.create(new InetSocketAddress("127.0.0.1", 0), 0);
    backend.createContext(
        "/",
        exchange -> {
          backendWasProbed.countDown();
          if (backendStalls.get()) {
            try {
              backendMayAnswer.await(20, TimeUnit.SECONDS);
            } catch (InterruptedException e) {
              Thread.currentThread().interrupt();
            }
          }
          byte[] body = "{}".getBytes(StandardCharsets.UTF_8);
          exchange.sendResponseHeaders(200, body.length);
          try (OutputStream out = exchange.getResponseBody()) {
            out.write(body);
          }
          exchange.close();
        });
    backend.setExecutor(Executors.newFixedThreadPool(2));
    backend.start();
  }

  /** Started per test rather than in {@code setUp} because the backend's state must be set first. */
  private void startAdmin() throws Exception {
    startAdmin(5_000);
  }

  private void startAdmin(long refreshMs) throws Exception {
    handler =
        new AdminHandler(
            new Metrics(),
            new Forwarder("http://127.0.0.1:" + backend.getAddress().getPort(), 3_000),
            new AtomicBoolean(false),
            now::get,
            refreshMs);
    admin = HttpServer.create(new InetSocketAddress("127.0.0.1", 0), 0);
    admin.createContext("/", handler);
    admin.setExecutor(Executors.newSingleThreadExecutor()); // production's executor, on purpose
    admin.start();
    base = "http://127.0.0.1:" + admin.getAddress().getPort();
  }

  @AfterEach
  void tearDown() {
    backendMayAnswer.countDown();
    if (handler != null) {
      handler.close();
    }
    if (admin != null) {
      admin.stop(0);
    }
    if (backend != null) {
      backend.stop(0);
    }
  }

  /**
   * The defect this file exists for: no admin request may wait on the backend.
   *
   * <p>A {@code /readyz} that calls the backend inline runs on the admin server's single thread
   * with a 3 s timeout. A backend that stops answering then holds that one thread for 3 s at a
   * time, and a {@code /healthz} arriving behind it is answered that late — against a liveness
   * probe that allows 3 s. The admin port exists so a saturated data plane cannot cause a restart
   * storm; an inline probe makes it cause one instead, during exactly the backend outage that
   * leaves every replica blocked on the same thing.
   *
   * <p>Both halves are asserted in sequence rather than by racing two requests, because a race
   * between them would be won by whichever thread the OS scheduled first and would pass by luck.
   * The first assertion is the load-bearing one: the readiness path itself does not block. The
   * second follows from it — on a single-threaded executor a request that returns in microseconds
   * cannot delay the next — and is asserted anyway because it is the symptom that would page.
   *
   * <p>Verified by breaking it: restoring the inline {@code probe()} call inside
   * {@code backendReachable} makes {@code /readyz} take about 3 000 ms here and this fails on the
   * first assertion.
   */
  @Test
  void neitherProbeWaitsOnABackendThatHasStoppedAnswering() throws Exception {
    startAdmin();
    assertTrue(
        backendWasProbed.await(5, TimeUnit.SECONDS),
        "the backend was never probed, so nothing here would have blocked either way");

    long startedAt = System.nanoTime();
    HttpResponse<String> ready = get("/readyz");
    long readyMs = (System.nanoTime() - startedAt) / 1_000_000;

    startedAt = System.nanoTime();
    HttpResponse<String> health = get("/healthz");
    long healthMs = (System.nanoTime() - startedAt) / 1_000_000;

    assertEquals(503, ready.statusCode(), "a backend that never answered is not ready");
    assertTrue(
        readyMs < 1_000,
        "/readyz waited " + readyMs + " ms on a hung backend; it holds the admin thread that long");
    assertEquals(200, health.statusCode());
    assertTrue(
        healthMs < 1_000,
        "/healthz waited " + healthMs + " ms behind /readyz; a liveness probe gives it 3 s");
  }

  /**
   * The control that makes the test above mean anything.
   *
   * <p>Moving the probe off the request thread would also be satisfied by a {@code /readyz} that
   * answered 503 forever and never looked at the backend at all, and that would pass the starvation
   * test while breaking readiness completely. So: no verdict means not ready, and the verdict the
   * background probe publishes has to arrive and be believed.
   */
  @Test
  void readinessIsRefusedUntilTheProbeHasObservedTheBackendAndGrantedAfter() throws Exception {
    startAdmin(); // the probe this starts blocks inside the stub backend, so no verdict exists yet
    assertTrue(backendWasProbed.await(5, TimeUnit.SECONDS), "the backend was never probed");
    assertEquals(503, get("/readyz").statusCode(), "not ready before the backend has been observed");

    backendStalls.set(false);
    backendMayAnswer.countDown(); // let that same probe complete and publish
    awaitReady();
  }

  /**
   * A wedged prober must not read as a healthy one.
   *
   * <p>Answering from a published verdict means nothing detects a prober stuck inside
   * {@code forward}: it simply stops publishing, and a {@code ready} recorded before it stuck would
   * be served indefinitely with nothing checking the backend. A verdict older than
   * {@code READY_STALE_MS} is therefore not reported, which fails the same way an unreachable
   * backend already did.
   *
   * <p>Verified by breaking it: dropping the {@code age &lt; READY_STALE_MS} clause from
   * {@code backendReachable} makes this return 200.
   */
  @Test
  void readinessIsNotServedFromAVerdictTooOldToTrust() throws Exception {
    backendStalls.set(false);
    startAdmin();
    awaitReady();

    backendStalls.set(true); // the refresh the next request kicks cannot land
    now.addAndGet(31_000); // one second past READY_STALE_MS

    assertEquals(503, get("/readyz").statusCode(), "a verdict this old must not be reported");
  }

  /**
   * The verdict must be refreshed by the prober, not by someone happening to ask.
   *
   * <p>A lazy refresh — {@code /readyz} kicking a probe when it finds the verdict older than the
   * TTL — passes any test that polls {@code /readyz} in a loop. A deployment that gates readiness
   * on {@code /healthz} never polls {@code /readyz}, so the first call after an idle gap answers
   * 503 against a healthy backend: a lazily refreshed verdict cannot tell a wedged prober from
   * nobody having asked. A readiness probe with {@code periodSeconds} above {@code READY_STALE_MS}
   * would never report ready at all.
   *
   * <p>So: age the verdict past the staleness bound and then DO NOT ask, which is the whole point.
   * A scheduled prober republishes anyway and the next call is 200.
   *
   * <p>Verified by breaking it: replacing the {@code scheduleWithFixedDelay} with a single
   * {@code execute} and a lazy kick from {@code backendReachable} returns 503 here.
   */
  @Test
  void theVerdictIsRefreshedOnAScheduleRatherThanWhenSomeoneAsks() throws Exception {
    backendStalls.set(false);
    startAdmin(40); // production refreshes every 5 s; this test should not take 5 s to find out
    awaitReady();

    now.addAndGet(31_000); // past READY_STALE_MS, with nothing calling /readyz to trigger a refresh
    Thread.sleep(500); // several refresh intervals

    assertEquals(
        200,
        get("/readyz").statusCode(),
        "the prober did not republish on its own, so readiness decayed while nobody was asking");
  }

  @Test
  void metricsIsServedAndUnknownAdminPathsAre404() throws Exception {
    startAdmin();
    HttpResponse<String> metrics = get("/metrics");
    assertEquals(200, metrics.statusCode());
    assertTrue(metrics.body().contains("kahshe_"), "expected kahshe metrics, got: " + metrics.body());
    assertEquals(404, get("/nope").statusCode());
  }

  private void awaitReady() throws Exception {
    long deadline = System.nanoTime() + Duration.ofSeconds(15).toNanos();
    while (System.nanoTime() < deadline) {
      if (get("/readyz").statusCode() == 200) {
        return;
      }
      Thread.sleep(20);
    }
    throw new AssertionError("/readyz never turned 200 against a backend that answers");
  }

  private HttpResponse<String> get(String path) throws Exception {
    return client.send(
        HttpRequest.newBuilder(URI.create(base + path)).timeout(Duration.ofSeconds(20)).build(),
        HttpResponse.BodyHandlers.ofString());
  }
}
