package io.kahshe.proxy.http;

import io.kahshe.common.Metrics;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
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
import java.util.concurrent.Executors;
import java.util.concurrent.atomic.AtomicBoolean;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

/**
 * The admin port's bearer check, {@code KAHSHE_ADMIN_TOKEN}.
 *
 * <p>Against a real socket, like {@link AdminHandlerTest}, because the header is read off the
 * exchange and a refusal is a response with a challenge on it: {@link AdminAuth#permits} tested
 * alone would pass with the handler never calling it.
 */
class AdminAuthTest {
  private static final String TOKEN = "s3cret-scrape-token";

  private HttpServer backend;
  private HttpServer admin;
  private AdminHandler handler;
  private String base;
  private final Metrics metrics = new Metrics();
  private final HttpClient client =
      HttpClient.newBuilder().version(HttpClient.Version.HTTP_1_1).build();

  @BeforeEach
  void setUp() throws Exception {
    backend = HttpServer.create(new InetSocketAddress("127.0.0.1", 0), 0);
    backend.createContext(
        "/",
        exchange -> {
          byte[] body = "{}".getBytes(StandardCharsets.UTF_8);
          exchange.sendResponseHeaders(200, body.length);
          try (OutputStream out = exchange.getResponseBody()) {
            out.write(body);
          }
          exchange.close();
        });
    backend.start();
  }

  /** Started per test, because the token is fixed at construction. */
  private void startAdmin(String token) throws Exception {
    handler =
        new AdminHandler(
            metrics,
            new Forwarder("http://127.0.0.1:" + backend.getAddress().getPort(), 3_000),
            new AtomicBoolean(false),
            AdminAuth.fromToken(token, metrics));
    admin = HttpServer.create(new InetSocketAddress("127.0.0.1", 0), 0);
    admin.createContext("/", handler);
    admin.setExecutor(Executors.newSingleThreadExecutor());
    admin.start();
    base = "http://127.0.0.1:" + admin.getAddress().getPort();
  }

  @AfterEach
  void tearDown() {
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

  @Test
  void metricsWithoutATokenIs401AndCounted() throws Exception {
    startAdmin(TOKEN);
    HttpResponse<String> response = get("/metrics", null);
    assertEquals(401, response.statusCode());
    assertEquals(
        AdminAuth.CHALLENGE, response.headers().firstValue("WWW-Authenticate").orElse(null));
    assertEquals(1, metrics.adminAuthRejected.sum());
    // and the refusal reaches the scrape, which is where an operator would look for it
    assertTrue(
        get("/metrics", TOKEN).body().contains("kahshe_admin_auth_rejected_total 1\n"),
        "the refusal was counted but never emitted");
  }

  @Test
  void metricsWithTheRightTokenIs200() throws Exception {
    startAdmin(TOKEN);
    HttpResponse<String> response = get("/metrics", TOKEN);
    assertEquals(200, response.statusCode());
    assertTrue(response.body().contains("kahshe_"), "expected kahshe metrics: " + response.body());
    assertEquals(0, metrics.adminAuthRejected.sum());
  }

  @Test
  void metricsWithAWrongTokenIs401AndCounted() throws Exception {
    startAdmin(TOKEN);
    assertEquals(401, get("/metrics", TOKEN + "x").statusCode(), "a longer token");
    assertEquals(401, get("/metrics", TOKEN.substring(1)).statusCode(), "a shorter token");
    assertEquals(401, get("/metrics", TOKEN.toUpperCase()).statusCode(), "a token differing in case");
    assertEquals(3, metrics.adminAuthRejected.sum());
  }

  /**
   * A kubelet cannot easily carry a bearer, so a token that gated the probes would take a healthy
   * pod out of service the day the token rotated. Both stay open with a token configured.
   */
  @Test
  void probesAnswerWithoutATokenWhenOneIsConfigured() throws Exception {
    startAdmin(TOKEN);
    assertEquals(200, get("/healthz", null).statusCode());
    awaitReady();
    assertEquals(0, metrics.adminAuthRejected.sum(), "a probe must not count as a refusal");
  }

  /** The port is open, as it always was, until an operator sets a token. */
  @Test
  void metricsIsOpenWhenNoTokenIsConfigured() throws Exception {
    startAdmin(null);
    assertEquals(200, get("/metrics", null).statusCode());
    assertEquals(0, metrics.adminAuthRejected.sum());
    assertFalse(AdminAuth.fromToken(" \n", metrics).enabled(), "a blank variable is not a token");
  }

  /** An unauthenticated caller learns nothing about what the port serves. */
  @Test
  void anUnknownPathIsRefusedBeforeItIsNotFound() throws Exception {
    startAdmin(TOKEN);
    assertEquals(401, get("/nope", null).statusCode());
    assertEquals(404, get("/nope", TOKEN).statusCode());
  }

  private void awaitReady() throws Exception {
    long deadline = System.nanoTime() + Duration.ofSeconds(15).toNanos();
    while (System.nanoTime() < deadline) {
      if (get("/readyz", null).statusCode() == 200) {
        return;
      }
      Thread.sleep(20);
    }
    throw new AssertionError("/readyz never turned 200 without a token against a backend that answers");
  }

  private HttpResponse<String> get(String path, String bearer) throws Exception {
    HttpRequest.Builder request =
        HttpRequest.newBuilder(URI.create(base + path)).timeout(Duration.ofSeconds(20));
    if (bearer != null) {
      request.header("Authorization", "Bearer " + bearer);
    }
    return client.send(request.build(), HttpResponse.BodyHandlers.ofString());
  }
}
