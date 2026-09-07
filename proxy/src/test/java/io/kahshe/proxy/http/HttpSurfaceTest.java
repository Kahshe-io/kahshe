package io.kahshe.proxy.http;

import io.kahshe.common.Metrics;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNotEquals;
import static org.junit.jupiter.api.Assertions.assertTrue;

import com.sun.net.httpserver.HttpServer;
import java.io.OutputStream;
import java.net.InetSocketAddress;
import java.net.URI;
import java.net.http.HttpClient;
import java.net.http.HttpRequest;
import java.net.http.HttpResponse;
import java.nio.charset.StandardCharsets;
import java.util.List;
import java.util.concurrent.atomic.AtomicReference;
import org.apache.iceberg.catalog.Namespace;
import org.apache.iceberg.catalog.TableIdentifier;
import org.apache.iceberg.catalog.TableIdentifierParser;
import org.apache.iceberg.rest.requests.CommitTransactionRequest;
import org.apache.iceberg.rest.requests.CommitTransactionRequestParser;
import org.apache.iceberg.rest.requests.UpdateTableRequest;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import io.kahshe.proxy.ProxyConfig;
import io.kahshe.proxy.TestConfigs;
import io.kahshe.proxy.catalog.BackendCatalogs;
import io.kahshe.proxy.catalog.MutatedTables;
import io.kahshe.proxy.plan.CountRoutes;
import io.kahshe.proxy.plan.PlanRoutes;
import io.kahshe.proxy.plan.PlanService;

/**
 * The externally reachable HTTP surface: {@code KahsheHandler}, {@code Forwarder},
 * {@code AuthGate}, {@code PlanRoutes} and {@code CountRoutes}.
 *
 * <p>These run the real dispatcher against a real stub backend over a real socket, because the
 * properties they pin live in the layer between the socket and the handler — header handling,
 * body framing, status relay — which no unit test of the handler's internals would reach.
 */
class HttpSurfaceTest {
  private HttpServer backend;
  private HttpServer proxy;
  private final AtomicReference<String> lastBackendPath = new AtomicReference<>();
  private final AtomicReference<Integer> backendStatus = new AtomicReference<>(200);
  private String base;
  private final Metrics metrics = new Metrics();
  private io.kahshe.proxy.catalog.BackendCatalogs catalogs;
  private final AtomicReference<String> backendBody = new AtomicReference<>("{\"ok\":true}");

  @BeforeEach
  void setUp() throws Exception {
    backend = HttpServer.create(new InetSocketAddress("127.0.0.1", 0), 0);
    backend.createContext(
        "/",
        exchange -> {
          lastBackendPath.set(exchange.getRequestURI().getPath());
          exchange.getRequestBody().readAllBytes();
          byte[] body = backendBody.get().getBytes(StandardCharsets.UTF_8);
          exchange.getResponseHeaders().set("Content-Type", "application/json");
          exchange.sendResponseHeaders(backendStatus.get(), body.length);
          try (OutputStream out = exchange.getResponseBody()) {
            out.write(body);
          }
        });
    backend.start();

    io.kahshe.proxy.ProxyConfig config =
        TestConfigs.proxy("http://127.0.0.1:" + backend.getAddress().getPort(), 4096);
    proxy = HttpServer.create(new InetSocketAddress("127.0.0.1", 0), 0);
    catalogs = new io.kahshe.proxy.catalog.BackendCatalogs(config);
    proxy.createContext("/", TestConfigs.handler(config, metrics, catalogs));
    proxy.setExecutor(java.util.concurrent.Executors.newFixedThreadPool(4));
    proxy.start();
    base = "http://127.0.0.1:" + proxy.getAddress().getPort();
  }

  @AfterEach
  void tearDown() {
    if (proxy != null) {
      proxy.stop(0);
    }
    if (backend != null) {
      backend.stop(0);
    }
  }

  private HttpResponse<String> send(HttpRequest request) throws Exception {
    return HttpClient.newHttpClient().send(request, HttpResponse.BodyHandlers.ofString());
  }

  /**
   * The body cap must bound what is READ, not what the client claims.
   *
   * <p>A chunked request carries no Content-Length, so the declared length parses as 0 and a
   * header-only check passes. A {@code readAllBytes()} behind that check then buffers the entire
   * stream. This request reaches the passthrough path, which is BEFORE any authorization, so an
   * unauthenticated client can exhaust the heap across every worker thread.
   *
   * <p>Verified by breaking it: restoring {@code exchange.getRequestBody().readAllBytes()} makes
   * this return 200 — the oversized body is accepted and forwarded.
   */
  @Test
  void anOversizedChunkedBodyIsRefusedEvenThoughItDeclaresNoLength() throws Exception {
    byte[] oversized = new byte[64 * 1024]; // cap is 4096
    java.util.Arrays.fill(oversized, (byte) 'x');
    // a streaming publisher of unknown length makes the JDK client send Transfer-Encoding: chunked
    HttpRequest request =
        HttpRequest.newBuilder(URI.create(base + "/v1/config"))
            .POST(
                HttpRequest.BodyPublishers.ofInputStream(
                    () -> new java.io.ByteArrayInputStream(oversized)))
            .build();

    HttpResponse<String> response = send(request);
    assertEquals(
        413,
        response.statusCode(),
        "an oversized chunked body was accepted; the cap checks the declared Content-Length, "
            + "which a chunked request does not send, so nothing bounded the read");
    assertTrue(response.body().contains("PayloadTooLarge"), response.body());
  }

  /** The declared-length fast path must still reject before reading anything. */
  @Test
  void anOversizedDeclaredBodyIsRefusedUpFront() throws Exception {
    byte[] oversized = new byte[64 * 1024];
    HttpResponse<String> response =
        send(
            HttpRequest.newBuilder(URI.create(base + "/v1/config"))
                .POST(HttpRequest.BodyPublishers.ofByteArray(oversized))
                .build());
    assertEquals(413, response.statusCode());
  }

  /** A body inside the cap still goes through, so the guard is not simply refusing everything. */
  @Test
  void aBodyWithinTheCapIsForwarded() throws Exception {
    HttpResponse<String> response =
        send(
            HttpRequest.newBuilder(URI.create(base + "/v1/namespaces/logs/tables/events"))
                .POST(HttpRequest.BodyPublishers.ofString("{\"small\":true}"))
                .build());
    assertEquals(200, response.statusCode(), response.body());
    assertEquals("/v1/namespaces/logs/tables/events", lastBackendPath.get());
  }

  /**
   * {@code Expect: 100-continue} must not break the forward.
   *
   * <p>The JDK's HttpClient treats {@code expect} as a restricted header and throws
   * {@code IllegalArgumentException} if it is set, while the JDK's HttpServer answers the
   * continuation itself and still shows the header to the handler. So copying it through turned
   * every such request into a 500 — and curl sends it by default for bodies over roughly 1 KB,
   * which means ordinary {@code curl -d @file} usage was broken on every endpoint.
   *
   * <p>Verified by breaking it: removing {@code "expect"} from {@code SKIP_REQUEST_HEADERS} makes
   * this return 500.
   */
  @Test
  void anExpectContinueHeaderDoesNotBreakTheForward() throws Exception {
    HttpResponse<String> response =
        send(
            HttpRequest.newBuilder(URI.create(base + "/v1/namespaces/logs/tables/events"))
                .expectContinue(true)
                .POST(HttpRequest.BodyPublishers.ofString("{\"payload\":\"" + "y".repeat(2000) + "\"}"))
                .build());
    assertNotEquals(
        500,
        response.statusCode(),
        "Expect: 100-continue produced a 500; the header is restricted by the JDK HttpClient and "
            + "must not be copied to the backend request");
    assertEquals(200, response.statusCode(), response.body());
  }

  /** The proxy relays the backend's status verbatim rather than reinterpreting it. */
  @Test
  void aBackendErrorStatusIsRelayedVerbatim() throws Exception {
    backendStatus.set(503);
    HttpResponse<String> response =
        send(HttpRequest.newBuilder(URI.create(base + "/v1/config")).GET().build());
    assertEquals(503, response.statusCode(), "a backend 503 must not be rewritten");
  }

  /**
   * A backend brownout must not be remembered as a credential failure.
   *
   * <p>The auth gate forwards a loadTable under the caller's own token and caches the verdict.
   * Treating ANY non-2xx as a denial, and replaying every cached denial as a synthesized 401,
   * turns a one-second backend 503 into two seconds of "not authorized" for every caller that hits
   * it. Engines treat 401 as an invalid credential and abort or re-authenticate; a 503 is
   * something they retry. The distinction is the whole point.
   *
   * <p>Verified by breaking it: caching every non-2xx, or replaying a fixed 401, makes the second
   * request below return 401.
   */
  @Test
  void aBackendBrownoutIsNotCachedAsADenial() throws Exception {
    backendStatus.set(503);
    HttpRequest count =
        HttpRequest.newBuilder(URI.create(base + "/kahshe/v1/lake/namespaces/logs/tables/events/_count"))
            .header("Authorization", "Bearer test-token")
            .POST(HttpRequest.BodyPublishers.ofString("{\"column\":\"msg\",\"term\":\"x\"}"))
            .build();

    assertEquals(503, send(count).statusCode(), "the first response should relay the backend's 503");
    assertEquals(
        503,
        send(count).statusCode(),
        "a backend 503 was cached as a denial and replayed as 401; engines read that as a bad "
            + "credential and stop retrying instead of waiting out the brownout");
  }

  /** A genuine 403 IS cached, so a bad-token storm still cannot amplify into the backend. */
  @Test
  void agenuineDenialIsStillCachedAndReplayedWithItsOwnStatus() throws Exception {
    backendStatus.set(403);
    HttpRequest count =
        HttpRequest.newBuilder(URI.create(base + "/kahshe/v1/lake/namespaces/logs/tables/events/_count"))
            .header("Authorization", "Bearer test-token")
            .POST(HttpRequest.BodyPublishers.ofString("{\"column\":\"msg\",\"term\":\"x\"}"))
            .build();
    assertEquals(403, send(count).statusCode());
    String pathAfterFirst = lastBackendPath.get();
    lastBackendPath.set(null);
    assertEquals(403, send(count).statusCode(), "a cached denial must keep its own status");
    assertEquals(
        null, lastBackendPath.get(), "the cached denial should not have re-asked the backend");
    assertTrue(pathAfterFirst != null && pathAfterFirst.contains("events"), pathAfterFirst);
  }

  /**
   * A plan id belonging to another table must not be honoured.
   *
   * <p>Iceberg resolves a plan id from a JVM-global {@code InMemoryPlanningState} with no table
   * binding, and {@code cancelPlanTableScan} takes no table at all. The auth gate proves only that
   * the caller may load the table named in the PATH — so a caller authorised on one table, holding
   * a plan id for another, could fetch that other table's file-scan tasks and their file
   * locations. The only obstacle was that plan ids are UUIDs, which is secrecy, not authorisation:
   * a plan id travels in URLs, logs and client state and is not treated as a secret anywhere.
   *
   * <p>404, matching the unknown-id response exactly — distinguishing "another table's" from
   * "does not exist" would confirm to the caller that the id is real.
   *
   * <p>Verified by breaking it: removing the {@code ownsPlan} guard makes these reach Iceberg,
   * which resolves the id happily and answers.
   */
  @Test
  void aPlanIdIsNotHonouredForATableItDoesNotBelongTo() throws Exception {
    String foreignId = "00000000-dead-beef-0000-000000000001";
    String base404 = base + "/v1/lake/namespaces/logs/tables/events";

    HttpResponse<String> fetch =
        send(HttpRequest.newBuilder(URI.create(base404 + "/plan/" + foreignId))
            .header("Authorization", "Bearer test-token").GET().build());
    assertEquals(404, fetch.statusCode(), fetch.body());
    assertTrue(fetch.body().contains("NoSuchPlanIdException"), fetch.body());

    HttpResponse<String> tasks =
        send(HttpRequest.newBuilder(URI.create(base404 + "/tasks"))
            .header("Authorization", "Bearer test-token")
            .POST(HttpRequest.BodyPublishers.ofString("{\"plan-task\":\"" + foreignId + "\"}"))
            .build());
    assertEquals(404, tasks.statusCode(), tasks.body());

    HttpResponse<String> cancel =
        send(HttpRequest.newBuilder(URI.create(base404 + "/plan/" + foreignId))
            .header("Authorization", "Bearer test-token").DELETE().build());
    assertEquals(404, cancel.statusCode(), cancel.body());
  }

  /**
   * Cancelling a plan the proxy answered inline is success, not absence.
   *
   * <p>{@code PlanService} mints a {@code sync-} plan id for a response carrying
   * {@code COMPLETED} and every task, so nothing is registered server-side. A cancel guard that
   * consults only the registered plans refuses such an id as one the proxy never issued — 404 on
   * every query, since Trino cancels every plan, and a log line asserting a security event that
   * is not happening. Reaching it takes a client that cancels, which is why it belongs here
   * rather than in a unit test of the guard.
   *
   * <p>The reply is decided by the id's SHAPE and never consults the plan record, which is what
   * keeps it from confirming that a given id is real — the property the 404 beside it exists for.
   * So an invented {@code sync-} id gets the same 204 as a genuine one, and that is asserted here
   * rather than left implied.
   *
   * <p>Fetching is still 404: the caller already holds the tasks and there is nothing to return.
   *
   * <p>Verified by breaking it: removing the {@code isInlinePlanId} branch from
   * {@code CANCEL_PLAN} returns 404 for both cancels below.
   */
  @Test
  void cancellingAPlanThatWasAnsweredInlineSucceeds() throws Exception {
    String base404 = base + "/v1/lake/namespaces/logs/tables/events";
    String inventedInlineId = "sync-00000000-0000-0000-0000-00000000beef";

    HttpResponse<String> cancel =
        send(HttpRequest.newBuilder(URI.create(base404 + "/plan/" + inventedInlineId))
            .header("Authorization", "Bearer test-token").DELETE().build());
    assertEquals(204, cancel.statusCode(), cancel.body());

    // A second, unrelated inline id gets the same answer: the reply carries no information about
    // which ids this proxy actually issued.
    HttpResponse<String> other =
        send(HttpRequest.newBuilder(
                URI.create(base404 + "/plan/sync-11111111-1111-1111-1111-111111111111"))
            .header("Authorization", "Bearer test-token").DELETE().build());
    assertEquals(204, other.statusCode(), other.body());

    // Fetching one is still not found -- an inline plan has no result to return later.
    HttpResponse<String> fetch =
        send(HttpRequest.newBuilder(URI.create(base404 + "/plan/" + inventedInlineId))
            .header("Authorization", "Bearer test-token").GET().build());
    assertEquals(404, fetch.statusCode(), fetch.body());
    assertTrue(fetch.body().contains("NoSuchPlanIdException"), fetch.body());
  }

  /**
   * A commit that names its tables in the body must invalidate them, like one that names it in the
   * path.
   *
   * <p>{@code MutatedTablesTest} proves the identifiers are read correctly; this proves the
   * dispatcher acts on them. The counter is asserted rather than the eviction because the
   * eviction lands in an Iceberg {@code CachingCatalog} that exists only once a prefix has a
   * client — the decision is what this file can see, and the decision is what matters.
   *
   * <p>Verified by breaking it: removing the {@code MutatedTables.of} block from
   * {@code KahsheHandler} leaves the count at 0 through both requests.
   */
  @Test
  void commitsThatNameTheirTablesInTheBodyAreInvalidatedToo() throws Exception {
    String transaction =
        CommitTransactionRequestParser.toJson(
            new CommitTransactionRequest(
                List.of(
                    UpdateTableRequest.create(
                        TableIdentifier.of(Namespace.of("logs"), "events"), List.of(), List.of()),
                    UpdateTableRequest.create(
                        TableIdentifier.of(Namespace.of("logs"), "spans"), List.of(), List.of()))));

    HttpResponse<String> committed =
        send(
            HttpRequest.newBuilder(URI.create(base + "/v1/prod/transactions/commit"))
                .POST(HttpRequest.BodyPublishers.ofString(transaction))
                .build());
    assertEquals(200, committed.statusCode(), committed.body());
    assertEquals(2, metrics.tableInvalidations.sum(), "both tables in the transaction");

    String rename =
        "{\"source\":"
            + TableIdentifierParser.toJson(TableIdentifier.of(Namespace.of("logs"), "live"))
            + ",\"destination\":"
            + TableIdentifierParser.toJson(TableIdentifier.of(Namespace.of("logs"), "archive"))
            + "}";
    HttpResponse<String> renamed =
        send(
            HttpRequest.newBuilder(URI.create(base + "/v1/prod/tables/rename"))
                .POST(HttpRequest.BodyPublishers.ofString(rename))
                .build());
    assertEquals(200, renamed.statusCode(), renamed.body());
    assertEquals(4, metrics.tableInvalidations.sum(), "and both ends of the rename");
    assertEquals(0, metrics.invalidationParseFailures.sum());
  }

  /**
   * A forwarded loadTable must record the snapshot it told the client about.
   *
   * <p>This is the half that cannot be seen from either side alone: a feature split this way can
   * be inert in production while both halves pass their own tests. {@code ObservedSnapshotTest}
   * proves the recording and the staleness decision are right; this proves the dispatcher actually
   * performs the recording, for a table declaring NO index columns — which is most tables, and
   * exactly the case the indexer's own observation skips.
   *
   * <p>Verified by breaking it: removing the {@code planRoutes.noteObserved} call from
   * {@code KahsheHandler} leaves the observation null.
   */
  @Test
  void aForwardedLoadTableRecordsTheSnapshotItShowedTheClient() throws Exception {
    backendBody.set(
        "{\"metadata-location\":\"x\",\"metadata\":{\"current-snapshot-id\":8675309,"
            + "\"snapshots\":[{\"snapshot-id\":8675309,"
            + "\"summary\":{\"total-delete-files\":\"0\"}}]}}");

    HttpResponse<String> loaded =
        send(HttpRequest.newBuilder(URI.create(base + "/v1/lake/namespaces/logs/tables/events"))
            .header("Authorization", "Bearer test-token").GET().build());
    assertEquals(200, loaded.statusCode(), loaded.body());

    assertEquals(
        8675309L,
        catalogs.observedSnapshot("lake", TableIdentifier.of(Namespace.of("logs"), "events")),
        "the proxy forwarded a snapshot to the client and did not record it; the plan path can "
            + "then serve a view older than the one the client was just given");
  }

  /** An internal failure must never relay exception text to the client. */
  @Test
  void internalErrorsDoNotLeakExceptionText() throws Exception {
    backend.stop(0);
    backend = null;
    HttpResponse<String> response =
        send(HttpRequest.newBuilder(URI.create(base + "/v1/config")).GET().build());
    assertEquals(500, response.statusCode());
    assertTrue(response.body().contains("see server logs"), response.body());
    assertTrue(
        !response.body().contains("Connection refused") && !response.body().contains("Exception"),
        "internal exception text reached the client: " + response.body());
  }
}
