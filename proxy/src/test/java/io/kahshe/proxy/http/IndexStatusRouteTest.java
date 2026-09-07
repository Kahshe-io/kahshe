package io.kahshe.proxy.http;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertTrue;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.sun.net.httpserver.HttpServer;
import io.kahshe.common.Metrics;
import io.kahshe.indexer.LocalTableFixture;
import io.kahshe.indexer.TableSource;
import io.kahshe.indexer.maintain.IndexerService;
import java.io.OutputStream;
import java.net.InetSocketAddress;
import java.net.URI;
import java.net.http.HttpClient;
import java.net.http.HttpRequest;
import java.net.http.HttpResponse;
import java.nio.charset.StandardCharsets;
import java.nio.file.Path;
import java.time.Duration;
import java.time.Instant;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.Executors;
import java.util.concurrent.atomic.AtomicBoolean;
import org.apache.iceberg.Table;
import org.apache.iceberg.catalog.TableIdentifier;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

/**
 * {@code GET /index} and {@code GET /index/{prefix}/{namespace}/{table}} on the admin port,
 * behind the bearer check, fed by a real {@link IndexerService} building a real table.
 *
 * <p>Over a socket rather than against the source alone, because what an operator gets is the
 * JSON and the status codes: a route that serialized the wrong field, or sat in front of the
 * gate, would pass a source-level test.
 *
 * <p>Verified red with {@code IndexerService.tables()} returning an empty list: the listing has
 * no table to await and the first test times out on "the build at snapshot ... to be listed".
 */
class IndexStatusRouteTest {
  private static final String TOKEN = "s3cret-status-token";
  private static final String COLUMN = LocalTableFixture.COLUMN;
  private static final ObjectMapper JSON = new ObjectMapper();

  @TempDir Path tmp;

  private HttpServer backend;
  private HttpServer admin;
  private AdminHandler handler;
  private String base;
  private final Metrics metrics = new Metrics();
  private final HttpClient client =
      HttpClient.newBuilder().version(HttpClient.Version.HTTP_1_1).build();

  /** Released to let a parked load through; released on teardown so no worker outlives the dir. */
  private final CountDownLatch gate = new CountDownLatch(1);
  private volatile boolean park;

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

  /** A source that hands out one table, and parks inside {@code load} while asked to. */
  private TableSource source(Table table) {
    return new TableSource() {
      @Override
      public Table load(String prefix, TableIdentifier ident) {
        if (park) {
          try {
            gate.await();
          } catch (InterruptedException e) {
            Thread.currentThread().interrupt();
          }
        }
        return table;
      }

      @Override
      public void invalidate(String prefix, TableIdentifier ident) {}
    };
  }

  /** The production wiring: the indexer is the handler's status source, behind the token. */
  private IndexerService startAdmin(Table table, boolean indexerOn) throws Exception {
    IndexerService indexer =
        new IndexerService(source(table), metrics, indexerOn, LocalTableFixture.config());
    handler =
        new AdminHandler(
            metrics,
            new Forwarder("http://127.0.0.1:" + backend.getAddress().getPort(), 3_000),
            new AtomicBoolean(false),
            AdminAuth.fromToken(TOKEN, metrics),
            indexer);
    admin = HttpServer.create(new InetSocketAddress("127.0.0.1", 0), 0);
    admin.createContext("/", handler);
    admin.setExecutor(Executors.newSingleThreadExecutor());
    admin.start();
    base = "http://127.0.0.1:" + admin.getAddress().getPort();
    return indexer;
  }

  @AfterEach
  void tearDown() {
    gate.countDown();
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
  void aBuiltTableIsListedAndACommitReadsBehindUntilTheNextBuild() throws Exception {
    Table table = LocalTableFixture.createTable(tmp, "alpha beta", "gamma");
    long first = table.currentSnapshot().snapshotId();
    IndexerService indexer = startAdmin(table, true);
    indexer.observe("p", "logs", "events", List.of(COLUMN), first);

    JsonNode events = awaitIndexedAt(first);
    assertEquals("p", events.path("prefix").asText());
    assertEquals("logs", events.path("namespace").asText());
    assertEquals("events", events.path("table").asText());
    assertEquals(List.of(COLUMN), strings(events.path("declared_columns")));
    assertEquals(first, events.path("current_snapshot").asLong());
    assertEquals(0, events.path("behind_seconds").asLong());
    JsonNode msg = events.path("columns").path(COLUMN);
    assertEquals("FULL", msg.path("kind").asText(), msg.toString());
    assertEquals(first, msg.path("snapshot").asLong());
    assertEquals(1, msg.path("files_covered").asLong());
    assertEquals(1, msg.path("files_added").asLong());
    assertEquals(0, msg.path("files_departed").asLong());
    assertFalse(msg.path("analyzer").asText().isEmpty(), "the analyzer id the index was cut under");
    Instant.parse(msg.path("built_at").asText()); // ISO-8601, or this throws
    assertTrue(msg.path("duration_ms").isIntegralNumber());
    assertTrue(events.path("last_failure").isNull());
    assertEquals(0, events.path("refused").size());

    // a commit the worker cannot yet cover: its load is parked
    park = true;
    LocalTableFixture.appendFile(table, "f2.parquet", "delta");
    table.refresh();
    long second = table.currentSnapshot().snapshotId();
    indexer.observe("p", "logs", "events", List.of(COLUMN), second);

    HttpResponse<String> one = get("/index/p/logs/events", TOKEN);
    assertEquals(200, one.statusCode(), one.body());
    assertEquals("application/json", one.headers().firstValue("Content-Type").orElse(null));
    JsonNode behind = JSON.readTree(one.body());
    assertEquals(second, behind.path("current_snapshot").asLong());
    assertEquals(first, behind.path("indexed_snapshot").asLong());
    assertTrue(behind.path("behind_seconds").asLong() > 0, behind.toString());
    assertEquals(404, get("/index/p/logs/nope", TOKEN).statusCode());

    gate.countDown();
    JsonNode caughtUp = awaitIndexedAt(second);
    assertEquals(0, caughtUp.path("behind_seconds").asLong());
    assertEquals("INCREMENTAL", caughtUp.path("columns").path(COLUMN).path("kind").asText());
    assertEquals(2, caughtUp.path("columns").path(COLUMN).path("files_covered").asLong());
    assertEquals(1, caughtUp.path("columns").path(COLUMN).path("files_added").asLong());
  }

  /** The status names tables and snapshots, so it sits behind the same gate as the scrape. */
  @Test
  void theStatusSitsBehindTheBearerCheck() throws Exception {
    startAdmin(LocalTableFixture.createTable(tmp, "alpha"), true);
    assertEquals(401, get("/index", null).statusCode());
    assertEquals(401, get("/index/p/logs/events", null).statusCode());
    assertEquals(2, metrics.adminAuthRejected.sum());

    HttpResponse<String> listing = get("/index", TOKEN);
    assertEquals(200, listing.statusCode());
    assertEquals("application/json", listing.headers().firstValue("Content-Type").orElse(null));
    JsonNode body = JSON.readTree(listing.body());
    assertTrue(body.path("tables").isArray() && body.path("tables").isEmpty(), listing.body());
    assertFalse(body.has("note"), "an indexer that builds and has seen nothing needs no excuse");
  }

  /** A replica with the indexer off cannot report what it does not maintain, and says so. */
  @Test
  void aReplicaWithTheIndexerOffAnswersAnEmptyListingAndSaysWhy() throws Exception {
    IndexerService indexer = startAdmin(LocalTableFixture.createTable(tmp, "alpha"), false);
    indexer.observe("p", "logs", "events", List.of(COLUMN), 7);

    JsonNode body = JSON.readTree(get("/index", TOKEN).body());
    assertEquals(0, body.path("tables").size(), body.toString());
    assertTrue(body.path("note").asText().contains("KAHSHE_INDEXER"), body.toString());
    HttpResponse<String> one = get("/index/p/logs/events", TOKEN);
    assertEquals(404, one.statusCode());
    assertTrue(one.body().contains("KAHSHE_INDEXER"), one.body());
  }

  /** Polls the listing until its one table reads as indexed at {@code snapshot}. */
  private JsonNode awaitIndexedAt(long snapshot) throws Exception {
    long deadline = System.nanoTime() + Duration.ofSeconds(30).toNanos();
    String last = "";
    while (System.nanoTime() < deadline) {
      HttpResponse<String> response = get("/index", TOKEN);
      assertEquals(200, response.statusCode(), response.body());
      JsonNode tables = JSON.readTree(response.body()).path("tables");
      last = response.body();
      if (tables.size() == 1 && tables.get(0).path("indexed_snapshot").asLong(-1) == snapshot) {
        return tables.get(0);
      }
      Thread.sleep(25);
    }
    throw new AssertionError("timed out waiting for the build at snapshot " + snapshot
        + " to be listed; last listing: " + last);
  }

  private static List<String> strings(JsonNode array) {
    List<String> out = new ArrayList<>();
    array.forEach(n -> out.add(n.asText()));
    return out;
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
