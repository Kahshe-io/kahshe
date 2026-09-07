package io.kahshe.proxy.http;

import io.kahshe.common.Metrics;
import com.sun.net.httpserver.HttpExchange;
import com.sun.net.httpserver.HttpHandler;
import io.kahshe.format.FormatConfig;
import java.io.IOException;
import java.io.OutputStream;
import java.util.List;
import java.util.Map;
import java.util.regex.Pattern;
import org.apache.iceberg.catalog.TableIdentifier;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import io.kahshe.proxy.ProxyConfig;
import io.kahshe.proxy.catalog.BackendCatalogs;
import io.kahshe.proxy.catalog.MutatedTables;
import io.kahshe.proxy.catalog.Mutations;
import io.kahshe.proxy.plan.CountRoutes;
import io.kahshe.proxy.plan.PlanRoutes;

/**
 * Root dispatcher for the data plane: {@code _count} and the four scan-planning endpoints are
 * served locally, everything else is forwarded to the backing catalog and its response rewritten
 * on the way back.
 */
public final class KahsheHandler implements HttpHandler {
  private static final Logger LOG = LoggerFactory.getLogger(KahsheHandler.class);

  private static final Pattern CONFIG_PATH = Pattern.compile("^/v1/config$");
  private static final Pattern TABLE_PATH =
      Pattern.compile("^/v1/([^/]+)/namespaces/([^/]+)/tables/([^/]+)$");
  private static final Pattern COUNT_PATH =
      Pattern.compile("^/kahshe/v1/([^/]+)/namespaces/([^/]+)/tables/([^/]+)/_count$");

  private final ProxyConfig config;
  private final FormatConfig format;
  private final Forwarder forwarder;
  private final PlanRoutes planRoutes;
  private final CountRoutes countRoutes;
  private final Metrics metrics;
  private final AuthGate authGate;

  public KahsheHandler(ProxyConfig config, FormatConfig format, Metrics metrics, BackendCatalogs catalogs,
      io.kahshe.indexer.maintain.IndexerService indexer) {
    this.config = config;
    this.format = format;
    this.metrics = metrics;
    this.forwarder = new Forwarder(config.backendBase(), config.backendTimeoutMs(), config.backendCa());
    io.kahshe.format.type.term.TermIndex termIndex = new io.kahshe.format.type.term.TermIndex(format, metrics);
    metrics.responseRewriteFailures = Mutations.REWRITE_FAILURES::sum;
    metrics.cacheBudgetBytes =
        format.indexCacheBytes() + format.termCacheBytes() + format.planCacheBytes()
            + (format.gramIndexEnabled() ? format.gramCacheBytes() : 0);
    this.planRoutes = new PlanRoutes(config, format, catalogs, metrics, termIndex);
    this.countRoutes = new CountRoutes(config, format, catalogs, termIndex);
    this.authGate = new AuthGate(forwarder, config.authCacheTtlMs(), metrics);
    this.indexer = indexer;
  }

  private final io.kahshe.indexer.maintain.IndexerService indexer;

  @Override
  public void handle(HttpExchange exchange) throws IOException {
    String method = exchange.getRequestMethod();
    String path = exchange.getRequestURI().getRawPath();
    String query = exchange.getRequestURI().getRawQuery();
    String pathWithQuery = query == null ? path : path + "?" + query;

    try {
      // Fast path only: a client that declares an oversized body is rejected before we read a
      // byte of it. It is not the guard -- see readBody.
      long declaredLength = parseLong(exchange.getRequestHeaders().getFirst("Content-Length"));
      if (declaredLength > config.maxBodyBytes()) {
        respond(exchange, 413, Mutations.errorBody(413, "PayloadTooLarge", "request body too large"),
            "application/json");
        return;
      }

      java.util.regex.Matcher countMatch = COUNT_PATH.matcher(path);
      if (countMatch.matches() && "POST".equals(method)) {
        handleCount(exchange, countMatch, path);
        return;
      }

      PlanRoutes.Match planMatch = planRoutes.match(method, path);
      if (planMatch != null) {
        handlePlan(exchange, planMatch, method, path);
        return;
      }

      handlePassthrough(exchange, method, path, pathWithQuery);
    } catch (BodyTooLarge e) {
      respond(exchange, 413, Mutations.errorBody(413, "PayloadTooLarge", "request body too large"),
          "application/json");
    } catch (Exception e) {
      metrics.errors.increment();
      LOG.error("{} {} failed", method, path, e);
      // never relay internal exception text to clients
      respond(
          exchange,
          500,
          Mutations.errorBody(500, "InternalServerError", "internal error, see server logs"),
          "application/json");
    } finally {
      exchange.close();
    }
  }

  /** Serves the local {@code _count} endpoint for an already-matched path. */
  private void handleCount(HttpExchange exchange, java.util.regex.Matcher countMatch, String path)
      throws Exception {
    metrics.countRequests.increment();
    byte[] body = readBody(exchange);
    // a count is a content oracle: the caller's own token must authorize reading the table
    Forwarder.Response denied =
        authGate.check(
            countMatch.group(1), countMatch.group(2), countMatch.group(3),
            exchange.getRequestHeaders());
    if (denied != null) {
      respond(exchange, denied.status(), denied.body(), "application/json");
      return;
    }
    PlanRoutes.Result result =
        countRoutes.count(
            countMatch.group(1), countMatch.group(2), countMatch.group(3), body,
            exchange.getRequestHeaders().getFirst("Authorization"));
    LOG.info("POST {} -> {} (_count)", path, result.status());
    respond(exchange, result.status(), result.body(), "application/json");
  }

  /** Serves an already-matched local plan endpoint. */
  private void handlePlan(
      HttpExchange exchange, PlanRoutes.Match planMatch, String method, String path)
      throws Exception {
    metrics.planRequests.increment();
    long start = System.nanoTime();
    byte[] body = readBody(exchange);
    Forwarder.Response denied =
        authGate.check(
            planMatch.rawPrefix(), planMatch.rawNamespace(), planMatch.rawTable(),
            exchange.getRequestHeaders());
    if (denied != null) {
      respond(exchange, denied.status(), denied.body(), "application/json");
      return;
    }
    PlanRoutes.Result result =
        planRoutes.handle(planMatch, body, exchange.getRequestHeaders().getFirst("Authorization"));
    metrics.planDurationUsTotal.add((System.nanoTime() - start) / 1_000);
    LOG.info("{} {} -> {} (local plan endpoint)", method, path, result.status());
    respond(exchange, result.status(), result.body(), "application/json");
  }

  /** Forwards to the backend, then mutates, invalidates and relays the response. */
  private void handlePassthrough(
      HttpExchange exchange, String method, String path, String pathWithQuery)
      throws IOException, InterruptedException {
    metrics.passthroughRequests.increment();

    byte[] requestBody = readBody(exchange);
    Forwarder.Response backend =
        forwarder.forward(method, pathWithQuery, exchange.getRequestHeaders(), requestBody);

    java.util.regex.Matcher tableMatch = TABLE_PATH.matcher(path);
    boolean isTablePath = tableMatch.matches();
    byte[] responseBody =
        mutateResponse(backend, method, path, pathWithQuery, tableMatch, isTablePath);
    invalidateMutatedTables(backend, method, path, requestBody, tableMatch, isTablePath);

    for (Map.Entry<String, List<String>> header : backend.headers().entrySet()) {
      for (String value : header.getValue()) {
        exchange.getResponseHeaders().add(header.getKey(), value);
      }
    }
    respond(exchange, backend.status(), responseBody, null);
  }

  /** Applies the config merge and the server-planning injection a passthrough response may need. */
  private byte[] mutateResponse(
      Forwarder.Response backend, String method, String path, String pathWithQuery,
      java.util.regex.Matcher tableMatch, boolean isTablePath) {
    byte[] responseBody = backend.body();
    if (backend.status() == 200 && "GET".equals(method)) {
      if (CONFIG_PATH.matcher(path).matches()) {
        responseBody = Mutations.mergeConfigEndpoints(responseBody);
        LOG.info("GET {} -> {} (merged plan endpoints)", pathWithQuery, backend.status());
      } else if (config.injectPlanning() && isTablePath) {
        // the loadTable passthrough is also the indexer's trigger: observe policy + snapshot
        Mutations.IndexPolicy policy = Mutations.indexPolicy(responseBody);
        if (policy != null && !policy.columns().isEmpty()) {
          indexer.observe(
              tableMatch.group(1), tableMatch.group(2), tableMatch.group(3),
              policy.columns(), policy.snapshotId());
        }
        // And record the snapshot for every table, indexed or not: this response is what the
        // client now believes, and the plan path must not serve it something older. The indexer's
        // observation above cannot stand in for it -- that one is skipped for any table declaring
        // no index columns, which is most of them.
        planRoutes.noteObserved(
            tableMatch.group(1), tableMatch.group(2), tableMatch.group(3),
            Mutations.currentSnapshotId(responseBody));
        Mutations.ServerPlanning planning =
            Mutations.injectServerPlanning(responseBody, config.serveDeleteBearing());
        responseBody = planning.body();
        if (planning.injected()) {
          LOG.info("GET {} -> {} (injected scan-planning-mode=server)", path, backend.status());
        } else {
          metrics.serverPlanningDeclined.increment();
          LOG.info(
              "GET {} -> {} (server planning NOT advertised: the snapshot is not provably "
                  + "delete-free, so this client will plan locally. Compaction makes it "
                  + "servable again; watch kahshe_server_planning_declined_total)",
              path, backend.status());
        }
      }
    }
    return responseBody;
  }

  /** Drops the plan catalog's cached view of every table a successful passthrough mutated. */
  private void invalidateMutatedTables(
      Forwarder.Response backend, String method, String path, byte[] requestBody,
      java.util.regex.Matcher tableMatch, boolean isTablePath) {
    if (backend.status() / 100 == 2) {
      if (isTablePath && ("POST".equals(method) || "DELETE".equals(method))) {
        planRoutes.invalidate(tableMatch.group(1), tableMatch.group(2), tableMatch.group(3));
        indexer.invalidate(tableMatch.group(1), tableMatch.group(2), tableMatch.group(3));
        metrics.tableInvalidations.increment();
      }
      // ...and so does one that named its tables in the body instead of the path. The indexer is
      // deliberately not invalidated here: its dedup key is the raw, client-chosen encoding of the
      // path segments, which a decoded identifier cannot reconstruct (%2D and - are the same table
      // and different keys). It does not need to be -- observe() re-enqueues whenever the snapshot
      // id it is handed differs from the one it recorded, and a commit always produces a new one.
      MutatedTables.Mutation mutation = MutatedTables.of(method, path, requestBody, metrics);
      if (mutation != null) {
        for (TableIdentifier ident : mutation.tables()) {
          planRoutes.invalidate(mutation.prefixRaw(), ident);
          metrics.tableInvalidations.increment();
        }
      }
    }
  }

  /** Signals a body that exceeded the cap while being read. */
  private static final class BodyTooLarge extends IOException {}

  /**
   * Reads the request body, enforcing the cap on the bytes actually read.
   *
   * <p>The Content-Length check above cannot be the guard: a request sent with {@code
   * Transfer-Encoding: chunked} carries no Content-Length at all, so the declared length parses as
   * 0 and sails past it. The passthrough path reaches here before any authorization happens, so an
   * unbounded read is an unauthenticated memory-exhaustion DoS against every worker thread at once.
   *
   * <p>Reads one buffer at a time and stops the moment the total crosses the cap, so an oversized
   * body costs the cap plus one buffer rather than however much the client chose to send.
   */
  private byte[] readBody(HttpExchange exchange) throws IOException {
    long cap = config.maxBodyBytes();
    java.io.InputStream in = exchange.getRequestBody();
    java.io.ByteArrayOutputStream out = new java.io.ByteArrayOutputStream();
    byte[] buffer = new byte[8192];
    long total = 0;
    int read;
    while ((read = in.read(buffer)) != -1) {
      total += read;
      if (total > cap) {
        throw new BodyTooLarge();
      }
      out.write(buffer, 0, read);
    }
    return out.toByteArray();
  }

  private static long parseLong(String value) {
    try {
      return value == null ? 0 : Long.parseLong(value);
    } catch (NumberFormatException e) {
      return 0;
    }
  }

  private static void respond(HttpExchange exchange, int status, byte[] body, String contentType)
      throws IOException {
    if (contentType != null) {
      exchange.getResponseHeaders().set("Content-Type", contentType);
    }
    if (body == null || body.length == 0 || status == 204) {
      exchange.sendResponseHeaders(status, -1);
      return;
    }
    exchange.getResponseHeaders().set("Content-Length", String.valueOf(body.length));
    exchange.sendResponseHeaders(status, body.length);
    try (OutputStream out = exchange.getResponseBody()) {
      out.write(body);
    }
  }
}
