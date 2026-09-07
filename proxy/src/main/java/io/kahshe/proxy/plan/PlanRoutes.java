package io.kahshe.proxy.plan;

import java.net.URLDecoder;
import java.nio.charset.StandardCharsets;
import java.util.regex.Matcher;
import java.util.regex.Pattern;
import org.apache.iceberg.catalog.Catalog;
import org.apache.iceberg.catalog.Namespace;
import org.apache.iceberg.catalog.TableIdentifier;
import org.apache.iceberg.exceptions.NoSuchTableException;
import io.kahshe.indexer.maintain.IndexerService;
import org.apache.iceberg.rest.RESTUtil;
import org.apache.iceberg.rest.requests.PlanTableScanRequest;
import org.apache.iceberg.rest.requests.PlanTableScanRequestParser;
import org.apache.iceberg.rest.responses.PlanTableScanResponse;
import org.apache.iceberg.rest.responses.PlanTableScanResponseParser;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import io.kahshe.proxy.ProxyConfig;
import io.kahshe.proxy.catalog.BackendCatalogs;

/**
 * The four scan-planning endpoints, served inline by {@link PlanService}: every plan, incremental
 * ones included, is answered COMPLETED with all of its tasks, so no plan id names server-side
 * state and no request depends on the replica that answered an earlier one. Fetch and cancel are
 * therefore decided by the id's shape alone.
 */
public final class PlanRoutes {
  private static final Logger LOG = LoggerFactory.getLogger(PlanRoutes.class);

  private static final Pattern SUBMIT =
      Pattern.compile("^/v1/([^/]+)/namespaces/([^/]+)/tables/([^/]+)/plan$");
  private static final Pattern BY_ID =
      Pattern.compile("^/v1/([^/]+)/namespaces/([^/]+)/tables/([^/]+)/plan/([^/]+)$");
  private static final Pattern TASKS =
      Pattern.compile("^/v1/([^/]+)/namespaces/([^/]+)/tables/([^/]+)/tasks$");

  public enum Route {
    SUBMIT_PLAN,
    FETCH_PLAN,
    CANCEL_PLAN,
    FETCH_TASKS
  }

  public record Match(
      Route route,
      String prefix,
      TableIdentifier ident,
      String planId,
      String rawPrefix,
      String rawNamespace,
      String rawTable) {}

  public record Result(int status, byte[] body) {}

  private final ProxyConfig config;
  private final BackendCatalogs backendCatalogs;
  private final PlanService planService;
  private final io.kahshe.common.Metrics metrics;

  public PlanRoutes(
      ProxyConfig config,
      io.kahshe.format.FormatConfig format,
      BackendCatalogs backendCatalogs,
      io.kahshe.common.Metrics metrics,
      io.kahshe.format.type.term.TermIndex termIndex) {
    this.config = config;
    this.backendCatalogs = backendCatalogs;
    this.metrics = metrics;
    this.planService = new PlanService(metrics, termIndex, config, format);
  }

  /**
   * Records the snapshot a forwarded loadTable told a client about.
   *
   * <p>Takes the raw path segments and decodes them exactly as {@link #invalidate} does, so the
   * identity written here and the one read at plan time are produced by the same code rather than
   * by two encoding conventions that happen to agree.
   */
  public void noteObserved(
      String prefixRaw, String namespaceRaw, String tableRaw, long snapshotId) {
    backendCatalogs.noteObserved(
        decode(prefixRaw),
        TableIdentifier.of(RESTUtil.decodeNamespace(namespaceRaw, IndexerService.NAMESPACE_SEPARATOR), decode(tableRaw)),
        snapshotId);
  }

  /**
   * Invalidate a cached table named in a request body rather than in the path.
   *
   * <p>The identifier arrives already decoded, because it was read out of JSON by Iceberg's own
   * parser rather than out of a percent-encoded path segment.
   */
  public void invalidate(String prefixRaw, TableIdentifier ident) {
    backendCatalogs.invalidate(decode(prefixRaw), ident);
  }

  /** Invalidate the cached table after a commit observed in the passthrough. */
  public void invalidate(String prefixRaw, String namespaceRaw, String tableRaw) {
    backendCatalogs.invalidate(
        decode(prefixRaw),
        TableIdentifier.of(RESTUtil.decodeNamespace(namespaceRaw, IndexerService.NAMESPACE_SEPARATOR), decode(tableRaw)));
  }

  /** Returns a match when this request targets a plan endpoint, else null. */
  public Match match(String method, String path) {
    Matcher m = SUBMIT.matcher(path);
    if (m.matches() && "POST".equals(method)) {
      return match(Route.SUBMIT_PLAN, m, null);
    }
    m = BY_ID.matcher(path);
    if (m.matches() && "GET".equals(method)) {
      return match(Route.FETCH_PLAN, m, decode(m.group(4)));
    }
    if (m.matches() && "DELETE".equals(method)) {
      return match(Route.CANCEL_PLAN, m, decode(m.group(4)));
    }
    m = TASKS.matcher(path);
    if (m.matches() && "POST".equals(method)) {
      return match(Route.FETCH_TASKS, m, null);
    }
    return null;
  }

  private static Match match(Route route, Matcher m, String planId) {
    return new Match(
        route, decode(m.group(1)), ident(m), planId, m.group(1), m.group(2), m.group(3));
  }

  /**
   * Refuses every plan id: nothing is fetchable, because every plan is answered inline.
   *
   * <p>404, never 403, and identical whatever the id — an id is never checked against a record.
   * Distinguishing "belongs to another table" from "does not exist" would confirm an id is real to
   * a caller who should not learn that, and the auth gate only proves the caller may load the table
   * named in the path, not the one a foreign plan id refers to.
   */
  private Result unknownPlan(Match match) {
    // Two different facts: an id this proxy minted for an inline plan is not evidence of anything,
    // while a foreign id might be.
    if (isInlinePlanId(match.planId())) {
      LOG.info(
          "plan id {} for {} was answered inline, so it names no plan to fetch",
          match.planId(), match.ident());
    } else {
      LOG.info("plan id {} is not one this proxy issued for {}", match.planId(), match.ident());
    }
    return json(
        404,
        "{\"error\":{\"message\":\"Cannot find plan with id "
            + String.valueOf(match.planId()).replace("\"", "'")
            + "\",\"type\":\"NoSuchPlanIdException\",\"code\":404}}");
  }

  /**
   * Whether this is an id {@code PlanService} minted for a plan it answered inline. Decided from
   * the id's shape alone — the shared {@link PlanService#INLINE_PLAN_ID_PREFIX} — so no record is
   * consulted and an invented id is indistinguishable from a real one.
   */
  private static boolean isInlinePlanId(String planId) {
    return planId != null && planId.startsWith(PlanService.INLINE_PLAN_ID_PREFIX);
  }

  /**
   * Whether the table really declares a column of this name. Asked only when a sentinel-shaped term
   * appears, and it loads through the same cached catalog {@code PlanService} is about to use, so it
   * costs a cache hit rather than a round trip.
   *
   * <p>Returns <b>true</b> when the schema cannot be read: that declines the hint and keeps every
   * file, which costs only pruning. Answering false on doubt would prune on a column that was never
   * ruled out.
   */
  private boolean declaresColumn(Catalog catalog, TableIdentifier ident, String name) {
    try {
      return catalog.loadTable(ident).schema().findField(name) != null;
    } catch (RuntimeException e) {
      LOG.warn(
          "could not read {}'s schema to rule out a real column named '{}'; planning it as an "
              + "ordinary predicate and pruning nothing on it",
          ident, name, e);
      return true;
    }
  }


  public Result handle(Match match, byte[] body, String callerToken) {
    Catalog catalog =
        config.callerIdentityPlanning() && callerToken != null
            ? backendCatalogs.forCaller(match.rawPrefix(), callerToken)
            : backendCatalogs.forPrefix(match.prefix());
    try {
      switch (match.route()) {
        case SUBMIT_PLAN -> {
          // Never plan a view older than the one this proxy handed the client. Clients do not pin
          // snapshot-id on ordinary scans, so kahshe is the only party that can notice.
          if (backendCatalogs.reconcileToObserved(catalog, match.prefix(), match.ident())) {
            metrics.planStaleViewReloads.increment();
          }
          ContainsExtractor.Extraction extraction =
              ContainsExtractor.extract(
                  new String(body, StandardCharsets.UTF_8),
                  name -> declaresColumn(catalog, match.ident(), name));
          PlanTableScanRequest request = PlanTableScanRequestParser.fromJson(extraction.cleanedJson());
          PlanTableScanResponse response =
              planService.plan(catalog, match.ident(), request, extraction.hints(), callerToken);
          return json(200, PlanTableScanResponseParser.toJson(response));
        }
        case FETCH_PLAN -> {
          // Every plan is answered inline and COMPLETED, so there is never a result to fetch: the
          // caller already holds the tasks, and no id names server-side state on any replica.
          return unknownPlan(match);
        }
        case CANCEL_PLAN -> {
          if (isInlinePlanId(match.planId())) {
            // Nothing to cancel, and that is success rather than absence: no server-side state was
            // ever created for this id. An invented id gets the same 204, because consulting a
            // record would confirm an id is real -- which is what unknownPlan's 404 avoids.
            return new Result(204, new byte[0]);
          }
          return unknownPlan(match);
        }
        case FETCH_TASKS -> {
          // No plan is ever paged: see FETCH_PLAN.
          return unknownPlan(match);
        }
      }
      throw new IllegalStateException("unreachable");
    } catch (PlanService.DeleteBearingSnapshotException e) {
      // 422, not 400: the request is perfectly well formed and we simply cannot serve it
      // correctly. Same shape as _count's refusals -- exact or refuse, never quietly wrong.
      LOG.warn("refusing to plan {} server-side: {}", match.ident(), e.getMessage());
      return json(
          422,
          "{\"error\":{\"message\":\""
              + String.valueOf(e.getMessage()).replace("\"", "'")
              + "\",\"type\":\"UnprocessableEntityException\",\"code\":422}}");
    } catch (org.apache.iceberg.exceptions.NotAuthorizedException e) {
      return json(
          401, "{\"error\":{\"message\":\"not authorized\",\"type\":\"NotAuthorizedException\",\"code\":401}}");
    } catch (org.apache.iceberg.exceptions.ForbiddenException e) {
      return json(
          403, "{\"error\":{\"message\":\"forbidden\",\"type\":\"ForbiddenException\",\"code\":403}}");
    } catch (org.apache.iceberg.exceptions.ValidationException | IllegalArgumentException e) {
      // A 400 that hides what was refused is useless to whoever reads the log; the body is the
      // client's own request, bounded so a large one cannot flood the log.
      String shown = new String(body, java.nio.charset.StandardCharsets.UTF_8);
      LOG.warn("refusing plan request for {} as malformed: {} -- body: {}", match.ident(),
          e.getMessage(), shown.length() > 1024 ? shown.substring(0, 1024) + "..." : shown);
      return json(
          400,
          "{\"error\":{\"message\":\""
              + String.valueOf(e.getMessage()).replace("\"", "'")
              + "\",\"type\":\"BadRequestException\",\"code\":400}}");
    } catch (org.apache.iceberg.exceptions.NoSuchPlanIdException e) {
      // The REST spec says 404 for a plan id the server does not hold. Unmapped this falls through
      // to a 500, and a client polling a plan that has aged out should see "gone" rather than "we
      // crashed", because only one of those is worth retrying.
      LOG.info("fetch for unknown plan id on {}", match.ident());
      return json(
          404,
          "{\"error\":{\"message\":\"Cannot find plan with id "
              + String.valueOf(match.planId()).replace("\"", "'")
              + "\",\"type\":\"NoSuchPlanIdException\",\"code\":404}}");
    } catch (NoSuchTableException e) {
      LOG.warn("plan request for missing table {}", match.ident());
      return json(
          404,
          "{\"error\":{\"message\":\"Table does not exist: "
              + match.ident()
              + "\",\"type\":\"NoSuchTableException\",\"code\":404}}");
    }
  }

  private static TableIdentifier ident(Matcher m) {
    // REST spec: namespace levels are separated by %1F in the path segment
    Namespace namespace = RESTUtil.decodeNamespace(m.group(2), IndexerService.NAMESPACE_SEPARATOR);
    return TableIdentifier.of(namespace, decode(m.group(3)));
  }

  private static String decode(String raw) {
    return URLDecoder.decode(raw, StandardCharsets.UTF_8);
  }

  private static Result json(int status, String body) {
    return new Result(status, body.getBytes(StandardCharsets.UTF_8));
  }
}
