package io.kahshe.proxy.catalog;

import io.kahshe.common.Metrics;
import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.node.ArrayNode;
import com.fasterxml.jackson.databind.node.ObjectNode;
import java.nio.charset.StandardCharsets;
import java.util.List;
import org.apache.iceberg.rest.Endpoint;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import io.kahshe.proxy.plan.PlanService;

/**
 * JSON rewrites applied to backend responses: advertise the plan endpoints in {@code /v1/config}
 * and inject {@code scan-planning-mode=server} into LoadTableResponse config.
 */
public final class Mutations {
  private static final Logger LOG = LoggerFactory.getLogger(Mutations.class);
  private static final ObjectMapper MAPPER = new ObjectMapper();

  /**
   * Response rewrites that failed and fell through unmodified.
   *
   * <p>Read by {@link io.kahshe.common.Metrics}. Non-zero means kahshe is passing traffic through
   * without advertising or injecting anything: correct, but otherwise invisible, because the only
   * symptom is that queries are not getting faster.
   */
  public static final java.util.concurrent.atomic.LongAdder REWRITE_FAILURES =
      new java.util.concurrent.atomic.LongAdder();

  static final List<Endpoint> PLAN_ENDPOINTS =
      List.of(
          Endpoint.V1_SUBMIT_TABLE_SCAN_PLAN,
          Endpoint.V1_FETCH_TABLE_SCAN_PLAN,
          Endpoint.V1_CANCEL_TABLE_SCAN_PLAN,
          Endpoint.V1_FETCH_TABLE_SCAN_PLAN_TASKS);

  /**
   * Endpoints synthesized when the backend advertises none. If the config response carries an
   * {@code endpoints} list, clients trust only that list, so an injected list must cover the
   * standard catalog surface too.
   */
  private static final List<Endpoint> BASELINE_ENDPOINTS =
      List.of(
          Endpoint.V1_LIST_NAMESPACES,
          Endpoint.V1_LOAD_NAMESPACE,
          Endpoint.V1_NAMESPACE_EXISTS,
          Endpoint.V1_CREATE_NAMESPACE,
          Endpoint.V1_UPDATE_NAMESPACE,
          Endpoint.V1_DELETE_NAMESPACE,
          Endpoint.V1_LIST_TABLES,
          Endpoint.V1_LOAD_TABLE,
          Endpoint.V1_TABLE_EXISTS,
          Endpoint.V1_CREATE_TABLE,
          Endpoint.V1_UPDATE_TABLE,
          Endpoint.V1_DELETE_TABLE,
          Endpoint.V1_RENAME_TABLE,
          Endpoint.V1_REGISTER_TABLE,
          Endpoint.V1_REPORT_METRICS,
          Endpoint.V1_COMMIT_TRANSACTION,
          Endpoint.V1_LIST_VIEWS,
          Endpoint.V1_LOAD_VIEW,
          Endpoint.V1_VIEW_EXISTS,
          Endpoint.V1_CREATE_VIEW,
          Endpoint.V1_UPDATE_VIEW,
          Endpoint.V1_DELETE_VIEW,
          Endpoint.V1_RENAME_VIEW);

  private Mutations() {}

  /**
   * Merge the scan-planning endpoints into the config response's advertised endpoint list.
   *
   * <p>Also strips any {@code uri} the backend advertises in defaults/overrides: clients honor
   * it as a redirect for all subsequent requests (Nessie advertises its own address), which
   * would route them around the proxy after the first config call.
   */
  public static byte[] mergeConfigEndpoints(byte[] body) {
    try {
      ObjectNode root = (ObjectNode) MAPPER.readTree(body);
      for (String section : new String[] {"overrides", "defaults"}) {
        JsonNode node = root.path(section);
        if (node.isObject() && node.has("uri")) {
          LOG.info("stripping backend-advertised uri from config {} (would bypass the proxy)", section);
          ((ObjectNode) node).remove("uri");
        }
      }
      ArrayNode endpoints;
      if (root.has("endpoints") && root.get("endpoints").isArray()) {
        endpoints = (ArrayNode) root.get("endpoints");
      } else {
        LOG.warn("backend /v1/config advertises no endpoints; synthesizing baseline list");
        endpoints = root.putArray("endpoints");
        BASELINE_ENDPOINTS.forEach(e -> endpoints.add(e.toString()));
      }
      for (Endpoint endpoint : PLAN_ENDPOINTS) {
        String repr = endpoint.toString();
        boolean present = false;
        for (JsonNode node : endpoints) {
          if (repr.equals(node.asText())) {
            present = true;
            break;
          }
        }
        if (!present) {
          endpoints.add(repr);
        }
      }
      return MAPPER.writeValueAsBytes(root);
    } catch (Exception e) {
      // Passing the body through is the safe direction -- clients see the backend's own config --
      // but it also leaves the plan endpoints unadvertised, so every client silently keeps
      // planning locally and kahshe does nothing at all. A counter makes fleet-wide format drift
      // visible instead of looking like "the index just isn't helping".
      REWRITE_FAILURES.increment();
      LOG.warn("failed to rewrite /v1/config response; passing through unmodified. Plan endpoints "
          + "are NOT advertised, so clients will plan locally.", e);
      return body;
    }
  }

  /**
   * The rewritten body and whether server planning was actually advertised in it. The caller needs
   * the second half to log truthfully, since a delete-bearing table is passed through un-injected.
   */
  public record ServerPlanning(byte[] body, boolean injected, String declinedBecause) {}

  /** {@code KAHSHE_ADVERTISE_SERVER_MODE}: which tables are told to plan server-side. */
  public static final String ADVERTISE_ALL = "all";

  public static final String ADVERTISE_INDEXED = "indexed";
  public static final String ADVERTISE_NONE = "none";

  public static ServerPlanning injectServerPlanning(byte[] body) {
    return injectServerPlanning(body, false);
  }

  public static ServerPlanning injectServerPlanning(byte[] body, boolean serveDeleteBearing) {
    return injectServerPlanning(body, serveDeleteBearing, ADVERTISE_ALL);
  }

  /**
   * Inject scan-planning-mode=server into a LoadTableResponse's config map.
   *
   * <p>Injection is skipped unless the current snapshot provably has zero delete files: the REST
   * scan-task wire format loses {@code DeleteFile.dataSequenceNumber}, which breaks merge-on-read
   * readers. Delete-bearing tables therefore fall back to local planning — correct, just
   * unaccelerated.
   *
   * @param serveDeleteBearing when true, advertise server planning even for a snapshot that is not
   *     provably delete-free. Must move together with {@code PlanService}'s guard on the same flag:
   *     gating only the serving half leaves the flag inert, because a client never told to plan
   *     server-side never asks. Off by default.
   * @param advertiseMode which tables to advertise to: {@code all} (the default), {@code indexed}
   *     (only a table declaring {@code kahshe.index}) or {@code none}. Narrowing costs the
   *     manifest-fetch saving on the tables it skips — they plan locally and read the same files —
   *     and buys back whatever a client cannot do while a table says it MUST plan server-side.
   *     A value this does not recognise is treated as {@code all}; {@code Kahshe} refuses one at
   *     startup, so an unrecognised value here means a caller bypassed that check.
   */
  public static ServerPlanning injectServerPlanning(
      byte[] body, boolean serveDeleteBearing, String advertiseMode) {
    try {
      if (ADVERTISE_NONE.equalsIgnoreCase(advertiseMode)) {
        return new ServerPlanning(body, false, "KAHSHE_ADVERTISE_SERVER_MODE=none");
      }
      ObjectNode root = (ObjectNode) MAPPER.readTree(body);
      if (ADVERTISE_INDEXED.equalsIgnoreCase(advertiseMode) && !declaresIndex(root.path("metadata"))) {
        return new ServerPlanning(
            body, false, "KAHSHE_ADVERTISE_SERVER_MODE=indexed and this table declares no kahshe.index");
      }
      if (!serveDeleteBearing && !provablyDeleteFree(root.path("metadata"))) {
        return new ServerPlanning(body, false, "the snapshot is not provably delete-free");
      }
      ObjectNode config;
      if (root.has("config") && root.get("config").isObject()) {
        config = (ObjectNode) root.get("config");
      } else {
        config = root.putObject("config");
      }
      config.put("scan-planning-mode", "server");
      return new ServerPlanning(MAPPER.writeValueAsBytes(root), true, null);
    } catch (Exception e) {
      REWRITE_FAILURES.increment();
      LOG.warn("failed to rewrite LoadTableResponse; passing through unmodified. "
          + "scan-planning-mode=server is NOT injected, so this client will plan locally.", e);
      return new ServerPlanning(body, false, "the LoadTableResponse could not be rewritten");
    }
  }

  /**
   * The columns a table's {@code kahshe.index} property names, and the snapshot they were read
   * against.
   */
  public record IndexPolicy(java.util.List<String> columns, long snapshotId) {}

  /**
   * The index policy in a loadTable response, or null when the table declares none, names no
   * current snapshot, or the body cannot be read. {@code columns} can still be empty if the
   * property holds nothing but separators, so callers check it before enqueuing a build.
   */
  public static IndexPolicy indexPolicy(byte[] loadTableBody) {
    try {
      JsonNode root = MAPPER.readTree(loadTableBody);
      JsonNode metadata = root.path("metadata");
      String columns = metadata.path("properties").path("kahshe.index").asText("");
      long snapshotId = metadata.path("current-snapshot-id").asLong(-1);
      if (columns.isBlank() || snapshotId <= 0) {
        return null;
      }
      return new IndexPolicy(
          java.util.Arrays.stream(columns.split(",")).map(String::trim).filter(s -> !s.isEmpty()).toList(),
          snapshotId);
    } catch (Exception e) {
      // A table that declares kahshe.index and is never indexed because this threw is the quietest
      // failure in the system: the property is set, no index appears, and because a missing index
      // only keeps files, every query is merely slower. Hence the warning.
      LOG.warn("could not read the kahshe.index policy from a LoadTableResponse; "
          + "this table will NOT be indexed until the response parses", e);
      return null;
    }
  }


  /**
   * The current snapshot id in a loadTable response, or -1 if it names none.
   *
   * <p>Separate from {@link #indexPolicy}, which returns null for a table declaring no index
   * columns — the snapshot matters for every table, indexed or not, because it is what the plan
   * path uses to avoid serving a view older than the one this response just gave the client.
   */
  public static long currentSnapshotId(byte[] loadTableBody) {
    try {
      return MAPPER.readTree(loadTableBody).path("metadata").path("current-snapshot-id").asLong(-1);
    } catch (Exception e) {
      // Not fatal: an unreadable body simply records no observation, and the plan path falls back
      // to the cached view it used before. The passthrough itself is unaffected.
      return -1;
    }
  }

  /**
   * Whether the table declares index columns at all. Deliberately not {@link #indexPolicy}: that
   * one also requires a current snapshot, because it drives indexing, and an empty table that
   * declares {@code kahshe.index} is still a table the operator has opted in.
   */
  private static boolean declaresIndex(JsonNode metadata) {
    return !metadata.path("properties").path("kahshe.index").asText("").isBlank();
  }

  private static boolean provablyDeleteFree(JsonNode metadata) {
    long currentSnapshotId = metadata.path("current-snapshot-id").asLong(-1);
    if (currentSnapshotId == -1) {
      return true; // empty table: nothing to delete from
    }
    for (JsonNode snapshot : metadata.path("snapshots")) {
      if (snapshot.path("snapshot-id").asLong() == currentSnapshotId) {
        return "0".equals(snapshot.path("summary").path("total-delete-files").asText(""));
      }
    }
    return false; // cannot prove: do not inject
  }

  public static byte[] errorBody(int code, String type, String message) {
    ObjectNode root = MAPPER.createObjectNode();
    ObjectNode error = root.putObject("error");
    error.put("message", message == null ? "" : message);
    error.put("type", type);
    error.put("code", code);
    return root.toString().getBytes(StandardCharsets.UTF_8);
  }
}
