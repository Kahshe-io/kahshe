package io.kahshe.proxy.catalog;

import io.kahshe.common.Metrics;
import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import java.nio.charset.StandardCharsets;
import java.util.ArrayList;
import java.util.List;
import java.util.regex.Matcher;
import java.util.regex.Pattern;
import org.apache.iceberg.catalog.TableIdentifier;
import org.apache.iceberg.catalog.TableIdentifierParser;
import org.apache.iceberg.rest.requests.CommitTransactionRequest;
import org.apache.iceberg.rest.requests.CommitTransactionRequestParser;
import org.apache.iceberg.rest.requests.UpdateTableRequest;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import io.kahshe.proxy.http.KahsheHandler;
import io.kahshe.proxy.plan.ContainsExtractor;
import io.kahshe.proxy.plan.PlanRoutes;

/**
 * The tables a mutating passthrough request changed, for the requests that name them in the body
 * rather than in the path.
 *
 * <p>An ordinary commit or drop names one table in its URL, and {@link KahsheHandler} invalidates
 * it straight from the path match. Two spec endpoints do not: a multi-table transaction ({@code
 * POST /v1/{prefix}/transactions/commit}) carries one identifier per change in its body, and a
 * rename ({@code POST /v1/{prefix}/tables/rename}) carries two. Neither matches the table-path
 * regex, so without this both would pass through leaving every cached table untouched.
 *
 * <p>Identifiers are read with Iceberg's own parsers rather than by picking fields out of the JSON,
 * so the producer and this reader cannot drift apart. This is an optimisation, not the safety net:
 * {@code BackendCatalogs.reconcileToObserved} is what makes a stale cached view safe.
 */
public final class MutatedTables {
  private static final Logger LOG = LoggerFactory.getLogger(MutatedTables.class);
  private static final ObjectMapper MAPPER = new ObjectMapper();

  private static final Pattern TRANSACTION_COMMIT =
      Pattern.compile("^/v1/([^/]+)/transactions/commit$");
  private static final Pattern RENAME = Pattern.compile("^/v1/([^/]+)/tables/rename$");

  private MutatedTables() {}

  /**
   * A request's raw path prefix and every table it changed.
   *
   * @param prefixRaw still percent-encoded, as it appeared in the path, because that is what
   *     {@code PlanRoutes} decodes
   */
  public record Mutation(String prefixRaw, List<TableIdentifier> tables) {}

  /**
   * Returns what this request changed, or null when it is not one of the two body-naming
   * mutations. Called only for a request the backend accepted.
   *
   * <p>A body this cannot read yields an empty mutation rather than null, and moves a counter a
   * scrape can alert on: the request really did change tables and kahshe has just failed to learn
   * which.
   */
  public static Mutation of(String method, String path, byte[] body, Metrics metrics) {
    if (!"POST".equals(method)) {
      return null;
    }
    Matcher commit = TRANSACTION_COMMIT.matcher(path);
    if (commit.matches()) {
      return new Mutation(commit.group(1), transactionTables(body, metrics));
    }
    Matcher rename = RENAME.matcher(path);
    if (rename.matches()) {
      return new Mutation(rename.group(1), renamedTables(body, metrics));
    }
    return null;
  }

  private static List<TableIdentifier> transactionTables(byte[] body, Metrics metrics) {
    try {
      CommitTransactionRequest request =
          CommitTransactionRequestParser.fromJson(new String(body, StandardCharsets.UTF_8));
      List<TableIdentifier> tables = new ArrayList<>();
      for (UpdateTableRequest change : request.tableChanges()) {
        // Inside a transaction every change names its own table; a change that somehow does not is
        // skipped rather than guessed at, since there is no URL here to fall back to.
        if (change.identifier() != null) {
          tables.add(change.identifier());
        }
      }
      return tables;
    } catch (RuntimeException e) {
      return unreadable("transactions/commit", body.length, metrics, e);
    }
  }

  private static List<TableIdentifier> renamedTables(byte[] body, Metrics metrics) {
    try {
      JsonNode root = MAPPER.readTree(body);
      // Both ends. The source name no longer resolves, and the destination name may already have a
      // cached table under it -- a drop followed by a rename onto the freed name would otherwise
      // keep serving the dropped table's files for the rest of the TTL, which is not staleness but
      // another table's data.
      return List.of(
          TableIdentifierParser.fromJson(root.get("source")),
          TableIdentifierParser.fromJson(root.get("destination")));
    } catch (Exception e) {
      return unreadable("tables/rename", body.length, metrics, e);
    }
  }

  private static List<TableIdentifier> unreadable(
      String what, int bodyBytes, Metrics metrics, Exception cause) {
    metrics.invalidationParseFailures.increment();
    LOG.warn(
        "could not read the tables changed by a {} request ({} bytes), so nothing was invalidated "
            + "for it; those tables stay cached until a plan reconciles them against a "
            + "forwarded snapshot, or the TTL expires. Sustained "
            + "kahshe_invalidation_parse_failures_total means the request format has moved.",
        what, bodyBytes, cause);
    return List.of();
  }
}
