package io.kahshe.proxy.plan;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.node.ObjectNode;
import io.kahshe.format.type.term.TermIndex;
import java.nio.charset.StandardCharsets;
import java.util.List;
import org.apache.iceberg.Table;
import org.apache.iceberg.catalog.TableIdentifier;
import io.kahshe.indexer.maintain.IndexerService;
import org.apache.iceberg.rest.RESTUtil;
import io.kahshe.proxy.catalog.BackendCatalogs;
import io.kahshe.proxy.http.KahsheHandler;

/**
 * {@code POST /kahshe/v1/.../_count}: token and prefix counts answered from the term index at plan
 * speed, without reading a data file.
 *
 * <p>Exact or refusing, never silently approximate — every condition under which the number would
 * be an upper bound answers 4xx with the reason instead. The gate in {@link KahsheHandler} proves
 * the caller's own bearer can load the table; the load that follows here runs through the client
 * {@link BackendCatalogs#forPlanning} chooses — the caller's own, by default — and a refusal from
 * it is answered with the backend's status rather than a 500. The count itself comes from the
 * index, which is built under the service identity whichever client loaded the table.
 */
public final class CountRoutes {
  private static final ObjectMapper MAPPER = new ObjectMapper();

  private final io.kahshe.format.FormatConfig format;
  private final BackendCatalogs catalogs;
  private final TermIndex termIndex;

  public CountRoutes(
      io.kahshe.format.FormatConfig format,
      BackendCatalogs catalogs,
      TermIndex termIndex) {
    this.format = format;
    this.catalogs = catalogs;
    this.termIndex = termIndex;
  }

  public PlanRoutes.Result count(
      String prefixRaw, String namespaceRaw, String tableRaw, byte[] body, String callerToken) {
    JsonNode request;
    try {
      request = MAPPER.readTree(body);
    } catch (Exception e) {
      return error(400, "invalid JSON body");
    }
    String column = request.path("column").asText("");
    String term = request.path("term").asText("");
    String prefix = request.path("prefix").asText("");
    if (column.isEmpty() || (term.isEmpty() == prefix.isEmpty())) {
      return error(400, "body must be {\"column\": ..., \"term\": ...} or {\"column\": ..., \"prefix\": ...}");
    }

    TableIdentifier ident =
        TableIdentifier.of(
            RESTUtil.decodeNamespace(namespaceRaw, IndexerService.NAMESPACE_SEPARATOR),
            java.net.URLDecoder.decode(tableRaw, StandardCharsets.UTF_8));
    Table table;
    try {
      table =
          catalogs
              .forPlanning(java.net.URLDecoder.decode(prefixRaw, StandardCharsets.UTF_8), callerToken)
              .catalog()
              .loadTable(ident);
    } catch (RuntimeException e) {
      PlanRoutes.Result refused = PlanRoutes.refused(e, ident);
      if (refused != null) {
        return refused;
      }
      throw e;
    }

    if (table.currentSnapshot() == null) {
      return new PlanRoutes.Result(
          200,
          ("{\"count\":0,\"exact\":true,\"coverage\":{\"files_total\":0,\"files_indexed\":0}}")
              .getBytes(StandardCharsets.UTF_8));
    }
    // fail closed: an absent summary key means we cannot prove there are no deletes
    String deleteFiles = table.currentSnapshot().summary().get("total-delete-files");
    if (deleteFiles == null || !"0".equals(deleteFiles)) {
      return error(422, "cannot prove table is delete-free; counts would be upper bounds — refusing");
    }

    var field = table.schema().findField(column);
    if (field == null) {
      return error(404, "no such column: " + column);
    }
    TermIndex.Loaded index = termIndex.forField(table, field.fieldId());
    if (index == null) {
      return error(404, "no term index for column " + column);
    }
    String token = null;
    String normalizedPrefix = null;
    if (!prefix.isEmpty()) {
      // every term under the prefix, summed: exact for the same reason one term is
      normalizedPrefix =
          index.contract().kind() == io.kahshe.analysis.analyzer.Analyzer.Kind.VALUE
              ? prefix
              : prefix.toLowerCase(java.util.Locale.ROOT);
      if (!index.contract().prefixable(normalizedPrefix)) {
        return error(422, "no term under this index's analyzer can start with '" + prefix + "'");
      }
    } else {
      // Under the index's analyzer: a tokens column counts one token, a whole-value column one value.
      List<String> tokens = index.contract().queryTerms(term);
      if (tokens.size() != 1) {
        return error(422, "only single-term queries are answerable exactly; got " + tokens.size() + " terms");
      }
      token = tokens.get(0);
    }
    // After the index is loaded, because the cap that decides this belongs to the index rather
    // than to this proxy: an index built under a larger cap holds tokens this deployment's default
    // would reject, and refusing them here would answer 422 for a term that is counted exactly.
    if (token != null && !index.contract().isIndexable(token)) {
      return error(
          422,
          "token is not indexable under this index's analyzer (" + index.analyzer()
              + "); it was never indexed, so it cannot be counted exactly");
    }
    long currentSnapshot = table.currentSnapshot().snapshotId();
    if (index.snapshotId() != currentSnapshot) {
      return error(
          422,
          "index covers snapshot " + index.snapshotId() + " but table is at " + currentSnapshot
              + "; rebuild the index — refusing to answer inexactly");
    }
    // A partial checkpoint counts a subset of the files. A file that left the table keeps its
    // ordinal so surviving bitmaps stay valid -- what makes pruning safe under retention -- but
    // total_count is a scalar that still includes its occurrences, and a bitmap cannot say how many
    // to subtract. One undercounts, the other overcounts; both would be labelled exact, so both
    // refuse.
    if (index.partial()) {
      return error(
          422,
          "the index is a checkpoint over part of the table's files (a build is running or died); "
              + "a count over them would be wrong labelled exact -- wait for the build to finish");
    }
    if (!index.countsExact()) {
      return error(
          422,
          "files have left the table since this index was built; occurrence counts would be "
              + "upper bounds — rebuild the index in full to answer exactly");
    }

    long count;
    int filesWithTerm;
    try {
      if (normalizedPrefix != null) {
        int cap = format.prefixMaxTerms();
        TermIndex.PrefixEntries run = termIndex.entriesForPrefix(table, index, normalizedPrefix, cap);
        if (run == null) {
          return error(
              422,
              "prefix '" + prefix + "' matches more than " + cap
                  + " terms (KAHSHE_PREFIX_MAX_TERMS); narrow it");
        }
        count = run.totalCount();
        filesWithTerm = run.ordinals().getCardinality();
      } else {
        TermIndex.Entry entry = termIndex.entriesFor(table, index, java.util.List.of(token)).get(token);
        count = entry == null ? 0L : entry.totalCount();
        filesWithTerm = entry == null ? 0 : entry.fileCount();
      }
    } catch (java.io.IOException | RuntimeException e) {
      // an unreadable leaf is not a count of zero: refuse rather than answer wrongly
      return error(503, "term index unreadable; cannot answer exactly");
    }
    ObjectNode response = MAPPER.createObjectNode();
    response.put("count", count);
    response.put("exact", true);
    ObjectNode coverage = response.putObject("coverage");
    coverage.put("files_total", index.files().size());
    coverage.put("files_indexed", index.files().size());
    coverage.put("files_with_term", filesWithTerm);
    coverage.put("snapshot_id", index.snapshotId());
    coverage.put("analyzer", index.analyzer());
    return new PlanRoutes.Result(200, response.toString().getBytes(StandardCharsets.UTF_8));
  }

  private static PlanRoutes.Result error(int code, String message) {
    return new PlanRoutes.Result(
        code,
        ("{\"error\":{\"message\":\"" + message.replace("\"", "'") + "\",\"type\":\"CountError\",\"code\":" + code + "}}")
            .getBytes(StandardCharsets.UTF_8));
  }
}
