package io.kahshe.proxy.plan;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.node.ObjectNode;
import io.kahshe.format.IndexPruner.ContainsHint;
import java.util.ArrayList;
import java.util.List;
import java.util.function.Predicate;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * kahshe filter extension: {@code {"type":"contains","term":"msg","value":"timeout"}}.
 *
 * <p>Iceberg's expression algebra has no substring predicate, so engines cannot push {@code LIKE
 * '%x%'} through the standard filter. kahshe accepts {@code contains} nodes in conjunctive
 * positions, strips them (replaced with {@code true}) before standard parsing, and applies them as
 * index-pruning hints. A {@code contains} under OR/NOT is rejected: replacing it there would
 * change scan semantics.
 */
public final class ContainsExtractor {
  private static final Logger LOG = LoggerFactory.getLogger(ContainsExtractor.class);
  private static final ObjectMapper MAPPER = new ObjectMapper();

  record Extraction(String cleanedJson, List<ContainsHint> hints) {}

  private ContainsExtractor() {}

  /**
   * Strips kahshe's filter extensions into hints and returns the standard JSON that is left.
   *
   * @param isRealColumn whether the table declares a column of exactly this name. Consulted only
   *     when a sentinel-shaped term appears, so an ordinary plan never pays for it;
   *     {@link #sentinelHint} says why a real column must win.
   */
  static Extraction extract(String rawJson, Predicate<String> isRealColumn) {
    try {
      ObjectNode root = (ObjectNode) MAPPER.readTree(rawJson);
      List<ContainsHint> hints = new ArrayList<>();
      JsonNode filter = root.get("filter");
      if (filter != null && !filter.isNull()) {
        root.set("filter", rewrite(filter, true, hints, isRealColumn));
      }
      return new Extraction(MAPPER.writeValueAsString(root), hints);
    } catch (ClassCastException | com.fasterxml.jackson.core.JacksonException e) {
      throw new IllegalArgumentException("invalid plan request JSON", e);
    }
  }

  private static JsonNode rewrite(
      JsonNode node,
      boolean conjunctive,
      List<ContainsHint> hints,
      Predicate<String> isRealColumn) {
    String type = node.path("type").asText("");
    if ("contains".equals(type) || "match".equals(type) || "match_prefix".equals(type)) {
      if (!conjunctive) {
        throw new IllegalArgumentException(
            "'" + type + "' is only supported in conjunctive positions (not under or/not)");
      }
      hints.add(
          new ContainsHint(
              node.path("term").asText(),
              node.path("value").asText(),
              switch (type) {
                case "match" -> io.kahshe.format.IndexPruner.HintKind.MATCH;
                case "match_prefix" -> io.kahshe.format.IndexPruner.HintKind.PREFIX;
                default -> io.kahshe.format.IndexPruner.HintKind.CONTAINS;
              }));
      // Iceberg's ExpressionParser represents alwaysTrue as the JSON literal `true`
      return MAPPER.getNodeFactory().booleanNode(true);
    }
    if ("apply".equals(type)) {
      // The draft expressions spec's function-application form (apache/iceberg#16961 App. B):
      // {"type":"apply","function":{"catalog":"iceberg_functions","identifier":["contains"]},
      //  "arguments":[{"type":"reference","id":5},"needle"]} — references are by field id.
      ContainsHint hint = applyHint(node);
      if (hint == null) {
        return node; // unknown function: leave it for the standard parser to accept or reject
      }
      if (!conjunctive) {
        throw new IllegalArgumentException(
            "text functions are only supported in conjunctive positions (not under or/not)");
      }
      hints.add(hint);
      return MAPPER.getNodeFactory().booleanNode(true);
    }
    JsonNode wrapped = wrappedApply(type, node);
    if (wrapped != null) {
      // The merged expressions spec's form (Appendix B): a value expression is not a predicate,
      // so a boolean function arrives compared -- eq(apply(...), true) -- with "left"/"right"
      // and, from a scan-planning client, a named reference. Only eq-to-true is a hint; any
      // other comparison of an apply is left for the standard parser, which refuses it loudly.
      ContainsHint hint = applyHint(wrapped);
      if (hint != null) {
        if (!conjunctive) {
          throw new IllegalArgumentException(
              "text functions are only supported in conjunctive positions (not under or/not)");
        }
        hints.add(hint);
        return MAPPER.getNodeFactory().booleanNode(true);
      }
    }
    ContainsHint sentinel = sentinelHint(type, node, isRealColumn);
    if (sentinel != null) {
      if (!conjunctive) {
        throw new IllegalArgumentException(
            "'" + node.path("term").asText()
                + "' is only supported in conjunctive positions (not under or/not)");
      }
      hints.add(sentinel);
      return MAPPER.getNodeFactory().booleanNode(true);
    }
    if ("and".equals(type)) {
      ObjectNode and = (ObjectNode) node;
      and.set("left", rewrite(and.get("left"), conjunctive, hints, isRealColumn));
      and.set("right", rewrite(and.get("right"), conjunctive, hints, isRealColumn));
      return and;
    }
    if (("or".equals(type) || "not".equals(type)) && extensionAnywhere(node, isRealColumn)) {
      throw new IllegalArgumentException(
          "'contains'/'match' are only supported in conjunctive positions (not under or/not)");
    }
    return node;
  }

  /**
   * The function catalog kahshe's own text predicates live under.
   *
   * <p>{@code iceberg_functions} is a reserved namespace and {@code contains}/{@code match} are not
   * in it, so reading kahshe's predicates from there would claim a name it has no right to and
   * collide the day Iceberg defines a function of that name with different semantics. The
   * expressions spec names the vendor-namespace pattern for exactly this.
   */
  public static final String FUNCTION_CATALOG = "kahshe_functions";

  /** Still accepted so plans written against the older form keep working; see FUNCTION_CATALOG. */
  private static final String LEGACY_FUNCTION_CATALOG = "iceberg_functions";

  /**
   * The apply node an {@code eq(apply, true)} comparison wraps, in the spec's "left"/"right" form
   * or the deprecated "term"/"value" form, with the boolean bare or as a literal object; null
   * for anything else, including eq-to-false and not-eq, which stay with the standard parser.
   */
  private static JsonNode wrappedApply(String type, JsonNode node) {
    if (!"eq".equals(type)) {
      return null;
    }
    JsonNode subject = node.has("left") ? node.get("left") : node.get("term");
    JsonNode object = unwrapLiteral(node.has("right") ? node.get("right") : node.get("value"));
    if (subject == null || !subject.isObject() || !"apply".equals(subject.path("type").asText())) {
      return null;
    }
    return object != null && object.isBoolean() && object.asBoolean() ? subject : null;
  }

  /** {@code {"type":"literal","value":X}} reads as X; anything else is itself. */
  private static JsonNode unwrapLiteral(JsonNode node) {
    if (node != null && node.isObject() && "literal".equals(node.path("type").asText())) {
      return node.get("value");
    }
    return node;
  }

  /** The column a comparison names: a "term" string, or a "left" reference by name. */
  private static String subjectName(JsonNode node) {
    if (node.path("term").isTextual()) {
      return node.path("term").asText();
    }
    JsonNode left = node.path("left");
    if (left.isObject() && "reference".equals(left.path("type").asText())) {
      return left.path("name").asText(left.path("term").asText(""));
    }
    return "";
  }

  /**
   * Maps a recognized text-function application to a hint; null for any other function, including
   * same-named functions from a catalog kahshe does not own (a foreign catalog's "match" may have
   * semantics the index must not prune on). Exactly two arguments — one field reference, one string
   * literal — are required; anything else is rejected rather than partially interpreted, because a
   * recognized node is replaced with `true` and any unparsed argument semantics would be
   * unrecoverable.
   */
  private static ContainsHint applyHint(JsonNode node) {
    String catalog = node.path("function").path("catalog").asText("");
    if (!FUNCTION_CATALOG.equals(catalog) && !LEGACY_FUNCTION_CATALOG.equals(catalog)) {
      return null;
    }
    JsonNode identifier = node.path("function").path("identifier");
    String name = identifier.isArray() && identifier.size() > 0
        ? identifier.get(identifier.size() - 1).asText()
        : "";
    io.kahshe.format.IndexPruner.HintKind kind =
        switch (name) {
          case "contains" -> io.kahshe.format.IndexPruner.HintKind.CONTAINS;
          case "match", "text_match" -> io.kahshe.format.IndexPruner.HintKind.MATCH;
          case "match_prefix" -> io.kahshe.format.IndexPruner.HintKind.PREFIX;
          default -> null;
        };
    if (kind == null) {
      return null;
    }
    JsonNode arguments = node.path("arguments");
    int fieldId = -1;
    String column = null;
    String value = null;
    boolean valid = arguments.isArray() && arguments.size() == 2;
    if (valid) {
      for (JsonNode argument : arguments) {
        JsonNode literal = unwrapLiteral(argument);
        if (argument.isObject() && "reference".equals(argument.path("type").asText())) {
          // by id (a stored, bound expression) or by name (a scan-planning client; also the
          // spec's deprecated "term" spelling of a name)
          valid &= fieldId < 0 && column == null;
          fieldId = argument.path("id").asInt(-1);
          column = argument.path("name").asText(argument.path("term").asText(null));
        } else if (literal != null && literal.isTextual()) {
          valid &= value == null;
          value = literal.asText();
        } else {
          valid = false;
        }
      }
    }
    if (!valid || (fieldId < 0 && (column == null || column.isEmpty())) || value == null) {
      throw new IllegalArgumentException(
          "'" + name + "' requires exactly a field reference and a string literal argument");
    }
    return new ContainsHint(fieldId < 0 ? column : null, fieldId, value, kind);
  }

  /**
   * Prefix marking a term name as a carrier rather than a column.
   *
   * <p>{@code Expressions.equal("__kahshe_match__msg", "token")} is an ordinary {@code
   * UnboundPredicate}, so every engine serializes it and it survives the wire unchanged. The prefix
   * must be stripped here, before {@code ExpressionParser} is asked to bind a name that is not a
   * column. It exists because iceberg-java's {@code ExpressionParser} has no {@code apply} node, so
   * an engine has no way to emit the function-reference form {@link #applyHint} parses.
   */
  private static final String SENTINEL_MATCH = "__kahshe_match__";

  private static final String SENTINEL_CONTAINS = "__kahshe_contains__";

  /**
   * A hint carried as {@code eq} on a sentinel term, or null if this is an ordinary predicate.
   *
   * <p>Only {@code eq} is accepted. The prefix alone could discriminate, but a sentinel arriving as
   * {@code lt} or {@code starts-with} means the producer and this reader disagree about the
   * encoding, and guessing at intent would replace the node with {@code true} — dropping a
   * predicate the engine believes it pushed. That is a false negative. Leaving it alone instead
   * sends a nonexistent column to {@code ExpressionParser}, which fails the plan loudly.
   */
  private static ContainsHint sentinelHint(
      String type, JsonNode node, Predicate<String> isRealColumn) {
    if (!"eq".equals(type)) {
      return null;
    }
    String term = subjectName(node);
    String column;
    io.kahshe.format.IndexPruner.HintKind kind;
    if (term.startsWith(SENTINEL_MATCH)) {
      column = term.substring(SENTINEL_MATCH.length());
      kind = io.kahshe.format.IndexPruner.HintKind.MATCH;
    } else if (term.startsWith(SENTINEL_CONTAINS)) {
      column = term.substring(SENTINEL_CONTAINS.length());
      kind = io.kahshe.format.IndexPruner.HintKind.CONTAINS;
    } else {
      return null;
    }
    // A real column of this name wins, and the sentinel loses. The prefix is a convention, not a
    // reservation, so nothing stops a table declaring `__kahshe_match__foo`; reading that as a hint
    // would prune by the term index of a different column, `foo`, and drop every file whose `foo`
    // lacks the token -- a false negative. Passing it through costs only pruning, since the node
    // binds against a column that genuinely exists. Logged because the symptom is otherwise
    // indistinguishable from data that is simply not selective.
    if (isRealColumn.test(term)) {
      LOG.warn(
          "table declares a real column named '{}', so it is being planned as an ordinary "
              + "predicate rather than as a kahshe {} hint; that column cannot be index-pruned "
              + "while it is named this",
          term, kind);
      return null;
    }
    JsonNode value = unwrapLiteral(node.has("value") ? node.get("value") : node.path("right"));
    if (column.isEmpty() || value == null || !value.isTextual() || value.asText().isEmpty()) {
      throw new IllegalArgumentException(
          "'" + term + "' requires a non-empty column suffix and a string value");
    }
    return new ContainsHint(column, value.asText(), kind);
  }

  /**
   * Whether a kahshe extension appears anywhere below {@code node}, used to refuse one under
   * OR/NOT.
   *
   * <p>It takes {@code isRealColumn} for the same reason {@link #sentinelHint} does, but getting
   * this half wrong fails the other way: a table with a real column named
   * {@code __kahshe_match__foo} could not put it under an OR at all, because the request would be
   * refused as a misplaced extension.
   */
  private static boolean extensionAnywhere(JsonNode node, Predicate<String> isRealColumn) {
    if (node.isObject()) {
      String t = node.path("type").asText("");
      if ("contains".equals(t) || "match".equals(t) || "match_prefix".equals(t)
          || ("apply".equals(t) && applyHint(node) != null)) {
        return true;
      }
      JsonNode wrapped = wrappedApply(t, node);
      if (wrapped != null && applyHint(wrapped) != null) {
        return true;
      }
      if (sentinelHint(t, node, isRealColumn) != null) {
        return true;
      }
    }
    for (JsonNode child : node) {
      if (extensionAnywhere(child, isRealColumn)) {
        return true;
      }
    }
    return false;
  }
}
