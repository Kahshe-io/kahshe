package io.kahshe.format;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.node.ArrayNode;
import com.fasterxml.jackson.databind.node.ObjectNode;
import java.io.IOException;
import java.io.InputStream;
import java.io.UncheckedIOException;
import java.nio.charset.StandardCharsets;
import java.util.ArrayList;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import org.apache.iceberg.io.FileIO;
import org.apache.iceberg.io.InputFile;
import org.apache.iceberg.io.PositionOutputStream;
import io.kahshe.format.type.term.TermIndexWriter;

/**
 * The record of one build, written beside the term metadata after every publish and read from
 * outside the builder's process: a watcher delivering the alerts the build raised, an operator, a
 * script, a second implementation writing the same document. One document per column, overwritten
 * at each publish; {@code build-id} and {@code previous-build-id} chain the builds, so a reader can
 * tell a report it missed from one it has seen.
 *
 * <p>The numbers use the Iceberg REST metrics vocabulary -- {@code report-type},
 * {@code table-name}, {@code snapshot-id}, a {@code metrics} map of counters and timers, a
 * {@code metadata} map -- so anything that reads Iceberg scan and commit reports reads these; the
 * lists and the alerts are kahshe's own. The shape is pinned by {@code build-report.schema.json}
 * beside this class.
 */
public record BuildReport(
    String buildId,
    String previousBuildId,
    String prefix,
    String namespace,
    String table,
    String column,
    int fieldId,
    long snapshotId,
    Kind kind,
    String analyzer,
    String grams,
    long startedMs,
    long publishedMs,
    Map<String, Long> counters,
    List<String> added,
    List<String> departed,
    List<String> leaves,
    List<String> warnings,
    List<Map<String, Object>> alerts) {

  public static final int FORMAT_VERSION = 1;
  public static final String REPORT_TYPE = "kahshe-build";
  public static final String FILE_NAME = "build-report.json";
  private static final ObjectMapper MAPPER = new ObjectMapper();

  public enum Kind {
    FULL,
    INCREMENTAL,
    RESTAMP
  }

  /** Where the report lives: beside the term metadata of {@code fieldId}. */
  public static String path(String indexRoot, int fieldId) {
    return TermIndexWriter.dir(indexRoot, fieldId) + "/" + FILE_NAME;
  }

  public ObjectNode toJson() {
    ObjectNode root = MAPPER.createObjectNode();
    root.put("format-version", FORMAT_VERSION);
    root.put("report-type", REPORT_TYPE);
    ObjectNode name = root.putObject("table-name");
    ArrayNode ns = name.putArray("namespace");
    if (!namespace.isEmpty()) {
      for (String part : namespace.split("\\.")) {
        ns.add(part);
      }
    }
    name.put("name", table);
    root.put("snapshot-id", snapshotId);
    ObjectNode metrics = root.putObject("metrics");
    for (Map.Entry<String, Long> c : counters.entrySet()) {
      ObjectNode counter = metrics.putObject(c.getKey());
      counter.put("unit", c.getKey().endsWith("-bytes") ? "bytes" : "count");
      counter.put("value", c.getValue());
    }
    ObjectNode timer = metrics.putObject("build");
    timer.put("time-unit", "milliseconds");
    timer.put("count", 1);
    timer.put("total-duration", Math.max(0, publishedMs - startedMs));
    ObjectNode metadata = root.putObject("metadata");
    metadata.put("build-id", buildId);
    if (previousBuildId != null) {
      metadata.put("previous-build-id", previousBuildId);
    }
    metadata.put("prefix", prefix);
    metadata.put("column", column);
    metadata.put("field-id", String.valueOf(fieldId));
    metadata.put("kind", kind.name());
    metadata.put("analyzer", analyzer);
    metadata.put("grams", grams);
    metadata.put("started-ms", String.valueOf(startedMs));
    metadata.put("published-ms", String.valueOf(publishedMs));
    ObjectNode files = root.putObject("files");
    added.forEach(files.putArray("added")::add);
    departed.forEach(files.putArray("departed")::add);
    leaves.forEach(root.putArray("leaves")::add);
    warnings.forEach(root.putArray("warnings")::add);
    ArrayNode alertsNode = root.putArray("alerts");
    for (Map<String, Object> alert : alerts) {
      alertsNode.add(MAPPER.valueToTree(alert));
    }
    return root;
  }

  /** Reads a report; a document newer than this reader is refused, as the tiers' are. */
  public static BuildReport parse(JsonNode root) {
    int version = root.path("format-version").asInt(0);
    if (version > FORMAT_VERSION) {
      throw new IllegalStateException(
          "build report format-version " + version + " is newer than this reader (" + FORMAT_VERSION + ")");
    }
    if (!REPORT_TYPE.equals(root.path("report-type").asText())) {
      throw new IllegalStateException("not a build report: report-type " + root.path("report-type").asText());
    }
    JsonNode metadata = root.path("metadata");
    List<String> ns = new ArrayList<>();
    root.path("table-name").path("namespace").forEach(n -> ns.add(n.asText()));
    Map<String, Long> counters = new LinkedHashMap<>();
    root.path("metrics").properties().forEach(e -> {
      if (e.getValue().has("value")) {
        counters.put(e.getKey(), e.getValue().path("value").asLong());
      }
    });
    List<Map<String, Object>> alerts = new ArrayList<>();
    for (JsonNode alert : root.path("alerts")) {
      @SuppressWarnings("unchecked")
      Map<String, Object> map = MAPPER.convertValue(alert, Map.class);
      alerts.add(map);
    }
    return new BuildReport(
        metadata.path("build-id").asText(),
        metadata.path("previous-build-id").asText(null),
        metadata.path("prefix").asText(""),
        String.join(".", ns),
        root.path("table-name").path("name").asText(),
        metadata.path("column").asText(),
        Integer.parseInt(metadata.path("field-id").asText("0")),
        root.path("snapshot-id").asLong(),
        Kind.valueOf(metadata.path("kind").asText("FULL")),
        metadata.path("analyzer").asText(null),
        metadata.path("grams").asText(null),
        Long.parseLong(metadata.path("started-ms").asText("0")),
        Long.parseLong(metadata.path("published-ms").asText("0")),
        counters,
        strings(root.path("files").path("added")),
        strings(root.path("files").path("departed")),
        strings(root.path("leaves")),
        strings(root.path("warnings")),
        alerts);
  }

  private static List<String> strings(JsonNode array) {
    List<String> out = new ArrayList<>();
    array.forEach(n -> out.add(n.asText()));
    return out;
  }

  /** Writes the report at {@link #path}, overwriting the previous build's. */
  public void write(FileIO io, String indexRoot) {
    try (PositionOutputStream out = io.newOutputFile(path(indexRoot, fieldId)).createOrOverwrite()) {
      out.write(MAPPER.writerWithDefaultPrettyPrinter().writeValueAsBytes(toJson()));
    } catch (IOException e) {
      throw new UncheckedIOException(e);
    }
  }

  /** The report at {@link #path}, or null when no build has written one. */
  public static BuildReport read(FileIO io, String indexRoot, int fieldId) {
    InputFile file = io.newInputFile(path(indexRoot, fieldId));
    if (!file.exists()) {
      return null;
    }
    try (InputStream in = file.newStream()) {
      return parse(MAPPER.readTree(new String(in.readAllBytes(), StandardCharsets.UTF_8)));
    } catch (IOException e) {
      throw new UncheckedIOException(e);
    }
  }
}
