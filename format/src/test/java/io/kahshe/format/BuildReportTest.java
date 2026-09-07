package io.kahshe.format;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.node.ObjectNode;
import java.io.InputStream;
import java.util.List;
import java.util.Map;
import org.junit.jupiter.api.Test;

/**
 * The build report document: what it writes parses back to itself, it carries every key the
 * committed schema requires (the schema is the contract a second implementation reads), and a
 * document from a newer format is refused rather than half-read.
 */
class BuildReportTest {
  private static BuildReport sample() {
    return new BuildReport("b2", "b1", "main", "logs.events", "events", "msg", 1, 42L,
        BuildReport.Kind.INCREMENTAL, "kahshe-ascii-v3-max256", "kahshe-grams-v2-n3",
        1_000L, 4_000L,
        Map.of("files-covered", 3L, "files-added", 1L, "files-departed", 0L, "rows", 10L, "gram-bytes", 512L),
        List.of("s3://t/data/f3.parquet"), List.of(), List.of("s3://i/term-v1-f1/aggregate-1-r00-x.parquet"),
        List.of("gram space saturated in 1 file(s)"),
        List.of(Map.of("rule", Map.of("id", "r1"), "file", Map.of("path", "s3://t/data/f3.parquet"))));
  }

  @Test
  void roundTrips() {
    BuildReport report = sample();
    BuildReport back = BuildReport.parse(report.toJson());
    assertEquals(report, back);
    JsonNode json = report.toJson();
    assertEquals("kahshe-build", json.path("report-type").asText());
    assertEquals(List.of("logs", "events"), List.of(json.path("table-name").path("namespace").get(0).asText(),
        json.path("table-name").path("namespace").get(1).asText()));
    assertEquals("bytes", json.path("metrics").path("gram-bytes").path("unit").asText());
    assertEquals(3000, json.path("metrics").path("build").path("total-duration").asLong(), "the timer is the build's wall clock");
  }

  @Test
  void carriesEveryKeyTheSchemaRequires() throws Exception {
    ObjectMapper mapper = new ObjectMapper();
    JsonNode schema;
    try (InputStream in = BuildReport.class.getResourceAsStream("build-report.schema.json")) {
      schema = mapper.readTree(in);
    }
    JsonNode json = sample().toJson();
    requireAll(schema, json, "");
    for (JsonNode counter : List.of("files-covered", "files-added", "files-departed").stream().map(k -> json.path("metrics").path(k)).toList()) {
      assertTrue(counter.has("unit") && counter.has("value"), "a counter is {unit, value}");
    }
    assertTrue(json.path("metrics").path("build").has("total-duration"), "a timer is {time-unit, count, total-duration}");
  }

  private static void requireAll(JsonNode schema, JsonNode doc, String at) {
    for (JsonNode key : schema.path("required")) {
      assertTrue(doc.has(key.asText()), "missing " + at + "/" + key.asText());
      JsonNode sub = schema.path("properties").path(key.asText());
      if (sub.has("required")) {
        requireAll(sub, doc.path(key.asText()), at + "/" + key.asText());
      }
    }
  }

  @Test
  void aNewerFormatIsRefused() {
    ObjectNode json = sample().toJson();
    json.put("format-version", BuildReport.FORMAT_VERSION + 1);
    assertThrows(IllegalStateException.class, () -> BuildReport.parse(json));
    json.put("format-version", BuildReport.FORMAT_VERSION);
    json.put("report-type", "scan-report");
    assertThrows(IllegalStateException.class, () -> BuildReport.parse(json), "another report type is not a build");
  }
}
