package io.kahshe.format;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNull;

import com.fasterxml.jackson.databind.JsonNode;
import io.kahshe.indexer.BuildConfig;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.Map;
import java.util.Set;
import java.util.TreeSet;
import org.apache.iceberg.Table;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;
import io.kahshe.format.type.bloom.IndexMeta;
import io.kahshe.format.type.term.TermIndexWriter;

/**
 * The readers against the committed bytes: every query in {@code expected.json} answers the same
 * from the golden artifact, and a document one format version newer is refused into keeping
 * every file. A reader change that moves any answer fails here; so does a fixture cut under a
 * changed format without the manifest being reviewed. Verified red with the compound emission
 * removed from the analyzer: the match on the IP kept nothing.
 */
class ConformanceReaderTest {
  @TempDir Path tmp;

  @Test
  void theGoldenArtifactAnswersTheManifest() throws Exception {
    Table table = ConformanceSupport.table(tmp);
    String root = "file:" + tmp.resolve("index");
    BuildConfig config = ConformanceSupport.config(root);
    Conformance.materialize(Conformance.FIXTURE.resolve("index"), table, root);
    JsonNode expected = Conformance.MAPPER.readTree(
        Files.readString(Conformance.FIXTURE.resolve("expected.json"), StandardCharsets.UTF_8));

    for (JsonNode q : expected.path("queries")) {
      Set<String> want = new TreeSet<>();
      q.path("kept").forEach(f -> want.add(f.asText()));
      Set<String> got = ConformanceSupport.kept(
          table, config.format(), IndexPruner.HintKind.valueOf(q.path("kind").asText()), q.path("value").asText());
      assertEquals(want, got, q.path("kind").asText() + " " + q.path("value").asText());
    }
    // The bloom tier on its own (gram tier off): probabilistic, so a superset of the manifest --
    // advisory keep -- and never a subset, which is what would drop a true match.
    FormatConfig bloomOnly = io.kahshe.common.Records.with(config.format(), Map.of("gramIndexEnabled", false));
    for (JsonNode q : expected.path("queries")) {
      if (!q.path("kind").asText().equals("CONTAINS")) {
        continue;
      }
      Set<String> want = new TreeSet<>();
      q.path("kept").forEach(f -> want.add(f.asText()));
      Set<String> got = ConformanceSupport.kept(table, bloomOnly, IndexPruner.HintKind.CONTAINS, q.path("value").asText());
      org.junit.jupiter.api.Assertions.assertTrue(got.containsAll(want),
          "bloom-only " + q.path("value").asText() + ": kept " + got + " must cover " + want);
    }
    for (Map.Entry<String, JsonNode> c : expected.path("counts").properties()) {
      Map<String, Object> got = ConformanceSupport.count(table, config.format(), c.getKey());
      if (c.getValue().isNull()) {
        assertNull(got, c.getKey());
      } else {
        assertEquals(c.getValue().path("files").asInt(), got.get("files"), c.getKey() + " files");
        assertEquals(c.getValue().path("total").asLong(), got.get("total"), c.getKey() + " total");
      }
    }

    // The list column: LIST_QUERIES pins that members are analysed one at a time, LIST_COUNTS that
    // a row counts a value once.
    int listFieldId = ConformanceSupport.listFieldId(table);
    for (JsonNode q : expected.path("list-queries")) {
      Set<String> want = new TreeSet<>();
      q.path("kept").forEach(f -> want.add(f.asText()));
      Set<String> got = ConformanceSupport.kept(table, config.format(),
          IndexPruner.HintKind.valueOf(q.path("kind").asText()), q.path("value").asText(),
          ConformanceSupport.LIST_COLUMN);
      assertEquals(want, got,
          "list " + q.path("kind").asText() + " " + q.path("value").asText());
    }
    for (Map.Entry<String, JsonNode> c
        : expected.path("list-counts").properties()) {
      Map<String, Object> got =
          ConformanceSupport.count(table, config.format(), c.getKey(), listFieldId);
      if (c.getValue().isNull()) {
        assertNull(got, "list " + c.getKey());
      } else {
        assertEquals(c.getValue().path("files").asInt(), got.get("files"),
            "list " + c.getKey() + " files");
        assertEquals(c.getValue().path("total").asLong(), got.get("total"),
            "list " + c.getKey() + " total");
      }
    }
  }

  @Test
  void aDocumentOneVersionNewerKeepsEveryFile() throws Exception {
    Table table = ConformanceSupport.table(tmp);
    String root = "file:" + tmp.resolve("index");
    BuildConfig config = ConformanceSupport.config(root);
    Conformance.materialize(Conformance.FIXTURE.resolve("index"), table, root);
    String dir = Conformance.indexDir(table, root).replace("file:", "");
    int fieldId = ConformanceSupport.fieldId(table);
    for (String doc : new String[] {
        TermIndexWriter.dir(dir, fieldId) + "/index-metadata.json", IndexMeta.metaPath(dir, fieldId)}) {
      Path p = Path.of(doc);
      String text = Files.readString(p, StandardCharsets.UTF_8);
      Files.writeString(p, text.replace("\"format-version\" : 1", "\"format-version\" : 2"), StandardCharsets.UTF_8);
      Files.deleteIfExists(p.resolveSibling("." + p.getFileName() + ".crc"));
    }
    Set<String> all = Set.of("f1.parquet", "f2.parquet", "f3.parquet");
    assertEquals(all, ConformanceSupport.kept(table, config.format(), IndexPruner.HintKind.MATCH, "zzz"),
        "a newer term document is refused, not read: every file kept");
    assertEquals(all, ConformanceSupport.kept(table, config.format(), IndexPruner.HintKind.CONTAINS, "zzq"),
        "a newer bloom document is refused, not read: every file kept");
  }
}
