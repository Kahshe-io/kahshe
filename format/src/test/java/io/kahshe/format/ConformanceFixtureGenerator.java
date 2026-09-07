package io.kahshe.format;

import static org.junit.jupiter.api.Assumptions.assumeTrue;

import com.fasterxml.jackson.databind.node.ArrayNode;
import com.fasterxml.jackson.databind.node.ObjectNode;
import io.kahshe.indexer.BuildConfig;
import io.kahshe.indexer.build.IndexBuilder;
import io.kahshe.indexer.LocalTableFixture;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.TreeSet;
import org.apache.iceberg.Table;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;
import io.kahshe.analysis.analyzer.Analyzer;
import io.kahshe.format.type.gram.Grams;
import io.kahshe.format.type.term.TermIndexWriter;

/**
 * Cuts the golden fixture. Runs only with {@code -Dkahshe.conformance.regenerate=true}; what it
 * writes is committed and then held by the reader, writer and analyzer tests. Regenerate when the
 * format changes on purpose, and review the diff of {@code expected.json} as a spec change.
 */
class ConformanceFixtureGenerator {
  @TempDir Path tmp;

  @Test
  void regenerate() throws Exception {
    assumeTrue(Boolean.getBoolean("kahshe.conformance.regenerate"), "set -Dkahshe.conformance.regenerate=true");
    Table table = ConformanceSupport.table(tmp);
    String root = "file:" + tmp.resolve("index");
    BuildConfig config = ConformanceSupport.config(root);
    IndexBuilder.buildColumn(table, LocalTableFixture.COLUMN, config);
    IndexBuilder.buildColumn(table, ConformanceSupport.LIST_COLUMN, config);
    int fieldId = ConformanceSupport.fieldId(table);
    int listFieldId = ConformanceSupport.listFieldId(table);

    Path index = Conformance.FIXTURE.resolve("index");
    if (Files.exists(index)) {
      try (var walk = Files.walk(index)) {
        walk.sorted(java.util.Comparator.reverseOrder()).forEach(p -> p.toFile().delete());
      }
    }
    // Two exports into one fixture directory: every artifact path is field-id keyed, so the two
    // columns land in their own subdirectories and materialize restores both without knowing there
    // are two.
    Conformance.export(table, root, fieldId, index);
    Conformance.export(table, root, listFieldId, index);

    ObjectNode expected = Conformance.MAPPER.createObjectNode();
    expected.put("format-version", TermIndexWriter.FORMAT_VERSION);
    expected.put("kahshe.format-version", TermIndexWriter.TIER_FORMAT_VERSION);
    Analyzer.Contract tokens = Analyzer.contract(Analyzer.Kind.TOKENS, Analyzer.DEFAULT_MAX_TOKEN_LEN);
    Analyzer.Contract value = Analyzer.contract(Analyzer.Kind.VALUE, Analyzer.DEFAULT_MAX_TOKEN_LEN);
    expected.put("analyzer", tokens.id());
    expected.put("grams", Grams.Contract.current(config.ngram()).id());
    ArrayNode queries = expected.putArray("queries");
    for (String[] q : ConformanceSupport.QUERIES) {
      Set<String> kept = ConformanceSupport.kept(table, config.format(), IndexPruner.HintKind.valueOf(q[0]), q[1]);
      ObjectNode node = queries.addObject();
      node.put("kind", q[0]);
      node.put("value", q[1]);
      ArrayNode files = node.putArray("kept");
      kept.forEach(files::add);
    }
    ObjectNode counts = expected.putObject("counts");
    for (String token : ConformanceSupport.COUNTS) {
      Map<String, Object> c = ConformanceSupport.count(table, config.format(), token);
      if (c == null) {
        counts.putNull(token);
      } else {
        counts.putObject(token).put("files", (Integer) c.get("files")).put("total", (Long) c.get("total"));
      }
    }
    ArrayNode listQueries = expected.putArray("list-queries");
    for (String[] q : ConformanceSupport.LIST_QUERIES) {
      Set<String> kept = ConformanceSupport.kept(table, config.format(),
          IndexPruner.HintKind.valueOf(q[0]), q[1], ConformanceSupport.LIST_COLUMN);
      ObjectNode node = listQueries.addObject();
      node.put("kind", q[0]);
      node.put("value", q[1]);
      ArrayNode files = node.putArray("kept");
      kept.forEach(files::add);
    }
    ObjectNode listCounts = expected.putObject("list-counts");
    for (String token : ConformanceSupport.LIST_COUNTS) {
      Map<String, Object> c =
          ConformanceSupport.count(table, config.format(), token, listFieldId);
      if (c == null) {
        listCounts.putNull(token);
      } else {
        listCounts.putObject(token).put("files", (Integer) c.get("files"))
            .put("total", (Long) c.get("total"));
      }
    }
    expected.put("list-analyzer", value.id());
    ObjectNode vectors = expected.putObject("analyzer-vectors");
    for (Analyzer.Contract c : List.of(tokens, value)) {
      ObjectNode byInput = vectors.putObject(c.id());
      for (String input : ConformanceSupport.VECTOR_INPUTS) {
        ArrayNode out = byInput.putArray(input);
        c.tokens(input).forEach(out::add);
      }
    }
    ObjectNode gramVectors = expected.putObject("gram-vectors");
    for (Grams.Contract c : List.of(Grams.Contract.current(3), Grams.Contract.current(4), Grams.Contract.v1())) {
      ObjectNode byInput = gramVectors.putObject(c.id());
      for (String input : ConformanceSupport.VECTOR_INPUTS) {
        ArrayNode out = byInput.putArray(input);
        new TreeSet<>(c.gramsOf(input)).forEach(out::add);
      }
    }
    Files.writeString(Conformance.FIXTURE.resolve("expected.json"),
        Conformance.MAPPER.writerWithDefaultPrettyPrinter().writeValueAsString(expected) + "\n", StandardCharsets.UTF_8);
  }
}
