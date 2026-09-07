package io.kahshe.format;

import static org.junit.jupiter.api.Assertions.assertEquals;

import com.fasterxml.jackson.databind.JsonNode;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.TreeSet;
import org.junit.jupiter.api.Test;
import io.kahshe.analysis.analyzer.Analyzer;
import io.kahshe.format.type.gram.Grams;

/**
 * The analyzer and gram contracts against the committed vectors: every input in
 * {@code expected.json} tokenizes and cuts as recorded, under every id. The cheapest third of
 * the fixture and the one that covers the forbidden class: a drift here desynchronises every
 * existing index from every new query.
 */
class ConformanceAnalyzerTest {
  @Test
  void everyContractReproducesItsVectors() throws Exception {
    JsonNode expected = Conformance.MAPPER.readTree(
        Files.readString(Conformance.FIXTURE.resolve("expected.json"), StandardCharsets.UTF_8));
    int checked = 0;
    for (Map.Entry<String, JsonNode> byId : expected.path("analyzer-vectors").properties()) {
      Analyzer.Contract c = Analyzer.contractOf(byId.getKey());
      for (Map.Entry<String, JsonNode> v : byId.getValue().properties()) {
        List<String> want = new ArrayList<>();
        v.getValue().forEach(t -> want.add(t.asText()));
        assertEquals(want, c.tokens(v.getKey()), byId.getKey() + " on " + v.getKey());
        checked++;
      }
    }
    for (Map.Entry<String, JsonNode> byId : expected.path("gram-vectors").properties()) {
      Grams.Contract c = Grams.Contract.of(byId.getKey());
      for (Map.Entry<String, JsonNode> v : byId.getValue().properties()) {
        TreeSet<String> want = new TreeSet<>();
        v.getValue().forEach(t -> want.add(t.asText()));
        assertEquals(want, new TreeSet<>(c.gramsOf(v.getKey())), byId.getKey() + " on " + v.getKey());
        checked++;
      }
    }
    assertEquals(40, checked, "two analyzer ids and three gram ids over eight inputs");
  }
}
