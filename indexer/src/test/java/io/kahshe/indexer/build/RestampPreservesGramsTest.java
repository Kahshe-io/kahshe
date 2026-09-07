package io.kahshe.indexer.build;

import io.kahshe.format.type.gram.GramIndex;
import io.kahshe.format.IndexPaths;
import io.kahshe.format.type.bloom.NgramBloom;
import io.kahshe.format.type.term.TermIndexWriter;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertTrue;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import io.kahshe.common.Metrics;
import java.nio.file.Path;
import org.apache.iceberg.Table;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;
import io.kahshe.indexer.BuildConfig;
import io.kahshe.indexer.LocalTableFixture;

/**
 * The no-new-files restamp path rewrites the parsed metadata tree; the gram nodes must survive
 * the round trip.
 */
class RestampPreservesGramsTest {
  @TempDir Path tmp;

  private static JsonNode termSnapshot(Table table, BuildConfig config) throws Exception {
    int fieldId = table.schema().findField(LocalTableFixture.COLUMN).fieldId();
    String path = TermIndexWriter.dir(IndexPaths.root(table, config.format().indexRoot()), fieldId)
        + "/index-metadata.json";
    try (var in = table.io().newInputFile(path).newStream()) {
      return TermIndexWriter.snapshotNode(new ObjectMapper().readTree(in));
    }
  }

  /**
   * A restamp over a legacy index migrates it off the postings and norms tiers.
   *
   * <p>A rebuild stops writing those leaves, so a from-scratch build is clean by construction —
   * which is exactly why the guard in TermIndexTest passes without proving anything about existing
   * indexes. The restamp path is different: it rewrites the PRIOR metadata document, so without a
   * migration it would carry a legacy document's postings and norms leaf lists forward untouched,
   * forever, and the leaves they name would be deleted by nothing on any code path. A table that is
   * only ever restamped is precisely the table nobody rebuilds, so the reclamation would never
   * reach it.
   */
  @Test
  void aRestampMigratesALegacyDocumentOffTheDeletedTiers() throws Exception {
    Table table = LocalTableFixture.createTable(tmp, "alpha bravo");
    BuildConfig config = LocalTableFixture.config();
    assertNotNull(IndexBuilder.buildColumn(table, LocalTableFixture.COLUMN, config));

    // forge a legacy document: leaf lists naming real files, and no spec fields
    int fieldId = table.schema().findField(LocalTableFixture.COLUMN).fieldId();
    String dir = TermIndexWriter.dir(IndexPaths.root(table, config.format().indexRoot()), fieldId);
    java.nio.file.Path legacyPostings = java.nio.file.Path.of(dir, "postings-1-o0.parquet");
    java.nio.file.Path legacyNorms = java.nio.file.Path.of(dir, "norms-1-o0.parquet");
    java.nio.file.Files.writeString(legacyPostings, "not really parquet, but it is an object");
    java.nio.file.Files.writeString(legacyNorms, "likewise");

    ObjectMapper mapper = new ObjectMapper();
    java.nio.file.Path metaPath = java.nio.file.Path.of(dir, "index-metadata.json");
    var root = (com.fasterxml.jackson.databind.node.ObjectNode) mapper.readTree(metaPath.toFile());
    var snapshot = (com.fasterxml.jackson.databind.node.ObjectNode) TermIndexWriter.snapshotNode(root);
    var leaves = (com.fasterxml.jackson.databind.node.ObjectNode) snapshot.path("leaves");
    leaves.putArray("postings").add(legacyPostings.toString());
    leaves.putArray("norms").add(legacyNorms.toString());
    root.remove("transform-function");
    root.remove("key-column-ids");
    java.nio.file.Files.writeString(metaPath, mapper.writeValueAsString(root));
    java.nio.file.Files.deleteIfExists(metaPath.resolveSibling("." + metaPath.getFileName() + ".crc"));

    // same snapshot, full coverage: this build restamps rather than rebuilding
    assertNotNull(IndexBuilder.buildColumn(table, LocalTableFixture.COLUMN, config));

    JsonNode after = termSnapshot(table, config);
    assertTrue(after.path("leaves").path("postings").isMissingNode(), "postings survived a restamp");
    assertTrue(after.path("leaves").path("norms").isMissingNode(), "norms survived a restamp");
    assertTrue(
        TermIndexWriter.aggregateLeaves(after).stream().anyMatch(leaf -> !leaf.isEmpty()),
        "the aggregate was lost");

    // the objects are reclaimed, not merely unlisted -- unlisted alone leaks them permanently
    assertFalse(java.nio.file.Files.exists(legacyPostings), "the orphaned postings leaf was leaked");
    assertFalse(java.nio.file.Files.exists(legacyNorms), "the orphaned norms leaf was leaked");

    // and the restamp is the migration point for the required spec fields too
    var migrated = mapper.readTree(metaPath.toFile());
    assertEquals("IDENTITY", migrated.path("transform-function").asText());
    assertEquals(fieldId, migrated.path("key-column-ids").get(0).asInt());
  }

  @Test
  void restampKeepsGramLeafAndCoverage() throws Exception {
    Table table = LocalTableFixture.createTable(tmp, "alpha bravo");
    BuildConfig config = LocalTableFixture.config();
    assertNotNull(IndexBuilder.buildColumn(table, LocalTableFixture.COLUMN, config));
    JsonNode before = termSnapshot(table, config);
    assertFalse(before.path("leaves").path("grams").asText().isEmpty());

    // same snapshot, full coverage: the second build only restamps the metadata
    assertNotNull(IndexBuilder.buildColumn(table, LocalTableFixture.COLUMN, config));
    JsonNode after = termSnapshot(table, config);
    assertEquals(
        before.path("leaves").path("grams").asText(), after.path("leaves").path("grams").asText());
    assertEquals(before.path("gram-coverage"), after.path("gram-coverage"));

    // and the layer still serves
    int fieldId = table.schema().findField(LocalTableFixture.COLUMN).fieldId();
    GramIndex.Loaded loaded = new GramIndex(config.format(), new Metrics()).forField(table, fieldId);
    assertNotNull(loaded);
    assertTrue(loaded.matches("alpha", NgramBloom.Mode.CONTAINS).contains(0));
  }
}
