package io.kahshe.indexer.build;

import io.kahshe.format.type.bloom.IndexMeta;
import io.kahshe.format.IndexPaths;
import io.kahshe.format.type.term.TermIndexWriter;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertNotEquals;
import static org.junit.jupiter.api.Assertions.assertTrue;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import java.nio.file.Files;
import java.nio.file.Path;
import org.apache.iceberg.Table;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;
import io.kahshe.indexer.BuildConfig;
import io.kahshe.indexer.LocalTableFixture;

/**
 * Per-column and per-table tier toggles and bloom fpp, resolved column, then table, then the
 * deployment flag — with the deployment flag as a CEILING for tiers, never merely a default.
 *
 * <p>Each toggle test asserts through the artifact the build publishes, not through the resolver's
 * return value: the claim is that the property changes what is WRITTEN, and a knob test has to
 * show the two configurations differ — which is why every negative assertion here sits next to a
 * control that the artifact exists when the property is absent.
 */
class IndexSettingsTest {
  @TempDir Path tmp;
  @TempDir Path tmp2;

  private static JsonNode termSnapshot(Table table, BuildConfig config) throws Exception {
    int fieldId = table.schema().findField(LocalTableFixture.COLUMN).fieldId();
    Path meta =
        Path.of(
            TermIndexWriter.dir(IndexPaths.root(table, config.format().indexRoot()), fieldId)
                + "/index-metadata.json");
    return TermIndexWriter.snapshotNode(new ObjectMapper().readTree(Files.readString(meta)));
  }

  private static boolean hasAggregate(JsonNode termSnapshot) {
    return TermIndexWriter.aggregateLeaves(termSnapshot).stream()
        .anyMatch(leaf -> leaf != null && !leaf.isEmpty());
  }

  @Test
  void aColumnLevelTermToggleRemovesTheAggregateAndOnlyTheAggregate() throws Exception {
    BuildConfig config = LocalTableFixture.config();
    Table control = LocalTableFixture.createTable(tmp, "alpha bravo");
    IndexBuilder.buildColumn(control, LocalTableFixture.COLUMN, config);
    assertTrue(
        hasAggregate(termSnapshot(control, config)),
        "control: with no property the deployment default (term on) builds an aggregate");

    Table table = LocalTableFixture.createTable(tmp2, "alpha bravo");
    table.updateProperties()
        .set("kahshe.index." + LocalTableFixture.COLUMN + ".term-index", "false")
        .commit();
    table.refresh();
    IndexBuilder.buildColumn(table, LocalTableFixture.COLUMN, config);
    JsonNode snapshot = termSnapshot(table, config);
    assertFalse(hasAggregate(snapshot), "the per-column toggle removes the aggregate");
    assertTrue(
        snapshot.path("files").size() > 0,
        "coverage is still written: only token MATCH is given up, not substring pruning");
    assertTrue(
        snapshot.path("leaves").has("grams"),
        "the gram layer is untouched by the term toggle");
  }

  @Test
  void aColumnLevelGramToggleDropsTheGramLayerAndKeepsTheRest() throws Exception {
    BuildConfig config = LocalTableFixture.config();
    Table control = LocalTableFixture.createTable(tmp, "alpha bravo");
    IndexBuilder.buildColumn(control, LocalTableFixture.COLUMN, config);
    assertTrue(
        termSnapshot(control, config).path("leaves").has("grams"),
        "control: with no property the deployment default (gram on) writes a grams leaf");

    Table table = LocalTableFixture.createTable(tmp2, "alpha bravo");
    table.updateProperties()
        .set("kahshe.index." + LocalTableFixture.COLUMN + ".gram-index", "false")
        .commit();
    table.refresh();
    long[] result = IndexBuilder.buildColumn(table, LocalTableFixture.COLUMN, config);
    JsonNode snapshot = termSnapshot(table, config);
    assertFalse(
        snapshot.path("leaves").has("grams"),
        "the per-column toggle drops the gram layer; the files degrade to blooms");
    assertTrue(hasAggregate(snapshot), "the term aggregate is untouched by the gram toggle");
    assertTrue(result[1] > 0, "blooms are still built and written");
  }

  @Test
  void aColumnLevelPropertyBeatsATableLevelOne() throws Exception {
    BuildConfig config = LocalTableFixture.config();
    Table table = LocalTableFixture.createTable(tmp, "alpha bravo");
    table.updateProperties()
        .set("kahshe.index.term-index", "false")
        .set("kahshe.index." + LocalTableFixture.COLUMN + ".term-index", "true")
        .commit();
    table.refresh();
    IndexBuilder.buildColumn(table, LocalTableFixture.COLUMN, config);
    assertTrue(
        hasAggregate(termSnapshot(table, config)),
        "column scope wins over table scope, in that order and not alphabetically");
  }

  @Test
  void theDeploymentFlagIsACeilingAPropertyCannotRaise() throws Exception {
    // Term tier off at the deployment: BuildBudget budgeted no arenas for it and the serving
    // caches were sized without it, so a property saying "true" must not conjure the tier.
    BuildConfig config = LocalTableFixture.configWithoutTermIndex();
    Table table = LocalTableFixture.createTable(tmp, "alpha bravo");
    table.updateProperties()
        .set("kahshe.index." + LocalTableFixture.COLUMN + ".term-index", "true")
        .commit();
    table.refresh();
    IndexBuilder.buildColumn(table, LocalTableFixture.COLUMN, config);
    assertFalse(
        hasAggregate(termSnapshot(table, config)),
        "the deployment flag is a ceiling; the property narrows only");
  }

  @Test
  void aMalformedFlagFallsThroughToTheNextLevelRatherThanReadingAsFalse() throws Exception {
    // Boolean.parseBoolean would read the typo as false — a tier silently off. The resolver
    // refuses the level instead, so the table-level value still governs.
    BuildConfig config = LocalTableFixture.config();
    Table table = LocalTableFixture.createTable(tmp, "alpha bravo");
    table.updateProperties()
        .set("kahshe.index." + LocalTableFixture.COLUMN + ".term-index", "ture")
        .set("kahshe.index.term-index", "false")
        .commit();
    table.refresh();
    IndexBuilder.buildColumn(table, LocalTableFixture.COLUMN, config);
    assertFalse(
        hasAggregate(termSnapshot(table, config)),
        "the malformed column level is ignored and the table level (off) governs");
  }

  @Test
  void bloomFppResolvesPerColumnAndTheArtifactRecordsIt() throws Exception {
    BuildConfig config = LocalTableFixture.config();
    Table dense = LocalTableFixture.createTable(tmp, "alpha bravo charlie delta echo");
    long defaultBytes = IndexBuilder.buildColumn(dense, LocalTableFixture.COLUMN, config)[1];

    Table sparse = LocalTableFixture.createTable(tmp2, "alpha bravo charlie delta echo");
    sparse.updateProperties()
        .set("kahshe.index." + LocalTableFixture.COLUMN + ".bloom-fpp", "0.5")
        .commit();
    sparse.refresh();
    long cheapBytes = IndexBuilder.buildColumn(sparse, LocalTableFixture.COLUMN, config)[1];

    // The comparison that makes it a knob test: same values, same grams, only the property
    // differs — and the bytes
    // must differ in the direction the dial promises (higher fpp, smaller bloom).
    assertNotEquals(defaultBytes, cheapBytes, "the property must change what is written");
    assertTrue(
        cheapBytes < defaultBytes,
        "fpp 0.5 must write a smaller bloom than the 0.01 default; got "
            + cheapBytes + " vs " + defaultBytes);

    int fieldId = sparse.schema().findField(LocalTableFixture.COLUMN).fieldId();
    IndexMeta meta =
        IndexMeta.parse(
            new ObjectMapper()
                .readTree(
                    Files.readString(
                        Path.of(
                            IndexMeta.metaPath(
                                IndexPaths.root(sparse, config.format().indexRoot()), fieldId)))));
    assertEquals(0.5, meta.fpp, 1e-9, "the artifact records the fpp it was built under");
  }
}
