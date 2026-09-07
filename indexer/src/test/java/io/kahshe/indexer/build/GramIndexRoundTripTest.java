package io.kahshe.indexer.build;

import io.kahshe.format.type.gram.GramIndex;
import io.kahshe.format.IndexPaths;
import io.kahshe.format.type.bloom.NgramBloom;
import io.kahshe.format.type.term.TermIndexWriter;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.junit.jupiter.api.Assertions.assertTrue;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import io.kahshe.common.Metrics;
import java.nio.file.Path;
import java.util.List;
import org.apache.iceberg.Table;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;
import org.roaringbitmap.RoaringBitmap;
import io.kahshe.indexer.BuildConfig;
import io.kahshe.indexer.LocalTableFixture;
import io.kahshe.indexer.RecordingListener;

/** Build -> probe round trips for the exact gram layer, over the HadoopTables fixture. */
class GramIndexRoundTripTest {
  @TempDir Path tmp;

  private static JsonNode termSnapshot(Table table, BuildConfig config) throws Exception {
    int fieldId = table.schema().findField(LocalTableFixture.COLUMN).fieldId();
    String path = TermIndexWriter.dir(IndexPaths.root(table, config.format().indexRoot()), fieldId)
        + "/index-metadata.json";
    try (var in = table.io().newInputFile(path).newStream()) {
      return TermIndexWriter.snapshotNode(new ObjectMapper().readTree(in));
    }
  }

  private static GramIndex.Loaded load(Table table, BuildConfig config) {
    int fieldId = table.schema().findField(LocalTableFixture.COLUMN).fieldId();
    return new GramIndex(config.format(), new Metrics()).forField(table, fieldId);
  }

  @Test
  void buildThenProbe() throws Exception {
    Table table = LocalTableFixture.createTable(tmp, "connection timeout on node", "ab");
    BuildConfig config = LocalTableFixture.config();
    assertNotNull(IndexBuilder.buildColumn(table, LocalTableFixture.COLUMN, config));

    GramIndex.Loaded loaded = load(table, config);
    assertNotNull(loaded);
    assertEquals(0, loaded.fromOrdinal());

    // present literal: conjunction over its sliding grams hits the file
    RoaringBitmap present = loaded.matches("timeout", NgramBloom.Mode.CONTAINS);
    assertNotNull(present);
    assertTrue(present.contains(0));

    // absent gram = empty bitmap: the layer is exact, the file is provably out
    assertTrue(loaded.matches("zzqxy", NgramBloom.Mode.CONTAINS).isEmpty());

    // short literals: CONTAINS/STARTS_WITH cannot prune, EQ probes the whole-value key
    assertNull(loaded.matches("ab", NgramBloom.Mode.CONTAINS));
    assertNull(loaded.matches("ab", NgramBloom.Mode.STARTS_WITH));
    assertTrue(loaded.matches("ab", NgramBloom.Mode.EQ).contains(0));
    assertTrue(loaded.matches("zz", NgramBloom.Mode.EQ).isEmpty());
  }

  @Test
  void incrementalAppendKeepsPriorCoverage() throws Exception {
    Table table = LocalTableFixture.createTable(tmp, "alpha bravo");
    BuildConfig config = LocalTableFixture.config();
    assertNotNull(IndexBuilder.buildColumn(table, LocalTableFixture.COLUMN, config));
    LocalTableFixture.appendFile(table, "f2.parquet", "charlie delta");
    assertNotNull(IndexBuilder.buildColumn(table, LocalTableFixture.COLUMN, config));

    assertEquals(0, termSnapshot(table, config).path("gram-coverage").path("from-ordinal").asInt());
    GramIndex.Loaded loaded = load(table, config);
    assertEquals(0, loaded.fromOrdinal());
    RoaringBitmap oldFile = loaded.matches("alpha", NgramBloom.Mode.CONTAINS);
    assertEquals(1, oldFile.getCardinality());
    assertTrue(oldFile.contains(0));
    RoaringBitmap newFile = loaded.matches("charlie", NgramBloom.Mode.CONTAINS);
    assertEquals(1, newFile.getCardinality());
    assertTrue(newFile.contains(1));
  }

  @Test
  void priorBuildWithoutGramsCoversOnlyNewOrdinals() throws Exception {
    Table table = LocalTableFixture.createTable(tmp, "alpha bravo");
    assertNotNull(
        IndexBuilder.buildColumn(table, LocalTableFixture.COLUMN, LocalTableFixture.config(false)));
    BuildConfig config = LocalTableFixture.config();
    assertTrue(termSnapshot(table, config).path("leaves").path("grams").isMissingNode());

    LocalTableFixture.appendFile(table, "f2.parquet", "charlie delta");
    assertNotNull(IndexBuilder.buildColumn(table, LocalTableFixture.COLUMN, config));

    // coverage starts where the prior (gram-less) build left off
    assertEquals(1, termSnapshot(table, config).path("gram-coverage").path("from-ordinal").asInt());
    GramIndex.Loaded loaded = load(table, config);
    assertEquals(1, loaded.fromOrdinal());
    assertTrue(loaded.matches("charlie", NgramBloom.Mode.CONTAINS).contains(1));
    assertTrue(loaded.matches("alpha", NgramBloom.Mode.CONTAINS).isEmpty());
  }

  @Test
  void unreadablePriorGramsLeafDegradesWithoutForcingFullRebuild() throws Exception {
    Table table = LocalTableFixture.createTable(tmp, "alpha bravo");
    BuildConfig config = LocalTableFixture.config();
    assertNotNull(IndexBuilder.buildColumn(table, LocalTableFixture.COLUMN, config));
    String priorGramsLeaf = termSnapshot(table, config).path("leaves").path("grams").asText();
    table.io().deleteFile(priorGramsLeaf);

    String newPath = LocalTableFixture.appendFile(table, "f2.parquet", "charlie delta");
    RecordingListener listener =
        new RecordingListener();
    assertNotNull(IndexBuilder.buildColumn(
        table, LocalTableFixture.COLUMN, config, listener, "p", "ns", "t"));

    // still incremental — only the new file was read; gram coverage restarted at its ordinal
    assertEquals(IndexBuildListener.BuildKind.INCREMENTAL, listener.kind);
    assertEquals(List.of(newPath), listener.files);
    assertEquals(1, termSnapshot(table, config).path("gram-coverage").path("from-ordinal").asInt());
    GramIndex.Loaded loaded = load(table, config);
    assertTrue(loaded.matches("charlie", NgramBloom.Mode.CONTAINS).contains(1));
    assertTrue(loaded.matches("alpha", NgramBloom.Mode.CONTAINS).isEmpty());
  }

  @Test
  void fullRebuildFallbackClearsGramState() throws Exception {
    Table table = LocalTableFixture.createTable(tmp, "alpha bravo");
    BuildConfig config = LocalTableFixture.config();
    long[] result = IndexBuilder.buildColumn(table, LocalTableFixture.COLUMN, config);
    assertNotNull(result);

    LocalTableFixture.appendFile(table, "f2.parquet", "charlie delta");
    // an unreadable aggregate forces the full-rebuild fallback; gram state must reset with it
    int fieldId = table.schema().findField(LocalTableFixture.COLUMN).fieldId();
    String root = IndexPaths.root(table, config.format().indexRoot());
    table.io().deleteFile(TermIndexWriter.dir(root, fieldId) + "/aggregate-" + result[0] + ".parquet");
    assertNotNull(IndexBuilder.buildColumn(table, LocalTableFixture.COLUMN, config));

    assertEquals(0, termSnapshot(table, config).path("gram-coverage").path("from-ordinal").asInt());
    GramIndex.Loaded loaded = load(table, config);
    assertEquals(0, loaded.fromOrdinal());
    assertEquals(1, loaded.matches("alpha", NgramBloom.Mode.CONTAINS).getCardinality());
    assertEquals(1, loaded.matches("charlie", NgramBloom.Mode.CONTAINS).getCardinality());
  }
}
