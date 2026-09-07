package io.kahshe.indexer.build;

import io.kahshe.analysis.analyzer.Analyzer;
import io.kahshe.format.IndexPaths;
import io.kahshe.format.IndexPruner;
import io.kahshe.format.type.term.TermIndex;
import io.kahshe.format.type.term.TermIndexWriter;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.junit.jupiter.api.Assertions.assertTrue;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.node.ObjectNode;
import io.kahshe.common.Metrics;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.Map;
import org.apache.iceberg.Table;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;
import io.kahshe.indexer.BuildConfig;
import io.kahshe.indexer.LocalTableFixture;

/**
 * The token-length cap: where it comes from, and what happens to an index built under another one.
 *
 * <p>Indexability is part of the pinned contract, because an index built under one cap does not
 * hold what another would probe for and absence prunes. So the cap is written into the analyzer id
 * and read back off the index rather than taken from whatever the reading process is configured
 * with: the two ends agree because only one of them decides.
 */
class TokenLengthCapTest {
  @TempDir Path tmp;

  private static Path metaPath(Table table, BuildConfig config) {
    int fieldId = table.schema().findField(LocalTableFixture.COLUMN).fieldId();
    return Path.of(
        TermIndexWriter.dir(IndexPaths.root(table, config.format().indexRoot()), fieldId)
            + "/index-metadata.json");
  }

  /**
   * The cap resolves per column, then per table, then from the deployment default.
   *
   * <p>Per column because indexability is a property of what the column holds: one column of
   * free text and one of long opaque ids do not want the same answer, and `kahshe.index` already
   * names columns in a table property.
   */
  @Test
  void theCapResolvesPerColumnThenPerTableThenFromConfig() throws Exception {
    BuildConfig config = LocalTableFixture.config();
    Table table = LocalTableFixture.createTable(tmp, "alpha");

    assertEquals(
        config.maxTokenLength(),
        IndexBuilder.maxTokenLength(table, LocalTableFixture.COLUMN, config),
        "with nothing declared it is the deployment default");

    table.updateProperties().set("kahshe.index.max-token-length", "64").commit();
    table.refresh();
    assertEquals(64, IndexBuilder.maxTokenLength(table, LocalTableFixture.COLUMN, config));

    table.updateProperties()
        .set("kahshe.index." + LocalTableFixture.COLUMN + ".max-token-length", "12")
        .commit();
    table.refresh();
    assertEquals(
        12,
        IndexBuilder.maxTokenLength(table, LocalTableFixture.COLUMN, config),
        "the column's own setting wins over the table's");
    assertEquals(
        64,
        IndexBuilder.maxTokenLength(table, "other", config),
        "and does not leak to a column that did not ask for it");

    // A value nobody can honour falls back rather than failing the build, and WARNs -- an operator
    // who typed it and silently got the default would have no way to tell.
    table.updateProperties().set("kahshe.index.other.max-token-length", "banana").commit();
    table.refresh();
    assertEquals(64, IndexBuilder.maxTokenLength(table, "other", config));
  }

  /**
   * An index built under an analyzer family this reader does not know is refused, not
   * reinterpreted.
   *
   * <p>This is the property that makes changing the contract survivable: probing an index with
   * another analyzer's notion of indexability would ask for tokens that build never wrote.
   * Absence prunes, so that is a false negative. Refusing keeps every file — correct, unpruned,
   * and logged. The previous family, {@code kahshe-ascii-v1}, is the N-1 exception: its rule is
   * kept and it is read under it ({@code AnalyzerGenerationTest}).
   *
   * <p>Verified by breaking it: accepting any analyzer string and defaulting the cap makes this
   * return a usable index for a document from an unknown family.
   */
  @Test
  void anIndexFromAnotherAnalyzerFamilyIsRefusedRatherThanRead() throws Exception {
    BuildConfig config = LocalTableFixture.config();
    Table table = LocalTableFixture.createTable(tmp, "alpha bravo");
    assertNotNull(IndexBuilder.buildColumn(table, LocalTableFixture.COLUMN, config));

    Metrics metrics = new Metrics();
    TermIndex index = new TermIndex(config.format(), metrics);
    int fieldId = table.schema().findField(LocalTableFixture.COLUMN).fieldId();
    TermIndex.Loaded ok = index.forField(table, fieldId);
    assertNotNull(ok, "precondition: the index this build wrote is usable");
    assertEquals(config.maxTokenLength(), ok.maxTokenLen(), "and carries the cap it was built at");
    assertEquals(Analyzer.V3_ID_PREFIX + config.maxTokenLength(), ok.analyzer());

    // Rewrite the recorded analyzer to a family this reader has never heard of.
    Path meta = metaPath(table, config);
    ObjectMapper mapper = new ObjectMapper();
    ObjectNode root = (ObjectNode) mapper.readTree(meta.toFile());
    ((ObjectNode) TermIndexWriter.propertiesNode(root)).put("analyzer", "kahshe-ascii-v0");
    Files.write(meta, mapper.writeValueAsBytes(root));
    // Hadoop's local filesystem keeps a .crc sidecar, and rewriting the file behind its back makes
    // the next read fail on the CHECKSUM instead of the analyzer. Without this the assertion below
    // passes for entirely the wrong reason.
    Files.deleteIfExists(meta.resolveSibling("." + meta.getFileName() + ".crc"));

    assertNull(
        new TermIndex(config.format(), metrics).forField(table, fieldId),
        "an index from an unknown family was read as if it were v2; every token it never wrote "
            + "would read as absent, and absence prunes");
  }

  /** End to end: a token v1 excluded is in the current dictionary and prunes. */
  @Test
  void aLongNumericTokenIsIndexedAndPrunes() throws Exception {
    BuildConfig config = LocalTableFixture.config();
    Table table = LocalTableFixture.createTable(tmp, "order 88213347 shipped");
    String other = LocalTableFixture.appendFile(table, "f2.parquet", "order 99999999 shipped");
    table.refresh();
    assertNotNull(IndexBuilder.buildColumn(table, LocalTableFixture.COLUMN, config));

    Metrics metrics = new Metrics();
    IndexPruner pruner = new IndexPruner(new TermIndex(config.format(), metrics), metrics, config.format());
    var kept =
        pruner.prune(
            table,
            null,
            java.util.List.of(
                new IndexPruner.ContainsHint(
                    LocalTableFixture.COLUMN, "99999999", IndexPruner.HintKind.MATCH)),
            LocalTableFixture.planTasks(table));
    assertEquals(
        java.util.List.of(other),
        kept.stream().map(t -> t.file().location()).toList(),
        "an 8-digit id must now prune to the one file holding it; under v1 it was excluded from "
            + "the dictionary and every file was kept");
    assertTrue(Map.of().isEmpty());
  }
}
