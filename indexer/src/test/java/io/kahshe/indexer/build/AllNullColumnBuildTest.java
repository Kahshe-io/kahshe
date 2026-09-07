package io.kahshe.indexer.build;

import io.kahshe.format.type.gram.GramIndex;
import io.kahshe.format.type.gram.GramIndexWriter;
import io.kahshe.format.IndexPaths;
import io.kahshe.format.IndexPruner;
import io.kahshe.format.type.term.TermIndex;
import io.kahshe.format.type.term.TermIndexWriter;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertTrue;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import io.kahshe.common.Metrics;
import java.nio.file.Path;
import org.apache.iceberg.Schema;
import org.apache.iceberg.Table;
import org.apache.iceberg.types.Types;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;
import io.kahshe.indexer.BuildConfig;
import io.kahshe.indexer.LocalTableFixture;

/**
 * A string column holding only nulls must build, and serve, like any other table.
 *
 * <p>The trap: zero non-null values leave the gram map empty, a zero-record grams leaf is never
 * physically created (Iceberg's parquet appender opens the file lazily, on the first row), so
 * {@code GramIndexWriter.writeLeaf} throws {@code NotFoundException} reading its own leaf's length
 * back and the build fails on EVERY observation — invariant-safe, since the table stays unindexed
 * and every file is kept, but a permanent crash-retry loop on legal input, with
 * {@code kahshe_index_max_behind_seconds} climbing on an innocent table. An all-EMPTY-STRING
 * column does not reach it (padding grams make the map non-empty); only all-null does.
 *
 * <p>An empty gram map is therefore treated exactly like a null one: no grams leaf, no
 * gram-coverage, CONTAINS served from blooms — the shape {@code GramIndex.token} already answers as
 * "absent" and the shape a gram-disabled build writes. Verified red against a writer without that
 * branch: both tests crash at the missing leaf inside {@code buildColumn} rather than reaching an
 * assertion.
 */
class AllNullColumnBuildTest {
  @TempDir Path tmp;

  private static final Schema SCHEMA =
      new Schema(
          Types.NestedField.optional(1, LocalTableFixture.COLUMN, Types.StringType.get()));

  @Test
  void anAllNullColumnBuildsAndPublishesACoveringIndexWithNoGramsLeaf() throws Exception {
    BuildConfig config = LocalTableFixture.config();
    Table table = LocalTableFixture.createTable(tmp, SCHEMA, new String[] {null});
    LocalTableFixture.appendFile(table, "f2.parquet", new String[] {null});
    table.refresh();
    assertNotNull(
        IndexBuilder.buildColumn(table, LocalTableFixture.COLUMN, config),
        "an all-null column is legal input; a build that cannot finish on it retries forever");

    // The chosen shape, pinned: no grams leaf and no gram-coverage. An empty-but-PRESENT leaf
    // would be the dangerous alternative -- the exact gram layer treats absence as proof from
    // from-ordinal on -- so the property worth asserting is that the metadata points at no leaf
    // that holds nothing, and the reader serves these files from blooms as it would for any
    // gram-less generation.
    int fieldId = table.schema().findField(LocalTableFixture.COLUMN).fieldId();
    JsonNode snapshot =
        TermIndexWriter.snapshotNode(
            new ObjectMapper()
                .readTree(
                    Path.of(
                            TermIndexWriter.dir(IndexPaths.root(table, config.format().indexRoot()), fieldId)
                                + "/index-metadata.json")
                        .toFile()));
    assertTrue(
        snapshot.path("leaves").path("grams").asText().isEmpty(),
        "no grams leaf may be recorded for a build that accumulated no grams");
    assertTrue(
        snapshot.path("gram-coverage").isMissingNode(),
        "and no gram-coverage: this is the same metadata a gram-disabled build writes");
    assertEquals(
        2,
        snapshot.path("files").size(),
        "both files are covered -- the build genuinely published, it did not bail early");

    // What serving looks like: a zero-token generation's aggregate list is all empty ranges, and
    // the reader DELIBERATELY treats that as "no term index to serve" and keeps every file (grep
    // "it wrote nothing" in TermIndex) -- the safe direction, costing a scan. Pruning both files
    // would also have been correct here (no null can match a token), but that is not the choice
    // the reader made, and this pins the choice so a change to it is a decision rather than
    // drift.
    Metrics metrics = new Metrics();
    IndexPruner pruner = new IndexPruner(new TermIndex(config.format(), metrics), metrics, config.format());
    var kept =
        pruner.prune(
            table,
            null,
            java.util.List.of(
                new IndexPruner.ContainsHint(
                    LocalTableFixture.COLUMN, "alpha", IndexPruner.HintKind.MATCH)),
            LocalTableFixture.planTasks(table));
    assertEquals(
        2,
        kept.size(),
        "a MATCH against a zero-term generation keeps every file: there is no term index to "
            + "serve, and absence of a tier never prunes");
  }

  @Test
  void aRealValuedFileAppendedAfterAnAllNullBuildIsServedExactly() throws Exception {
    BuildConfig config = LocalTableFixture.config();
    Table table = LocalTableFixture.createTable(tmp, SCHEMA, new String[] {null});
    assertNotNull(IndexBuilder.buildColumn(table, LocalTableFixture.COLUMN, config));

    // The incremental build reads the no-grams metadata the first build wrote, restarts gram
    // coverage at the new file's ordinal (the priorGramsLeaf-is-empty branch), and must both
    // finish and keep the file that matches -- the degrade-a-layer shape: the all-null file
    // sits outside gram coverage, and the matching file must survive anyway.
    String valued = LocalTableFixture.appendFile(table, "f2.parquet", "alpha bravo");
    table.refresh();
    assertNotNull(
        IndexBuilder.buildColumn(table, LocalTableFixture.COLUMN, config),
        "the incremental path must accept a prior generation that recorded no gram layer");

    Metrics metrics = new Metrics();
    IndexPruner pruner = new IndexPruner(new TermIndex(config.format(), metrics), metrics, config.format());
    var kept =
        pruner.prune(
            table,
            null,
            java.util.List.of(
                new IndexPruner.ContainsHint(
                    LocalTableFixture.COLUMN, "alpha", IndexPruner.HintKind.CONTAINS)),
            LocalTableFixture.planTasks(table));
    assertTrue(
        kept.stream().map(t -> t.file().location()).toList().contains(valued),
        "the file holding the substring must survive pruning (the one error class this system "
            + "may never make)");
  }
}
