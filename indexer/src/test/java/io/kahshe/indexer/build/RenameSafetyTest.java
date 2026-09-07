package io.kahshe.indexer.build;

import io.kahshe.format.IndexPaths;
import io.kahshe.format.type.term.TermIndexWriter;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertTrue;

import java.nio.file.Files;
import java.nio.file.Path;
import org.apache.iceberg.Table;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;
import io.kahshe.indexer.BuildConfig;
import io.kahshe.indexer.LocalTableFixture;

/**
 * Renaming an indexed column keeps its index maintained and its properties applied: configuration
 * is written by NAME, artifacts are keyed by field ID, and a rename that is not bridged between
 * the two ends maintenance in silence. Verified red with {@code IndexSettings.resolveField}
 * reduced to the current schema: the old-name build is refused and the old-name property falls
 * back to the deployment default.
 */
class RenameSafetyTest {
  @TempDir Path tmp;

  @Test
  void aRenamedColumnKeepsItsIndexAndItsProperties() throws Exception {
    BuildConfig config = LocalTableFixture.config();
    Table table = LocalTableFixture.createTable(tmp, "alpha bravo");
    table.updateProperties().set("kahshe.index.msg.max-token-length", "8").commit();
    table.refresh();
    assertNotNull(IndexBuilder.buildColumn(table, "msg", config));
    int fieldId = table.schema().findField("msg").fieldId();

    table.updateSchema().renameColumn("msg", "message").commit();
    table.refresh();

    // the property is still keyed by the old name, and still applies to the field
    assertEquals(
        8, IndexBuilder.maxTokenLength(table, "message", config),
        "a per-column property written under the old name follows the field id");
    // the kahshe.index entry still says the old name, and the build maintains the same index
    assertNotNull(
        IndexBuilder.buildColumn(table, "msg", config),
        "the old name resolves through the table's schema history to the renamed field");
    assertEquals(fieldId, table.schema().findField("message").fieldId());
    assertTrue(
        Files.exists(
            Path.of(
                TermIndexWriter.dir(IndexPaths.root(table, config.format().indexRoot()), fieldId)
                    + "/index-metadata.json")),
        "one index, under the field id, whichever name the property uses");
    assertNotNull(IndexBuilder.buildColumn(table, "message", config), "and the new name works");
  }
}
