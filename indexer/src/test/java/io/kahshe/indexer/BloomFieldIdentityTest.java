package io.kahshe.indexer;

import io.kahshe.format.type.bloom.IndexMeta;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNotEquals;
import static org.junit.jupiter.api.Assertions.assertTrue;

import java.nio.file.Path;
import org.apache.iceberg.Table;
import org.apache.iceberg.types.Types;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

/**
 * The bloom tier is keyed by Iceberg FIELD ID, not by column name.
 *
 * <p>A name is not an identity in Iceberg. It moves on rename, and it is reusable after a drop —
 * so a name-keyed index directory can be handed to a different column than the one it describes.
 * That matters here more than it would elsewhere, because blooms are keyed INSIDE by data-file
 * path, and paths survive schema changes. The old blooms would be found, would be believed, and
 * would prune files that genuinely match the new column: a FALSE NEGATIVE, which is the one class
 * of error advisory-keep forbids outright.
 *
 * <p>These tests pin the property at the path level, which is where the identity is decided.
 */
class BloomFieldIdentityTest {
  @TempDir Path tmp;

  @Test
  void twoColumnsSharingANameButNotAFieldDoNotShareADirectory() {
    // the exact hazard: drop `msg`, re-add `msg`, and Iceberg assigns a NEW field id
    String dropped = IndexMeta.dir("s3://x/_indexes/tbl", 5);
    String readded = IndexMeta.dir("s3://x/_indexes/tbl", 12);
    assertNotEquals(dropped, readded,
        "a re-added column reused the dropped column's bloom directory");
    assertTrue(dropped.endsWith("/ngram-bloom-f5"), dropped);
    assertTrue(readded.endsWith("/ngram-bloom-f12"), readded);
  }

  @Test
  void renamingAColumnDoesNotMoveItsIndex() throws Exception {
    Table table = LocalTableFixture.createTable(tmp, "alpha beta", "beta gamma");
    int before = table.schema().findField(LocalTableFixture.COLUMN).fieldId();

    table.updateSchema().renameColumn(LocalTableFixture.COLUMN, "message").commit();
    table.refresh();
    int after = table.schema().findField("message").fieldId();

    // Iceberg keeps the field id across a rename; that is exactly why keying on it is correct
    assertEquals(before, after, "Iceberg changed a field id on rename");
    assertEquals(
        IndexMeta.dir("root", before),
        IndexMeta.dir("root", after),
        "a rename moved the index directory, so the built index would be lost");
  }

  @Test
  void theMetadataAndLeafPathsAgreeWithTheDirectory() {
    String dir = IndexMeta.dir("root", 7);
    assertEquals(dir + "/index-metadata.json", IndexMeta.metaPath("root", 7));
    assertEquals(dir + "/leaf-99-abc123.parquet", IndexMeta.leafPath("root", 7, 99L, "abc123"));
  }

  @Test
  void aDroppedColumnLeavesNoIndexRatherThanTheWrongOne() throws Exception {
    Table table = LocalTableFixture.createTable(tmp, "alpha beta");
    table.updateSchema().addColumn("other", Types.StringType.get()).commit();
    table.refresh();

    Types.NestedField msg = table.schema().findField(LocalTableFixture.COLUMN);
    Types.NestedField other = table.schema().findField("other");
    assertNotEquals(msg.fieldId(), other.fieldId());
    assertNotEquals(
        IndexMeta.dir("root", msg.fieldId()),
        IndexMeta.dir("root", other.fieldId()),
        "two distinct columns resolved to one bloom directory");
  }
}
