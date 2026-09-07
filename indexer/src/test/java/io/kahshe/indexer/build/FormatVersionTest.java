package io.kahshe.indexer.build;

import io.kahshe.format.type.gram.GramIndex;
import io.kahshe.format.type.bloom.IndexMeta;
import io.kahshe.format.IndexPaths;
import io.kahshe.format.IndexPruner;
import io.kahshe.format.type.term.TermIndex;
import io.kahshe.format.type.term.TermIndexWriter;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.node.ObjectNode;
import io.kahshe.common.Metrics;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.List;
import org.apache.iceberg.FileScanTask;
import org.apache.iceberg.Table;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;
import io.kahshe.indexer.BuildConfig;
import io.kahshe.indexer.LocalTableFixture;

/**
 * {@code format-version} is honoured, not merely written: a reader refuses a newer index and keeps
 * every file, and a builder refuses to build over one rather than downgrade it in place.
 *
 * <p>A field no reader branches on gains a future writer nothing, and leaves downgrade safety
 * resting on prose. The metadata is patched on disk to a version from the future; the {@code .crc}
 * sidecar is deleted first, because a patched file that fails its Hadoop checksum is refused for
 * the WRONG reason and would keep this test green with the check deleted. Verified red with the
 * reader's check removed: the absent token prunes every file.
 */
class FormatVersionTest {
  @TempDir Path tmp;

  private static void patchVersion(String path, int version) throws Exception {
    Path file = Path.of(path);
    ObjectNode root = (ObjectNode) new ObjectMapper().readTree(Files.readString(file));
    root.put("format-version", version);
    Files.writeString(file, root.toString());
    Files.deleteIfExists(file.resolveSibling("." + file.getFileName() + ".crc"));
  }

  private static List<FileScanTask> prune(
      Table table, BuildConfig config, IndexPruner.HintKind kind) throws Exception {
    Metrics metrics = new Metrics();
    IndexPruner pruner = new IndexPruner(new TermIndex(config.format(), metrics), metrics, config.format());
    return pruner.prune(
        table,
        null,
        List.of(new IndexPruner.ContainsHint(LocalTableFixture.COLUMN, "zzzabsent", kind)),
        LocalTableFixture.planTasks(table));
  }

  @Test
  void aNewerTermIndexIsRefusedByReadersAndByBuilders() throws Exception {
    BuildConfig config = LocalTableFixture.config();
    Table table = LocalTableFixture.createTable(tmp, "alpha bravo");
    IndexBuilder.buildColumn(table, LocalTableFixture.COLUMN, config);
    int fieldId = table.schema().findField(LocalTableFixture.COLUMN).fieldId();
    assertTrue(
        prune(table, config, IndexPruner.HintKind.MATCH).isEmpty(),
        "control: at the current version an absent token prunes the only file");
    assertNotNull(
        new GramIndex(config.format(), new Metrics()).forField(table, fieldId),
        "control: the gram tier loads from the same document");

    patchVersion(
        TermIndexWriter.dir(IndexPaths.root(table, config.format().indexRoot()), fieldId)
            + "/index-metadata.json",
        TermIndexWriter.FORMAT_VERSION + 1);

    assertEquals(
        1, prune(table, config, IndexPruner.HintKind.MATCH).size(),
        "a newer term index keeps every file rather than being misread");
    assertNull(
        new GramIndex(config.format(), new Metrics()).forField(table, fieldId),
        "the gram tier reads the same document and must refuse it the same way");
    assertThrows(
        IllegalStateException.class,
        () -> IndexBuilder.buildColumn(table, LocalTableFixture.COLUMN, config),
        "a builder must not rebuild over a newer index and downgrade it in place");
  }

  @Test
  void aNewerBloomIndexIsRefusedByReadersAndByBuilders() throws Exception {
    BuildConfig config = LocalTableFixture.config(false); // grams off: contains -> blooms only
    Table table = LocalTableFixture.createTable(tmp, "alpha bravo");
    IndexBuilder.buildColumn(table, LocalTableFixture.COLUMN, config);
    int fieldId = table.schema().findField(LocalTableFixture.COLUMN).fieldId();
    assertTrue(
        prune(table, config, IndexPruner.HintKind.CONTAINS).isEmpty(),
        "control: at the current version the blooms prune the only file");

    patchVersion(
        IndexMeta.metaPath(IndexPaths.root(table, config.format().indexRoot()), fieldId),
        IndexMeta.FORMAT_VERSION + 1);

    assertEquals(
        1, prune(table, config, IndexPruner.HintKind.CONTAINS).size(),
        "a newer bloom index keeps every file rather than being misread");
    assertThrows(
        IllegalStateException.class,
        () -> IndexBuilder.buildColumn(table, LocalTableFixture.COLUMN, config),
        "a builder must not rebuild over a newer bloom index");
  }
}
