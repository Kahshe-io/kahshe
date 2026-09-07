package io.kahshe.format.type.term;

import io.kahshe.indexer.BuildConfig;
import io.kahshe.indexer.build.IndexBuilder;
import io.kahshe.indexer.LocalTableFixture;
import io.kahshe.format.IndexPaths;
import io.kahshe.format.IndexPruner;
import io.kahshe.format.type.term.TermIndex;
import io.kahshe.format.type.term.TermIndexWriter;
import io.kahshe.format.type.term.TermRanges;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNotEquals;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.junit.jupiter.api.Assertions.assertTrue;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.node.ArrayNode;
import com.fasterxml.jackson.databind.node.ObjectNode;
import io.kahshe.common.Metrics;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.List;
import org.apache.iceberg.FileScanTask;
import org.apache.iceberg.Table;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

/**
 * The term tier's two join keys — the range a token routes to, and the token a resolved lookup is
 * cached under — must both name the thing they think they name.
 *
 * <p>Both defects tested here produce the same outcome, which is the one outcome this system may
 * never have: a file that matches is pruned, with no exception, no log and no metric. Neither is
 * reachable through an ordinary build — each needs an artifact or an error that a green suite never
 * produces — so each is forged here.
 */
class TermIndexIdentityTest {
  @TempDir Path tmp;

  private static int fieldId(Table table) {
    return table.schema().findField(LocalTableFixture.COLUMN).fieldId();
  }

  private static Path metaPath(Table table, BuildConfig config) {
    return Path.of(
        TermIndexWriter.dir(IndexPaths.root(table, config.format().indexRoot()), fieldId(table))
            + "/index-metadata.json");
  }

  /** Hadoop's local FileSystem keeps a sibling checksum that must go when the file is rewritten. */
  private static void write(Path file, byte[] bytes) throws Exception {
    Files.write(file, bytes);
    Files.deleteIfExists(file.resolveSibling("." + file.getFileName() + ".crc"));
  }

  /**
   * An aggregate written under a DIFFERENT range scheme must be refused, not read.
   *
   * <p>The leaf list is positional against {@link TermRanges#of}, and nothing in the artifact says
   * which routing function produced it. A reader whose {@code TermRanges} disagrees with the
   * writer's sends a token to the wrong slot — or past the end of the list, where {@code
   * entriesFor}'s bounds check silently drops it. A dropped token is then indistinguishable from an
   * absent one, and an absent one PRUNES. That is a false negative across every table the reader
   * serves.
   *
   * <p>This test simulates the skew the cheap way — truncating the list, as a reader routing into a
   * larger space than the writer used would experience — and asserts the safe direction: the
   * artifact is refused and the file is kept. Verified by removing the leaf-count check in {@code
   * TermIndex.load}, which makes both assertions below fail.
   */
  @Test
  void anAggregateFromADifferentRangeSchemeIsRefusedRatherThanMisread() throws Exception {
    BuildConfig config = LocalTableFixture.config();
    // 'z' routes to the LAST range, so dropping one slot sends it past the end of the list
    Table table = LocalTableFixture.createTable(tmp, "zebra quagga");
    assertNotNull(IndexBuilder.buildColumn(table, LocalTableFixture.COLUMN, config));
    assertEquals(TermRanges.COUNT - 1, TermRanges.of("zebra"), "precondition: 'z' is the last range");

    Metrics metrics = new Metrics();
    TermIndex index = new TermIndex(config.format(), metrics);
    assertNotNull(index.forField(table, fieldId(table)), "precondition: the index loads untampered");

    // rewrite the metadata with one range slot removed -- an artifact this reader cannot route into
    ObjectMapper mapper = new ObjectMapper();
    Path meta = metaPath(table, config);
    ObjectNode root = (ObjectNode) mapper.readTree(Files.readAllBytes(meta));
    JsonNode snapshot = TermIndexWriter.snapshotNode(root);
    ArrayNode leaves = (ArrayNode) snapshot.path("leaves").path("aggregate");
    assertEquals(TermRanges.COUNT, leaves.size(), "precondition: the build wrote one slot per range");
    leaves.remove(leaves.size() - 1);
    write(meta, mapper.writeValueAsBytes(root));

    // a fresh TermIndex, so nothing is served from the cache populated above
    TermIndex reader = new TermIndex(config.format(), new Metrics());
    assertNull(
        reader.forField(table, fieldId(table)),
        "an aggregate whose range count this reader cannot route into was accepted; tokens in the "
            + "missing ranges would read as absent and prune every file that holds them");

    IndexPruner pruner = new IndexPruner(reader, metrics, config.format());
    List<FileScanTask> tasks = LocalTableFixture.planTasks(table);
    List<IndexPruner.ContainsHint> hints = List.of(
        new IndexPruner.ContainsHint(
            LocalTableFixture.COLUMN, "zebra", IndexPruner.HintKind.MATCH));
    assertEquals(
        tasks.size(),
        pruner.prune(table, null, hints, tasks).size(),
        "a file containing the token was pruned by an index written under another range scheme");
  }

  /**
   * Two unreadable observations must not share a revalidation token.
   *
   * <p>The token is the key {@code entriesFor} caches resolved lookups under, and that cache records
   * token ABSENCE, which prunes. A shared sentinel makes the key collide across every table and
   * every generation that hits a transient error, so one table's "no file holds this token" is
   * served for another's — pruning every file that does. The cache has no TTL, so the poisoning
   * outlives the error that caused it.
   *
   * <p>A shared sentinel is also wrong for revalidation on its own terms: {@code refresh} renews an
   * entry whose token compares equal, so two consecutive unreadable observations would renew a stale
   * index instead of retrying.
   *
   * <p>Verified by returning a constant {@code "unreadable"} from {@code TermIndex.fingerprint},
   * which fails this test.
   */
  @Test
  void twoUnreadableObservationsDoNotShareARevalidationToken() throws Exception {
    BuildConfig config = LocalTableFixture.config();
    Table table = LocalTableFixture.createTable(tmp, "alpha beta");
    assertNotNull(IndexBuilder.buildColumn(table, LocalTableFixture.COLUMN, config));

    // present but unparseable: exists() is true, so this takes the error path rather than "absent"
    write(metaPath(table, config), "{ this is not json".getBytes(java.nio.charset.StandardCharsets.UTF_8));

    String first = TermIndex.fingerprint(table, fieldId(table), config.format());
    String second = TermIndex.fingerprint(table, fieldId(table), config.format());

    assertNotEquals("absent", first, "precondition: the metadata exists, so this is an error");
    assertTrue(first.startsWith("unreadable"), "precondition: this is the unreadable path");
    assertNotEquals(
        first,
        second,
        "two unreadable observations produced the same revalidation token; it keys the resolved-"
            + "entry cache, so one table's cached token ABSENCE would be served for another's");
  }
}
