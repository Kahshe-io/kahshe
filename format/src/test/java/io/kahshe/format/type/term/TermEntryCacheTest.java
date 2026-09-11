package io.kahshe.format.type.term;

import io.kahshe.indexer.BuildConfig;
import io.kahshe.indexer.build.IndexBuilder;
import io.kahshe.indexer.LocalTableFixture;
import io.kahshe.format.IndexPruner;
import io.kahshe.format.type.term.TermIndex;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertTrue;

import io.kahshe.common.Metrics;
import java.nio.file.Path;
import java.util.List;
import org.apache.iceberg.Table;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

/**
 * {@link TermIndex#entriesFor} caches resolved lookups, and the dangerous half of that is the
 * cached ABSENCE.
 *
 * <p>A token with no entry does not mean "unknown", it means "no file holds this term" — and
 * {@code TermIndexType.partitionByTerms} turns that into dropping every file. So a negative that outlives
 * the bytes it was read from is a false negative: rows missing from someone's answer, no exception,
 * no metric. That is the one error class this index may never have.
 *
 * <p>It is safe only because the cache key is the index's revalidation fingerprint, which folds
 * every leaf's byte length in and therefore moves whenever any leaf's content moves. These tests
 * pin that, not the caching.
 */
class TermEntryCacheTest {
  @TempDir Path tmp;

  private static List<IndexPruner.ContainsHint> match(String token) {
    return List.of(
        new IndexPruner.ContainsHint(LocalTableFixture.COLUMN, token, IndexPruner.HintKind.MATCH));
  }

  /**
   * The whole point of the cache: ask twice, read the leaf once.
   *
   * <p>Asserted through the hit counter rather than through timing, which at this corpus size would
   * be noise. Pinning the counter also means a change that quietly stops consulting the cache shows
   * up here rather than as a latency regression nobody attributes.
   */
  @Test
  void askingTheSameQuestionTwiceDoesNotReadTheLeafTwice() throws Exception {
    Table table = LocalTableFixture.createTable(tmp, "alpha bravo", "alpha charlie");
    BuildConfig config = LocalTableFixture.config();
    assertNotNull(IndexBuilder.buildColumn(table, LocalTableFixture.COLUMN, config));

    Metrics metrics = new Metrics();
    TermIndex index = new TermIndex(config.format(), metrics);
    int fieldId = table.schema().findField(LocalTableFixture.COLUMN).fieldId();
    TermIndex.Loaded loaded = index.forField(table, fieldId);
    assertNotNull(loaded);

    assertEquals(0, metrics.termEntryCacheHits.sum(), "nothing has been asked yet");
    assertEquals(1, index.entriesFor(table, loaded, List.of("alpha")).size());
    assertEquals(0, metrics.termEntryCacheHits.sum(), "the first lookup must reach the leaf");

    assertEquals(1, index.entriesFor(table, loaded, List.of("alpha")).size());
    assertTrue(metrics.termEntryCacheHits.sum() > 0, "the second lookup re-read the leaf");

    // an absent token is cached too, and still answers absent
    assertEquals(0, index.entriesFor(table, loaded, List.of("zulu")).size());
    long afterAbsent = metrics.termEntryCacheHits.sum();
    assertEquals(0, index.entriesFor(table, loaded, List.of("zulu")).size());
    assertTrue(metrics.termEntryCacheHits.sum() > afterAbsent, "absence was not cached");
  }

  /**
   * The guard. A token absent from one generation and present in the next must resolve, and a
   * cached negative must not survive the rebuild that made it wrong.
   *
   * <p>Constructed so the stale answer is a FALSE NEGATIVE rather than a harmless miss: `zulu` is
   * asked for while it exists nowhere, which caches the absence, and only then does a file
   * containing it arrive. If the cache key were anything that does not move with content — the leaf
   * path, the snapshot id, the table location — the second lookup returns the cached absence, the
   * pruner reads that as "no file holds this term", and the file that plainly does hold it is
   * dropped.
   */
  @Test
  void aCachedAbsenceDoesNotSurviveTheRebuildThatMakesItWrong() throws Exception {
    Table table = LocalTableFixture.createTable(tmp, "alpha bravo");
    BuildConfig config = LocalTableFixture.config();
    assertNotNull(IndexBuilder.buildColumn(table, LocalTableFixture.COLUMN, config));

    Metrics metrics = new Metrics();
    TermIndex index = new TermIndex(config.format(), metrics);
    int fieldId = table.schema().findField(LocalTableFixture.COLUMN).fieldId();

    TermIndex.Loaded before = index.forField(table, fieldId);
    assertNotNull(before);
    assertEquals(
        0, index.entriesFor(table, before, List.of("zulu")).size(),
        "zulu should not exist yet — the test needs the absence cached");

    // a file that contains it, and a rebuild that indexes the file
    String added = LocalTableFixture.appendFile(table, "f2.parquet", "zulu yankee");
    table.refresh();
    assertNotNull(IndexBuilder.buildColumn(table, LocalTableFixture.COLUMN, config));

    // past the 30s TTL so the metadata is re-read rather than renewed
    index.nowMs = () -> System.currentTimeMillis() + 60_000;
    TermIndex.Loaded after = index.forField(table, fieldId);
    assertNotNull(after);
    assertTrue(
        after.fingerprint() != null && !after.fingerprint().equals(before.fingerprint()),
        "the fingerprint did not move across a rebuild, so the cache key proves nothing");

    var entries = index.entriesFor(table, after, List.of("zulu"));
    assertEquals(
        1, entries.size(),
        "a cached absence survived the rebuild that made it wrong — this prunes a matching file");

    // and it names the file that actually holds it
    assertEquals(
        1, entries.get("zulu").ordinals().getCardinality(), "zulu should name exactly one file");
    assertEquals(
        added,
        after.files().get(entries.get("zulu").ordinals().first()),
        "zulu resolved to the wrong data file");
  }

  /** End to end through the pruner, which is where a stale negative actually costs rows. */
  @Test
  void thePrunerKeepsTheNewFileRatherThanServingTheCachedAbsence() throws Exception {
    Table table = LocalTableFixture.createTable(tmp, "alpha bravo");
    BuildConfig config = LocalTableFixture.config();
    assertNotNull(IndexBuilder.buildColumn(table, LocalTableFixture.COLUMN, config));

    Metrics metrics = new Metrics();
    TermIndex index = new TermIndex(config.format(), metrics);
    IndexPruner pruner = new IndexPruner(index, metrics, config.format());

    // cache the absence through the real pruning path
    assertTrue(
        pruner.prune(table, null, match("zulu"), LocalTableFixture.planTasks(table)).isEmpty(),
        "zulu is in no file yet, so everything should prune");

    String added = LocalTableFixture.appendFile(table, "f2.parquet", "zulu yankee");
    table.refresh();
    assertNotNull(IndexBuilder.buildColumn(table, LocalTableFixture.COLUMN, config));
    index.nowMs = () -> System.currentTimeMillis() + 60_000;

    List<String> kept =
        pruner.prune(table, null, match("zulu"), LocalTableFixture.planTasks(table)).stream()
            .map(t -> t.file().location())
            .toList();
    assertEquals(
        List.of(added), kept,
        "the pruner served a stale absence and dropped the file that contains the token");
  }
}
