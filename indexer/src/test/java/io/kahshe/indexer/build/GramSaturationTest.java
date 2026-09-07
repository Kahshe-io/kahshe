package io.kahshe.indexer.build;

import io.kahshe.format.type.gram.Grams;
import io.kahshe.format.type.bloom.NgramBloom;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertTrue;

import io.kahshe.common.Metrics;
import java.nio.file.Path;
import java.util.Set;
import java.util.stream.IntStream;
import org.apache.iceberg.Table;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;
import io.kahshe.indexer.LocalTableFixture;

/**
 * The density collision made visible at build time: a file whose gram set saturates its
 * own alphabet's trigram space is counted on {@code kahshe_gram_saturated_files_total} and named
 * in a WARN, because on the query side the symptom — every probe keeps the file — is
 * indistinguishable from data that simply is not selective.
 *
 * <p>The signal is the gram set against its measured alphabet, deliberately not the bloom's fill
 * fraction: {@code NgramBloom.build} sizes the filter from the gram count, so a well-formed bloom
 * sits near half-full whatever the column holds.
 */
class GramSaturationTest {
  @TempDir Path tmp;
  @TempDir Path tmp2;

  private static String[] denseIds() {
    // ~300 twelve-digit ids stride the numeric trigram space: ~3,000 gram draws over the 1,000
    // possible all-digit trigrams leaves well over half of them present, past the size floor.
    return IntStream.range(0, 300)
        .mapToObj(i -> String.valueOf(100_000_000_000L + i * 7_919_463_207L % 899_999_999_999L))
        .toArray(String[]::new);
  }

  @Test
  void theSignalIsTheGramSetAgainstItsOwnAlphabetNotTheBloom() {
    Set<String> dense = NgramBloom.gramsOf(String.join(" ", denseIds()), Grams.Contract.current(3));
    // sanity: the fixture really does saturate — digits plus the space character
    assertTrue(dense.size() >= IndexBuilder.GRAM_SATURATION_MIN, "fixture must clear the floor");
    assertTrue(IndexBuilder.gramSpaceSaturated(dense, Grams.Contract.current(3)), "a dense numeric id column saturates");

    Set<String> prose =
        NgramBloom.gramsOf("the quick brown fox jumps over the lazy dog near the riverbank", Grams.Contract.current(3));
    assertFalse(
        IndexBuilder.gramSpaceSaturated(prose, Grams.Contract.current(3)),
        "prose uses a sparse corner of its alphabet's cube and is below the floor besides");

    // Below the floor nothing is flagged, however tiny the alphabet: a small gram set is
    // selective whatever characters it uses, and a file that small costs nothing to scan.
    Set<String> tinyDenseAlphabet = NgramBloom.gramsOf("aaabbbaaabbb", Grams.Contract.current(3));
    assertFalse(IndexBuilder.gramSpaceSaturated(tinyDenseAlphabet, Grams.Contract.current(3)));
  }

  @Test
  void aSaturatedFileMovesTheCounterAndAProseFileDoesNot() throws Exception {
    Metrics metrics = new Metrics();
    Table dense = LocalTableFixture.createTable(tmp, denseIds());
    IndexBuilder.buildColumn(
        dense, LocalTableFixture.COLUMN, LocalTableFixture.config(),
        IndexBuildListener.NONE, "", "", "", metrics);
    assertEquals(
        1, metrics.gramSaturatedFiles.sum(),
        "the dense file is counted — a degradation nobody can alert on is one nobody finds");

    Table prose = LocalTableFixture.createTable(tmp2, "alpha bravo charlie delta echo foxtrot");
    IndexBuilder.buildColumn(
        prose, LocalTableFixture.COLUMN, LocalTableFixture.config(),
        IndexBuildListener.NONE, "", "", "", metrics);
    assertEquals(
        1, metrics.gramSaturatedFiles.sum(),
        "the prose control moves nothing: the counter must not fire on ordinary text");
  }
}
