package io.kahshe.format.type.term;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNotEquals;
import static org.junit.jupiter.api.Assertions.assertTrue;

import java.io.IOException;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.ArrayList;
import java.util.Comparator;
import java.util.List;
import java.util.Map;
import java.util.Random;
import java.util.TreeMap;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;
import io.kahshe.analysis.analyzer.Analyzer;

/**
 * The arena replaces a map whose size had to be guessed, so what it produces has to be checked
 * against the map it replaces — not against itself.
 *
 * <p>The oracle is a {@code TreeMap<String, TreeMap<Integer, Long>>} ordered by unsigned UTF-8
 * bytes: what the terms would be if nothing were ever spilled, coalesced or sorted in a packed byte
 * array. The buffer's runs, merged, must equal it exactly — the same {@code (term, ordinal, count)}
 * triples in the same order. Self-consistency would prove nothing here, because the danger is drift
 * between two ways of ordering and counting the same terms, and both would drift together.
 *
 * <p>Order is asserted, not just membership. A comparator that produced the right SET of rows in
 * the wrong ORDER would satisfy every set-equality check and then feed the k-way merge a stream it
 * believes is ascending, which drops rows — a data file wrongly pruned. That is the failure this
 * class exists to catch, and it is why the assertions below compare lists rather than maps.
 */
class RunBufferTest {
  @TempDir Path tmp;

  /**
   * The order the whole term path uses: unsigned UTF-8 bytes, matching Parquet's string statistics
   * and Iceberg's own comparator, NOT {@code String.compareTo}.
   *
   * <p>Deliberately {@code java.util.Arrays.compareUnsigned} and not {@link TermRun#compare}:
   * ordering the oracle by the comparator under test makes the oracle a mirror of it. Change that
   * comparator to signed bytes — a real defect, and one that puts every byte at or above 0x80
   * before every ASCII byte — and the whole class stays green, because the expected order moves
   * with it.
   */
  private static final Comparator<String> BYTE_ORDER =
      (a, b) ->
          java.util.Arrays.compareUnsigned(
              a.getBytes(StandardCharsets.UTF_8), b.getBytes(StandardCharsets.UTF_8));

  /** The comparator under test must agree with the JDK's, over the places a hand-rolled unsigned
   * comparison plausibly goes wrong. */
  @Test
  void theComparatorAgreesWithTheJdkOverBytesAboveAscii() {
    List<String> corpus =
        List.of("", "a", "b", "ab", "abc", "z", "zz", "0", "9", "é", "ÿ", "€", "中", "𐀀", "", "￿",
            "\u0080", "\u007f", "aé", "aa", "a\u0080");
    for (String a : corpus) {
      for (String b : corpus) {
        byte[] x = a.getBytes(StandardCharsets.UTF_8);
        byte[] y = b.getBytes(StandardCharsets.UTF_8);
        assertEquals(
            Integer.signum(java.util.Arrays.compareUnsigned(x, y)),
            Integer.signum(TermRun.compare(x, y)),
            "TermRun.compare disagrees with unsigned byte order on [" + a + "] vs [" + b + "]");
      }
    }
    Random random = new Random(7L);
    for (int i = 0; i < 5_000; i++) {
      byte[] x = new byte[1 + random.nextInt(6)];
      byte[] y = new byte[1 + random.nextInt(6)];
      random.nextBytes(x);
      random.nextBytes(y);
      assertEquals(
          Integer.signum(java.util.Arrays.compareUnsigned(x, y)),
          Integer.signum(TermRun.compare(x, y)),
          "TermRun.compare disagrees with unsigned byte order on random bytes");
    }
  }

  private RunBuffer.Names names(String dir) throws IOException {
    Path root = tmp.resolve(dir);
    Files.createDirectories(root);
    return (slot, seq) -> root.resolve("run-" + slot + "-" + seq + ".terms");
  }

  private record Triple(String term, int ordinal, long count) {}

  /** Every row of every run, in the order the runs were written. */
  private static List<Triple> readAll(List<Path> runs) throws IOException {
    List<Triple> rows = new ArrayList<>();
    for (Path run : runs) {
      try (TermRun.Cursor cursor = new TermRun.Cursor(run)) {
        for (byte[] term = cursor.term(); term != null; cursor.next(), term = cursor.term()) {
          rows.add(
              new Triple(
                  new String(term, java.nio.charset.StandardCharsets.UTF_8),
                  cursor.ordinal(),
                  cursor.count()));
        }
      }
    }
    return rows;
  }

  /** The runs merged the way the final merge will: sum by (term, ordinal), ascending. */
  private static List<Triple> merged(List<Path> runs) throws IOException {
    TreeMap<String, TreeMap<Integer, Long>> byTerm = new TreeMap<>(BYTE_ORDER);
    for (Triple t : readAll(runs)) {
      byTerm.computeIfAbsent(t.term(), k -> new TreeMap<>()).merge(t.ordinal(), t.count(), Long::sum);
    }
    return flatten(byTerm);
  }

  private static List<Triple> flatten(TreeMap<String, TreeMap<Integer, Long>> byTerm) {
    List<Triple> rows = new ArrayList<>();
    for (Map.Entry<String, TreeMap<Integer, Long>> term : byTerm.entrySet()) {
      for (Map.Entry<Integer, Long> ord : term.getValue().entrySet()) {
        rows.add(new Triple(term.getKey(), ord.getKey(), ord.getValue()));
      }
    }
    return rows;
  }

  @Test
  void whatTheRunsHoldIsWhatAnUnspilledMapWouldHold() throws Exception {
    Random random = new Random(20260829L);
    // a small arena against a large corpus, so the flush boundary falls repeatedly and in
    // different places than any single term or file boundary
    RunBuffer buffer = new RunBuffer(4096, names("oracle"), 0);
    TreeMap<String, TreeMap<Integer, Long>> oracle = new TreeMap<>(BYTE_ORDER);

    int ordinal = 0;
    for (int i = 0; i < 20_000; i++) {
      if (random.nextInt(500) == 0) {
        ordinal++; // a new data file
      }
      // a vocabulary with both shapes that matter: recurring words and near-unique identifiers
      String term =
          random.nextInt(3) == 0
              ? "w" + random.nextInt(40)
              : Long.toHexString(random.nextLong() >>> 1);
      long count = 1 + random.nextInt(5);
      buffer.append(term, count, ordinal);
      oracle.computeIfAbsent(term, k -> new TreeMap<>()).merge(ordinal, count, Long::sum);
    }
    buffer.close();

    assertTrue(buffer.flushes() > 5, "arena never filled, so no flush boundary was exercised");
    assertTrue(ordinal > 2, "corpus spanned too few ordinals to test attribution");
    assertEquals(flatten(oracle), merged(buffer.runs()),
        "the runs do not hold what an unspilled map would hold");
  }

  @Test
  void everyRunIsStrictlyAscendingAndCoalesced() throws Exception {
    Random random = new Random(4242L);
    RunBuffer buffer = new RunBuffer(2048, names("ascending"), 0);
    for (int i = 0; i < 8_000; i++) {
      // a deliberately tiny vocabulary, so the same (term, ordinal) recurs constantly and
      // coalescing has real work to do
      buffer.append("t" + random.nextInt(12), 1, random.nextInt(3));
    }
    buffer.close();

    assertTrue(buffer.flushes() > 3, "arena never filled");
    for (Path run : buffer.runs()) {
      List<Triple> rows = readAll(List.of(run));
      for (int i = 1; i < rows.size(); i++) {
        Triple prev = rows.get(i - 1);
        Triple next = rows.get(i);
        int cmp = BYTE_ORDER.compare(prev.term(), next.term());
        assertTrue(
            cmp < 0 || (cmp == 0 && prev.ordinal() < next.ordinal()),
            "run " + run.getFileName() + " is not strictly ascending at row " + i
                + ": [" + prev.term() + "/" + prev.ordinal() + "] then ["
                + next.term() + "/" + next.ordinal() + "]");
      }
    }
  }

  /**
   * The coalescing walk is the whole reason the flush is cheap, and removing it produces a run that
   * is still perfectly sorted and still has the right total — just spread over more rows. Nothing
   * about the run's shape catches that. Counting the rows does.
   */
  @Test
  void coalescingCollapsesRepeatsRatherThanEmittingThemAll() throws Exception {
    RunBuffer buffer = new RunBuffer(1 << 16, names("coalesce"), 0);
    for (int i = 0; i < 1_000; i++) {
      buffer.append("repeated", 1, 7);
    }
    buffer.close();

    List<Triple> rows = readAll(buffer.runs());
    assertEquals(1, rows.size(), "1000 identical (term, ordinal) records did not collapse to one");
    assertEquals(1_000L, rows.get(0).count(), "the collapsed row lost occurrences");
    assertEquals(1_000L, rows.stream().mapToLong(Triple::count).sum(), "total occurrences changed");
  }

  /**
   * The real corpus shape: a handful of terms in every row, plus one unique identifier per row.
   *
   * <p>This is what a log-like column looks like — a dozen or more tokens appearing in <b>100% of
   * rows</b> against a per-row unique hex trace — and it is the shape that makes the arena sort the
   * build's bottleneck. A 64 MiB arena of it holds roughly fifty distinct common terms repeated
   * hundreds of thousands of times each alongside a long singleton tail, which is the worst case
   * for a two-way partition and the best case for a three-way one.
   *
   * <p>The sort is tuned for speed; this test exists so that tuning cannot quietly cost
   * correctness. A three-way partition moves equal keys into a middle region and never revisits
   * them, so an off-by-one in the region boundaries loses or duplicates exactly the records that
   * are hardest to notice — the repeated ones, whose counts still look plausible.
   */
  @Test
  void aCorpusOfRepeatedTermsAndUniqueIdentifiersMatchesTheOracle() throws Exception {
    String[] common = {
      "connection", "established", "completed", "validation", "payload", "planner",
      "status", "ok", "phase", "steady", "peer", "latency", "retries", "svc"
    };
    Random random = new Random(50_000L);
    RunBuffer buffer = new RunBuffer(1 << 16, names("logshape"), 0);
    TreeMap<String, TreeMap<Integer, Long>> oracle = new TreeMap<>(BYTE_ORDER);

    int ordinal = 0;
    for (int row = 0; row < 6_000; row++) {
      if (row % 1500 == 0 && row > 0) {
        ordinal++;
      }
      for (String c : common) {
        buffer.append(c, 1, ordinal);
        oracle.computeIfAbsent(c, k -> new TreeMap<>()).merge(ordinal, 1L, Long::sum);
      }
      String trace = Long.toHexString(random.nextLong() >>> 1);
      buffer.append(trace, 1, ordinal);
      oracle.computeIfAbsent(trace, k -> new TreeMap<>()).merge(ordinal, 1L, Long::sum);
    }
    buffer.close();

    assertTrue(buffer.flushes() > 3, "the arena never filled, so the sort was barely exercised");
    assertEquals(flatten(oracle), merged(buffer.runs()),
        "a corpus of repeated terms plus unique identifiers did not survive the sort");

    // and the counts of the repeated terms specifically, which is what a mishandled equal-region
    // boundary would corrupt while leaving the row shape looking entirely reasonable
    for (String c : common) {
      long total =
          merged(buffer.runs()).stream()
              .filter(t -> t.term().equals(c))
              .mapToLong(Triple::count)
              .sum();
      assertEquals(6_000L, total, "term [" + c + "] appears in every row but totalled " + total);
    }
  }

  /**
   * The arena is allocated, not grown, and nothing about the terms changes that. This is the
   * design's central claim, and it has to be asserted against allocated bytes: an estimate
   * compared against the budget that same estimator feeds cannot show a divergence.
   */
  @Test
  void capacityDoesNotDependOnTheVocabulary() throws Exception {
    RunBuffer distinct = new RunBuffer(8192, names("distinct"), 0);
    RunBuffer repeated = new RunBuffer(8192, names("repeated"), 0);
    for (int i = 0; i < 30_000; i++) {
      distinct.append("u" + i, 1, 0); // every term unique: the shape that exhausts a growing map
      repeated.append("same", 1, 0); // one term forever
    }
    distinct.close();
    repeated.close();

    assertEquals(distinct.capacityBytes(), repeated.capacityBytes(),
        "capacity moved with the vocabulary");
    assertEquals(8192 + 4L * (8192 / RunBuffer.BYTES_PER_RECORD), distinct.capacityBytes(),
        "capacity is not arena + offset index");
    // and the offset index is a real, sizeable share of it -- peak heap is
    // readers x (arena + offsets + sort scratch), and dropping the middle term understates it
    assertTrue(
        distinct.capacityBytes() > 8192,
        "the offset index contributes nothing to capacity, so the formula is wrong");
    // and the two corpora really were different shapes
    assertEquals(30_000, readAll(distinct.runs()).size(), "distinct terms did not survive");
    assertTrue(readAll(repeated.runs()).size() < 100, "identical terms did not coalesce");
  }

  /**
   * A token longer than the whole arena cannot be flushed-and-retried — that makes no progress —
   * and must not be dropped, because {@link Analyzer} caps no token length and a long alphanumeric
   * token is indexable. Dropping it is a term the index never recorded, which is a data file
   * wrongly pruned.
   */
  @Test
  void aTermLargerThanTheArenaIsStillIndexed() throws Exception {
    RunBuffer buffer = new RunBuffer(64, names("oversized"), 0);
    String huge = "z".repeat(500);
    buffer.append("small", 1, 0);
    buffer.append(huge, 3, 1);
    buffer.append("other", 1, 2);
    buffer.close();

    List<Triple> all = readAll(buffer.runs());
    assertTrue(all.stream().anyMatch(t -> t.term().equals(huge) && t.count() == 3),
        "a term larger than the arena was lost");
    assertTrue(all.stream().anyMatch(t -> t.term().equals("small")), "small lost");
    assertTrue(all.stream().anyMatch(t -> t.term().equals("other")), "other lost");
  }

  /**
   * Term lengths that cross a varint boundary. The length prefix grows from one byte to two at 128
   * and from two to three at 16384, and the offset stored for a record points at that prefix — so a
   * comparator that started at the offset rather than past the prefix would order by length here,
   * and a flush that mis-stepped the prefix width would read the term from the wrong byte.
   */
  @Test
  void termLengthsThatCrossVarintBoundariesRoundTrip() throws Exception {
    RunBuffer buffer = new RunBuffer(1 << 20, names("varint"), 0);
    TreeMap<String, TreeMap<Integer, Long>> oracle = new TreeMap<>(BYTE_ORDER);
    int[] lengths = {1, 2, 126, 127, 128, 129, 130, 16382, 16383, 16384, 16385};
    for (int i = 0; i < lengths.length; i++) {
      String term = "a".repeat(lengths[i] - 1) + (char) ('b' + (i % 20));
      assertEquals(lengths[i], term.length(), "test built the wrong length");
      buffer.append(term, i + 1L, i);
      oracle.computeIfAbsent(term, k -> new TreeMap<>()).merge(i, i + 1L, Long::sum);
    }
    buffer.close();
    assertEquals(flatten(oracle), merged(buffer.runs()),
        "a term whose length crosses a varint boundary did not round-trip");
  }

  /**
   * Ordering is unsigned UTF-8 bytes, which is what Parquet's string statistics and Iceberg's own
   * comparator use — and is NOT {@code String.compareTo}. The two disagree on supplementary-plane
   * characters, whose UTF-16 surrogates sort below U+E000..U+FFFF while their codepoints sort
   * above. A signed byte comparison is wrong far more cheaply than that: it puts every byte at or
   * above 0x80 before every ASCII byte.
   */
  @Test
  void orderIsUnsignedByteOrderNotUtf16Order() throws Exception {
    String astral = "𐀀"; // U+10000
    String bmpHigh = ""; // U+E000, private use
    assertTrue(astral.compareTo(bmpHigh) < 0, "premise: UTF-16 order puts the astral term first");
    assertTrue(BYTE_ORDER.compare(astral, bmpHigh) > 0, "byte order must put it second");

    RunBuffer buffer = new RunBuffer(4096, names("order"), 0);
    for (String term : List.of(astral, bmpHigh, "zebra", "apple", "éclair")) {
      buffer.append(term, 1, 0);
    }
    buffer.close();

    List<Triple> rows = readAll(buffer.runs());
    List<String> got = rows.stream().map(Triple::term).toList();
    List<String> want = new ArrayList<>(got);
    want.sort(BYTE_ORDER);
    assertEquals(want, got, "the run is not in unsigned byte order");
    assertNotEquals(
        got.stream().sorted().toList(), got,
        "byte order and String.compareTo agreed, so this corpus cannot tell them apart");
  }

  /** A collision between two readers' run names must fail the build, not truncate a finished run
   * in place -- the default open mode would silently do the latter. */
  @Test
  void aRunNameCollisionFailsRatherThanOverwriting() throws Exception {
    Path root = tmp.resolve("collide");
    Files.createDirectories(root);
    RunBuffer.Names fixed = (slot, seq) -> root.resolve("same.terms");

    RunBuffer first = new RunBuffer(256, fixed, 0);
    first.append("alpha", 1, 0);
    first.close();

    RunBuffer second = new RunBuffer(256, fixed, 1);
    second.append("bravo", 1, 0);
    IOException failure = org.junit.jupiter.api.Assertions.assertThrows(IOException.class, second::close);
    assertTrue(
        failure instanceof java.nio.file.FileAlreadyExistsException,
        "a colliding run name must fail, not overwrite: " + failure);
    assertEquals(
        List.of(new Triple("alpha", 0, 1L)), readAll(List.of(root.resolve("same.terms"))),
        "the first reader's finished run was destroyed by the collision");
  }
}
