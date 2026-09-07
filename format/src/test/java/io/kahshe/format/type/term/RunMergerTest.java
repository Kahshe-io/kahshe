package io.kahshe.format.type.term;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertThrows;
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
import org.roaringbitmap.RoaringBitmap;

/**
 * The merge is where a term's ordinals become a bitmap and where an incremental build's previous
 * generation rejoins its own index. Both are places a wrong answer is a data file wrongly pruned,
 * and neither is checkable by looking at the output alone — a merge that drops a source produces a
 * perfectly well-formed aggregate.
 *
 * <p>So the oracle is a plain {@code TreeMap} over every triple that went in, ordered by unsigned
 * UTF-8 bytes, and every test below compares the merge against it: the same terms, the same totals,
 * the same ordinal sets, in the same order.
 */
class RunMergerTest {
  @TempDir Path tmp;

  private static final Comparator<String> BYTE_ORDER =
      (a, b) ->
          java.util.Arrays.compareUnsigned(
              a.getBytes(StandardCharsets.UTF_8), b.getBytes(StandardCharsets.UTF_8));

  private record Row(String term, long total, List<Integer> ordinals) {}

  private Path dir(String name) throws IOException {
    Path root = tmp.resolve(name);
    Files.createDirectories(root);
    return root;
  }

  private RunMerger.Names names(Path root) {
    return (level, seq, intermediate) -> root.resolve("merge-" + level + "-" + seq + ".terms");
  }

  /** Writes one run from triples, in the order a RunBuffer would have sorted them. */
  private static Path writeRun(Path file, List<int[]> ordinalAndCount, List<String> terms)
      throws IOException {
    TreeMap<String, TreeMap<Integer, Long>> sorted = new TreeMap<>(BYTE_ORDER);
    for (int i = 0; i < terms.size(); i++) {
      sorted
          .computeIfAbsent(terms.get(i), k -> new TreeMap<>())
          .merge(ordinalAndCount.get(i)[0], (long) ordinalAndCount.get(i)[1], Long::sum);
    }
    try (TermRun.Writer writer = new TermRun.Writer(file)) {
      for (var term : sorted.entrySet()) {
        byte[] bytes = term.getKey().getBytes(StandardCharsets.UTF_8);
        for (var ord : term.getValue().entrySet()) {
          writer.append(bytes, 0, bytes.length, ord.getKey(), ord.getValue());
        }
      }
    }
    return file;
  }

  private static List<Row> collect(TreeMap<String, TreeMap<Integer, Long>> oracle) {
    List<Row> rows = new ArrayList<>();
    for (Map.Entry<String, TreeMap<Integer, Long>> e : oracle.entrySet()) {
      long total = e.getValue().values().stream().mapToLong(Long::longValue).sum();
      rows.add(new Row(e.getKey(), total, List.copyOf(e.getValue().keySet())));
    }
    return rows;
  }

  private static RunMerger.RowSink sink(List<Row> out) {
    return (term, total, ordinals) -> {
      List<Integer> ords = new ArrayList<>();
      ordinals.forEach((org.roaringbitmap.IntConsumer) ords::add);
      out.add(new Row(term, total, ords));
    };
  }

  @Test
  void mergedRunsEqualAMapOverEverythingThatWentIn() throws Exception {
    Path root = dir("many");
    Random random = new Random(918273L);
    TreeMap<String, TreeMap<Integer, Long>> oracle = new TreeMap<>(BYTE_ORDER);
    List<Path> runs = new ArrayList<>();

    for (int r = 0; r < 9; r++) {
      List<String> terms = new ArrayList<>();
      List<int[]> pairs = new ArrayList<>();
      for (int i = 0; i < 400; i++) {
        String term =
            random.nextInt(3) == 0
                ? "shared" + random.nextInt(25)
                : Long.toHexString(random.nextLong() >>> 8);
        int ordinal = random.nextInt(11);
        int count = 1 + random.nextInt(4);
        terms.add(term);
        pairs.add(new int[] {ordinal, count});
        oracle.computeIfAbsent(term, k -> new TreeMap<>()).merge(ordinal, (long) count, Long::sum);
      }
      runs.add(writeRun(root.resolve("r" + r + ".terms"), pairs, terms));
    }

    List<Row> got = new ArrayList<>();
    long terms = RunMerger.merge(runs, List.of(), names(root), sink(got));

    List<Row> want = collect(oracle);
    assertEquals(want.size(), terms, "the merge reported a different term count than it emitted");
    assertEquals(want, got, "merged rows differ from a map over every input triple");
  }

  /**
   * A cascade must give the same answer as one level. Seven thousand runs is the real case; a
   * fan-in of three over nine runs is the same code path, two levels deep, without opening seven
   * thousand files to prove it.
   */
  @Test
  void aCascadingMergeAgreesWithASingleLevelMerge() throws Exception {
    Path flat = dir("flat");
    Path cascade = dir("cascade");
    Random random = new Random(5150L);

    List<List<String>> allTerms = new ArrayList<>();
    List<List<int[]>> allPairs = new ArrayList<>();
    for (int r = 0; r < 9; r++) {
      List<String> terms = new ArrayList<>();
      List<int[]> pairs = new ArrayList<>();
      for (int i = 0; i < 250; i++) {
        terms.add(random.nextInt(2) == 0 ? "w" + random.nextInt(30) : "id" + random.nextInt(4000));
        pairs.add(new int[] {random.nextInt(7), 1 + random.nextInt(3)});
      }
      allTerms.add(terms);
      allPairs.add(pairs);
    }

    List<Path> flatRuns = new ArrayList<>();
    List<Path> cascadeRuns = new ArrayList<>();
    for (int r = 0; r < 9; r++) {
      flatRuns.add(writeRun(flat.resolve("r" + r + ".terms"), allPairs.get(r), allTerms.get(r)));
      cascadeRuns.add(
          writeRun(cascade.resolve("r" + r + ".terms"), allPairs.get(r), allTerms.get(r)));
    }

    List<Row> single = new ArrayList<>();
    RunMerger.merge(flatRuns, List.of(), names(flat), 64, sink(single));

    List<Row> cascaded = new ArrayList<>();
    RunMerger.merge(cascadeRuns, List.of(), names(cascade), 3, sink(cascaded));

    assertTrue(
        Files.list(cascade).anyMatch(p -> p.getFileName().toString().startsWith("merge-")),
        "fan-in of 3 over 9 runs did not actually cascade");
    assertEquals(single, cascaded, "a cascading merge disagreed with a single-level merge");
  }

  /**
   * An incremental build's prior generation arrives as bitmap-bearing rows while this build's terms
   * arrive one ordinal at a time. Both are sources, and the counts and ordinals have to combine
   * across the two shapes.
   *
   * <p>Getting this wrong is the quiet one: the published index still covers every data file, so
   * the metadata asserts the old files are indexed, while a term only the old files contain has
   * gone missing — every one of them pruned for that term.
   */
  @Test
  void priorAggregateRowsMergeWithThisBuildsRuns() throws Exception {
    Path root = dir("prior");
    // this build read files 3 and 4
    Path run =
        writeRun(
            root.resolve("new.terms"),
            List.of(new int[] {3, 5}, new int[] {4, 2}, new int[] {3, 1}),
            List.of("alpha", "alpha", "gamma"));

    // the prior generation covered files 0..2
    RunMerger.Source prior =
        source(
            List.<Object[]>of(
                new Object[] {"alpha", 10L, new int[] {0, 1}},
                new Object[] {"beta", 4L, new int[] {2}}));

    List<Row> got = new ArrayList<>();
    RunMerger.merge(List.of(run), List.of(prior), names(root), sink(got));

    assertEquals(
        List.of(
            new Row("alpha", 17L, List.of(0, 1, 3, 4)),
            new Row("beta", 4L, List.of(2)),
            new Row("gamma", 1L, List.of(3))),
        got,
        "prior rows and run rows did not combine");
  }

  /**
   * file_count is the union's cardinality and never a sum. The distinction only shows when one data
   * file reaches the merge through more than one source, which is the normal case the moment a
   * reader's arena fills mid-file — and a summed file_count is then larger than the number of files
   * that exist.
   */
  @Test
  void fileCountIsTheUnionCardinalityNotASumOverSources() throws Exception {
    Path root = dir("cardinality");
    // the same (term, ordinal) in three separate runs: one file, split across three arena flushes
    List<Path> runs =
        List.of(
            writeRun(root.resolve("a.terms"), List.of(new int[] {6, 2}), List.of("split")),
            writeRun(root.resolve("b.terms"), List.of(new int[] {6, 3}), List.of("split")),
            writeRun(root.resolve("c.terms"), List.of(new int[] {6, 4}), List.of("split")));

    List<Row> got = new ArrayList<>();
    RunMerger.merge(runs, List.of(), names(root), sink(got));

    assertEquals(1, got.size());
    assertEquals(List.of(6), got.get(0).ordinals(), "one file was counted as three");
    assertEquals(9L, got.get(0).total(), "the occurrences across the three flushes were not summed");
  }

  /**
   * The merge trusts nothing. Order comes from a hand-written byte comparator over a packed arena
   * rather than from a sorted map, so a source that goes backwards has to be a loud failure rather
   * than a duplicate term row that a reader resolves last-wins.
   */
  @Test
  void aNonAscendingSourceFailsTheMergeRatherThanEmittingADuplicate() throws Exception {
    Path root = dir("unsorted");
    RunMerger.Source backwards =
        source(
            List.<Object[]>of(
                new Object[] {"zulu", 1L, new int[] {0}},
                new Object[] {"alpha", 1L, new int[] {1}}));

    IOException failure =
        assertThrows(
            IOException.class,
            () -> RunMerger.merge(List.of(), List.of(backwards), names(root), sink(new ArrayList<>())));
    assertTrue(
        failure.getMessage().contains("non-ascending"),
        "the merge accepted a descending source: " + failure.getMessage());
  }

  /** A term whose only home is the prior generation must survive a build that read nothing. */
  @Test
  void anIncrementalBuildThatReadNothingStillRepublishesThePrior() throws Exception {
    Path root = dir("empty");
    RunMerger.Source prior =
        source(List.<Object[]>of(new Object[] {"kept", 7L, new int[] {0, 1, 2}}));

    List<Row> got = new ArrayList<>();
    RunMerger.merge(List.of(), List.of(prior), names(root), sink(got));
    assertEquals(List.of(new Row("kept", 7L, List.of(0, 1, 2))), got,
        "an empty run list dropped the prior generation");
  }

  /**
   * A cascading merge must charge its intermediate runs against the local-disk budget and release
   * the runs it consumes.
   *
   * <p>An intermediate is real bytes on the same volume as everything else, and it is easy for it
   * to be charged for nothing at all: the enforcement point is called from the arena, while the
   * cascade names its runs through a different method. A build that skips the charge writes a
   * second copy of every run it holds — on a corpus large enough to cascade, roughly doubling peak
   * scratch — while the budget that exists to bound scratch sees none of it. Deleting each group's
   * inputs as they are consumed is what makes the second copy transient rather than additive.
   */
  @Test
  void aCascadeChargesItsIntermediatesAndReleasesWhatItConsumes() throws Exception {
    Path root = dir("accounting");
    List<Path> runs = new ArrayList<>();
    Random random = new Random(31337L);
    for (int r = 0; r < 9; r++) {
      List<String> terms = new ArrayList<>();
      List<int[]> pairs = new ArrayList<>();
      for (int i = 0; i < 200; i++) {
        terms.add("t" + random.nextInt(900));
        pairs.add(new int[] {random.nextInt(5), 1});
      }
      runs.add(writeRun(root.resolve("r" + r + ".terms"), pairs, terms));
    }
    long inputBytes = 0;
    for (Path run : runs) {
      inputBytes += Files.size(run);
    }

    List<Path> charged = new ArrayList<>();
    List<Path> releasedPaths = new ArrayList<>();
    RunMerger.Names accounting =
        new RunMerger.Names() {
          @Override
          public Path next(int level, int seq, boolean intermediate) {
            return root.resolve("merge-" + level + "-" + seq + ".terms");
          }

          @Override
          public void finished(Path run) {
            charged.add(run);
          }

          @Override
          public void released(Path run) {
            releasedPaths.add(run);
          }
        };

    List<Row> got = new ArrayList<>();
    RunMerger.merge(runs, List.of(), accounting, 3, sink(got));

    assertTrue(!charged.isEmpty(), "the cascade charged nothing for its intermediate runs");
    assertEquals(
        new java.util.HashSet<>(runs), new java.util.HashSet<>(releasedPaths.subList(0, runs.size())),
        "the cascade did not release the level-0 runs it consumed");
    for (Path run : runs) {
      assertTrue(
          !Files.exists(run),
          "a consumed run was left on disk: " + run.getFileName()
              + " — peak scratch is then inputs plus outputs, not one or the other");
    }
    assertTrue(inputBytes > 0, "the fixture wrote nothing");
    assertTrue(got.size() > 100, "the merge produced too little to be a real cascade");
  }

  /**
   * The prior generation's cursors are open before the merge is called, and the cascade can throw.
   * They must be closed anyway.
   *
   * <p>They are Parquet readers over the previous index's range leaves — up to
   * {@link TermRanges#COUNT} of them per build, each holding a file handle and a decompression
   * buffer. Leaking one set per failed build is how a retrying indexer runs a node out of
   * descriptors while every individual failure looks like an ordinary build error.
   */
  @Test
  void aCascadeThatThrowsStillClosesThePriorSources() throws Exception {
    Path root = dir("leak");
    List<Path> runs = new ArrayList<>();
    for (int r = 0; r < 9; r++) {
      runs.add(
          writeRun(
              root.resolve("r" + r + ".terms"),
              List.of(new int[] {r, 1}),
              List.of("term" + r)));
    }
    boolean[] closed = {false, false};
    List<RunMerger.Source> prior =
        List.of(tracked(closed, 0, "alpha"), tracked(closed, 1, "beta"));

    RunMerger.Names exploding =
        (level, seq, intermediate) -> {
          throw new IOException("scratch volume full");
        };

    IOException failure =
        assertThrows(
            IOException.class,
            () -> RunMerger.merge(runs, prior, exploding, 3, sink(new ArrayList<>())));
    assertTrue(failure.getMessage().contains("scratch volume full"), failure.getMessage());
    assertTrue(closed[0] && closed[1],
        "a cascade that threw leaked the prior generation's open leaf readers");
  }

  private static RunMerger.Source tracked(boolean[] closed, int at, String term) {
    return new RunMerger.Source() {
      private boolean done;

      @Override
      public byte[] term() {
        return done ? null : term.getBytes(StandardCharsets.UTF_8);
      }

      @Override
      public long count() {
        return 1L;
      }

      @Override
      public void ordinalsInto(RoaringBitmap target) {
        target.add(0);
      }

      @Override
      public void next() {
        done = true;
      }

      @Override
      public void close() {
        closed[at] = true;
      }
    };
  }

  /** A bitmap-bearing source, as an incremental build's aggregate leaf will be. */
  private static RunMerger.Source source(List<Object[]> rows) {
    return new RunMerger.Source() {
      private int at = 0;

      @Override
      public byte[] term() {
        return at >= rows.size()
            ? null
            : ((String) rows.get(at)[0]).getBytes(StandardCharsets.UTF_8);
      }

      @Override
      public long count() {
        return (Long) rows.get(at)[1];
      }

      @Override
      public void ordinalsInto(RoaringBitmap target) {
        for (int o : (int[]) rows.get(at)[2]) {
          target.add(o);
        }
      }

      @Override
      public void next() {
        at++;
      }

      @Override
      public void close() {}
    };
  }
}
