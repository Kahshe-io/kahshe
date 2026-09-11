package io.kahshe.format.type;

import java.io.IOException;
import java.nio.file.Path;
import java.util.List;
import java.util.Map;
import org.apache.iceberg.Table;
import org.apache.iceberg.io.FileIO;
import org.roaringbitmap.RoaringBitmap;
import io.kahshe.format.Coverage;
import io.kahshe.format.FormatConfig;
import io.kahshe.format.IndexPruner;
import io.kahshe.analysis.analyzer.Analyzer;
import io.kahshe.format.type.bloom.IndexMeta;
import io.kahshe.format.type.bloom.IndexStore;
import io.kahshe.format.type.bloom.NgramBloom;
import io.kahshe.format.type.gram.GramIndex;
import io.kahshe.format.type.gram.GramIndexWriter;
import io.kahshe.format.type.gram.Grams;
import io.kahshe.format.type.term.TermIndex;
import io.kahshe.format.type.term.TermRunStore;

/**
 * One index type, behind the five seams a build and a plan touch it through: its key, its
 * collection during the read pass, its write at publish, its reader at serve, and the question it
 * answers about files. Implementations are discovered with {@link java.util.ServiceLoader}; see
 * {@link IndexTypes}.
 *
 * <p>Absence is the safe answer everywhere: {@link #load} returns null for an index that is
 * missing, refused or unreadable, and a type that proves nothing answers {@code unknown}, which
 * every caller must treat as "cannot rule it out". A type may therefore fail without making a plan
 * wrong, only slower.
 *
 * <p>It states facts about FILES and nothing about who is asking. No concept belonging to a
 * consumer — a scan task, a rule, an alert, a delete file — may appear in this interface, or the
 * next consumer inherits the last one's vocabulary. That is why {@link #partition} answers in plan
 * ordinals rather than in the {@code FileScanTask}s it used to take and return.
 *
 * <p>What it does not abstract: the built-in tiers share one {@link PublishContext} rather than each
 * carrying its own, and the read pass reaches a file's content only through {@link Collector}.
 */
public interface IndexType {

  /** The leaves key and directory identity this type owns; unique across the registry. */
  String key();

  /** This type's collector for one column build, or {@link Collector#NONE} to collect nothing. */
  Collector collector(BuildContext ctx);

  /** Writes this type's immutable nonce-named leaf/leaves and reports what it published. */
  Leaves write(PublishContext ctx) throws IOException;

  /** This type's reader for one table, or null when it is absent, refused or unreadable. */
  Loaded load(ReadContext ctx);

  /**
   * Sorts the files still in play into what this type PROVED holds the probe, what it PROVED does
   * not, and what it cannot speak for. A type that proves nothing answers all-unknown.
   *
   * <p>The three verdicts are returned separately because two of them are not recoverable from a
   * list of survivors. {@code absent} is — it is the input minus the output — but a file this type
   * matched and a file it has never heard of both survive, and a caller that must tell those apart
   * cannot. Pruning is the caller for which they coincide, not the shape of the answer.
   */
  Partition partition(Loaded loaded, Probe probe, FileSet files);

  /**
   * One column build's per-file hooks.
   *
   * <p>Called on the thread reading that data file. One file's {@code file}, {@code row} and
   * {@code fileDone} calls are ordered and contiguous, but several files are read concurrently, so
   * a collector that keeps state must be safe for concurrent files.
   */
  interface Collector {
    /** A collector that wants nothing; the build drops it rather than calling it per row. */
    Collector NONE = new Collector() {};

    /** One data file is about to be read, under the ordinal its bitmaps will name it by. */
    default void file(String path, int ordinal) {}

    /** One row of that file. The tokens are empty when nothing on this build tokenizes. */
    default void row(int rowPosition, List<String> tokens) {}

    /** That file's rows are done: the last call for that ordinal, and where per-file state goes. */
    default void fileDone() {}
  }

  /** A loaded reader. Types keep their own; the pruner only hands it back to {@link #partition}. */
  interface Loaded {}

  /**
   * The data files one plan is asking about, and which of them are still in play.
   *
   * <p>Position {@code i} of {@code paths} is that file's PLAN ORDINAL, and that is the identity
   * every tier answers in. It is deliberately NOT the coverage ordinal a tier's own bitmaps use
   * ({@link io.kahshe.format.Coverage}), for two reasons that decide it rather than merely favour
   * it. A file the index has never seen has no coverage ordinal at all — that is exactly what
   * {@code unknown} means — so a partition expressed in coverage ordinals could not name one of
   * its own three members. And the bloom tier has no ordinal space whatever: its reader is keyed
   * by path, and it runs first, before any tier that owns coverage has loaded. A position in the
   * caller's own list is the one identity every file is guaranteed to have.
   *
   * <p>The list does not shrink as the cascade narrows; {@code inPlay} does. Plan ordinals
   * therefore mean the same thing to every tier and to the caller reading the answer back.
   *
   * @param paths every file the caller asked about, in the caller's own order. Two positions may
   *     carry the same path — Iceberg may split one data file into several scan tasks — and each
   *     is judged on its own, which is what the path-keyed loop this replaced also did
   * @param inPlay the positions no earlier tier has ruled out; a tier must judge these and only
   *     these, and the partition it returns must cover exactly them
   */
  record FileSet(List<String> paths, RoaringBitmap inPlay) {

    /** Every position in play, for the first tier's turn. */
    public static FileSet of(List<String> paths) {
      RoaringBitmap all = new RoaringBitmap();
      all.add(0L, paths.size());
      return new FileSet(List.copyOf(paths), all);
    }

    /** The same files, narrowed to what a tier left in play. */
    public FileSet narrowedTo(RoaringBitmap stillInPlay) {
      return new FileSet(paths, stillInPlay);
    }

    /** The path at a plan ordinal. */
    public String pathOf(int ordinal) {
      return paths.get(ordinal);
    }
  }

  /**
   * One type's three-way answer over the plan ordinals it was handed: proven to match, proven not
   * to, and not spoken for. The three are disjoint and together they are exactly {@code inPlay}.
   *
   * <p>Only {@code absent} may cause a file to be skipped. That is this system's one invariant —
   * a file that MIGHT match is always kept, because pruning one that does match silently drops
   * rows from an answer with no error and no metric — stated in the type system rather than in a
   * comment. {@code unknown} is where every failure lands — an
   * unreadable leaf, a column this tier does not index, a file outside coverage, a literal class
   * it cannot probe — and what to do with it belongs to the caller, not to this type. The
   * principle rather than a list of today's callers, because the next one is not written yet:
   * KEEP it where examining it is merely slower; EXAMINE it where reporting it unexamined would
   * be a claim about a file nobody looked at; REFUSE where an approximate answer would go out
   * labelled exact.
   *
   * <p>A tier with no false negatives can populate {@code absent} and never {@code hits}: the
   * bloom's own contract is that a positive "means nothing" ({@link
   * io.kahshe.format.type.bloom.NgramBloom}), so its honest vocabulary is two of these three
   * words. That is why the answer has three and not two.
   */
  record Partition(RoaringBitmap hits, RoaringBitmap absent, RoaringBitmap unknown) {

    public Partition {
      if (RoaringBitmap.intersects(hits, absent)
          || RoaringBitmap.intersects(hits, unknown)
          || RoaringBitmap.intersects(absent, unknown)) {
        // A file proved present AND absent is not a slower plan, it is two tiers disagreeing about
        // a fact, and the caller that keeps it would be right by accident. Loud, per §2's trap.
        throw new IllegalStateException(
            "a file cannot be two verdicts at once: hits=" + hits + " absent=" + absent
                + " unknown=" + unknown);
      }
      hits = hits.clone();
      absent = absent.clone();
      unknown = unknown.clone();
    }

    /** This type proved nothing about any of them: the answer of a tier that could not load. */
    public static Partition allUnknown(FileSet files) {
      return new Partition(new RoaringBitmap(), new RoaringBitmap(), files.inPlay());
    }

    /** Hits and absences over {@code files}; whatever neither names is unknown. */
    public static Partition of(FileSet files, RoaringBitmap hits, RoaringBitmap absent) {
      RoaringBitmap unknown = files.inPlay().clone();
      unknown.andNot(hits);
      unknown.andNot(absent);
      return new Partition(hits, absent, unknown);
    }

    /**
     * Two answers to two halves of one conjunction, combined into the answer for the whole.
     *
     * <p>Either half proving absence proves the conjunction absent, so absences UNION. Both halves
     * must prove presence for the conjunction to hold, so hits INTERSECT — which is why a half
     * nobody could answer collapses the hits to nothing rather than letting the other half's
     * evidence stand for the pair. That is the conservative direction and the only sound one.
     */
    public Partition and(Partition other) {
      RoaringBitmap inPlay = RoaringBitmap.or(hits, absent, unknown);
      RoaringBitmap bothAbsent = RoaringBitmap.or(absent, other.absent);
      RoaringBitmap bothHit = RoaringBitmap.and(hits, other.hits);
      RoaringBitmap neither = inPlay.clone();
      neither.andNot(bothAbsent);
      neither.andNot(bothHit);
      return new Partition(bothHit, bothAbsent, neither);
    }

    /** What survives this type's turn: everything it did not prove absent. */
    public RoaringBitmap kept() {
      RoaringBitmap kept = hits.clone();
      kept.or(unknown);
      return kept;
    }
  }

  /** What a write published: the leaf paths, the bytes they cost, and the coverage they carry. */
  record Leaves(List<String> paths, long bytes, List<Coverage.Entry> coverage) {
    /** A type that wrote nothing this build. */
    public static final Leaves NONE = new Leaves(List.of(), 0L, List.of());
  }

  /**
   * What a collector is cut for: the column, its contracts, and which tiers this build runs.
   *
   * @param contract the analyzer contract this build tokenizes under. It is resolved from column,
   *     table and deployment settings and stamped into the metadata this build publishes — not
   *     read back off the prior generation, which is why changing it forces a full rebuild rather
   *     than an incremental one
   * @param gramRule the gram contract this build cuts under, resolved the same way
   * @param incremental whether this build extends the prior generation rather than replacing it,
   *     so only the files it does not already cover are read
   */
  record BuildContext(
      Table table,
      String column,
      int fieldId,
      long snapshotId,
      Analyzer.Contract contract,
      Grams.Contract gramRule,
      boolean gramEnabled,
      boolean termEnabled,
      boolean incremental) {}

  /**
   * The publish state one column build holds when its leaves are written.
   *
   * <p>It is one record rather than one per type because the built-in types share it: the gram leaf
   * lives in the term directory and its coverage sits in the term metadata, so the term type's
   * write publishes both.
   *
   * @param incremental whether the prior generation is being extended rather than replaced; a type
   *     that carries leaves forward reads its prior state only when this is set
   * @param partial a checkpoint publish, consistent over the files read so far rather than over the
   *     table: freshness reads it as not current and {@code _count} refuses it, while pruning is
   *     unaffected because an uncovered file is kept
   * @param ordinalRemap old ordinal -> new, when this build renumbered live files; null when it did
   *     not. Non-null obliges a type to rewrite every bitmap it owns in this same publish, since
   *     nothing else will translate them
   * @param gramFromOrdinal the first ordinal the gram leaf's bitmaps describe; below it the gram
   *     tier proves nothing and the bloom decides
   * @param countsExact whether an occurrence count read off this index is exact; false once any
   *     covered file has left the table, and sticky until a full rebuild
   * @param priorTermsPerRange the prior generation's per-range term counts, which let a range no
   *     run has a row for be carried forward by reference. Absent, or the wrong length, every range
   *     is rewritten rather than have its count guessed
   */
  record PublishContext(
      Table table,
      FileIO indexIo,
      String indexRoot,
      String column,
      int fieldId,
      long snapshotId,
      Analyzer.Contract contract,
      Grams.Contract gramRule,
      boolean incremental,
      boolean partial,
      List<Coverage.Entry> coverage,
      Map<String, NgramBloom> blooms,
      IndexMeta priorBloomMeta,
      String priorBloomUuid,
      double bloomFpp,
      boolean termEnabled,
      List<Path> runs,
      TermRunStore runStore,
      List<String> priorAggregates,
      long rowsTotal,
      long tokenTotal,
      String priorTermUuid,
      Map<GramIndexWriter.ByteKey, RoaringBitmap> gramMap,
      int gramFromOrdinal,
      Map<Integer, Integer> ordinalRemap,
      boolean countsExact,
      int mergeThreads,
      List<Long> priorTermsPerRange) {}

  /**
   * What a type loads a reader against.
   *
   * <p>The three built-in readers are the pruner's own, handed over so their caches outlive one
   * plan; a type from outside this module builds its own from {@code config} and {@code metrics}.
   */
  record ReadContext(
      Table table,
      FormatConfig config,
      io.kahshe.common.Metrics metrics,
      IndexStore blooms,
      GramIndex grams,
      TermIndex terms) {}

  /**
   * One plan's question, resolved once: the conjunctive candidates taken from the filter tree, the
   * comparison ranges they were tightened into, and the text hints the caller carried.
   */
  record Probe(
      Table table,
      List<IndexPruner.Candidate> candidates,
      List<IndexPruner.RangeCandidate> ranges,
      List<IndexPruner.ContainsHint> hints,
      FormatConfig config,
      GramProbes gramProbes) {}

  /**
   * The gram layer's answer per candidate, resolved once per plan and read by both the bloom and
   * the gram type: the gram layer is exact over the files it covers, so a file it answers is never
   * probed against a bloom.
   */
  final class GramProbes {
    private IndexPruner.GramProbe[] probes;

    /**
     * The gram answers for this plan's candidates: computed on the first call, returned unchanged
     * afterwards. Memoized without synchronization because an instance belongs to one plan on one
     * thread — shared between threads, or kept past the plan it was made for, it races on the field
     * or serves another plan's answers.
     */
    public IndexPruner.GramProbe[] resolve(
        GramIndex index, Table table, List<IndexPruner.Candidate> candidates) {
      if (probes == null) {
        IndexPruner.GramProbe[] resolved = new IndexPruner.GramProbe[candidates.size()];
        if (index != null) {
          for (int i = 0; i < candidates.size(); i++) {
            resolved[i] = IndexPruner.gramProbe(index, table, candidates.get(i));
          }
        }
        probes = resolved;
      }
      return probes;
    }
  }
}
