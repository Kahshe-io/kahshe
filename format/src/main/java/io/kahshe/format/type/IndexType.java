package io.kahshe.format.type;

import java.io.IOException;
import java.nio.file.Path;
import java.util.List;
import java.util.Map;
import org.apache.iceberg.FileScanTask;
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
 * collection during the read pass, its write at publish, its reader at serve, and its pruning.
 * Implementations are discovered with {@link java.util.ServiceLoader}; see {@link IndexTypes}.
 *
 * <p>Absence is the safe answer everywhere: {@link #load} returns null for an index that is
 * missing, refused or unreadable, and a type that prunes nothing keeps every file. A type may
 * therefore fail without making a plan wrong, only slower.
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

  /** Drops the tasks this type proves cannot match; anything it cannot answer is kept. */
  List<FileScanTask> prune(Loaded loaded, Probe probe, List<FileScanTask> tasks);

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

  /** A loaded reader. Types keep their own; the pruner only hands it back to {@link #prune}. */
  interface Loaded {}

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
