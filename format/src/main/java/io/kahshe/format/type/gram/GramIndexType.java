package io.kahshe.format.type.gram;

import java.util.ArrayList;
import java.util.List;
import org.apache.iceberg.FileScanTask;
import io.kahshe.format.IndexPruner;
import io.kahshe.format.type.IndexType;
import io.kahshe.format.type.term.TermIndexType;
import io.kahshe.format.type.term.TermIndexWriter;

/**
 * The exact gram tier behind {@link IndexType}: {@link GramAccumulator} builds it,
 * {@link GramIndexWriter} writes it, {@link GramIndex} reads it.
 *
 * <p>Its leaf lives in the term directory and its coverage sits in the term metadata, so its write
 * is the term type's — {@link TermIndexWriter#finish} publishes both in one metadata write.
 */
public final class GramIndexType implements IndexType {

  /** This type's key under {@code snapshots[0].leaves} of the term metadata. */
  public static final String KEY = "grams";

  /** The gram reader for one plan. */
  record Grams(GramIndex index) implements Loaded {}

  @Override
  public String key() {
    return KEY;
  }

  /** Nothing: a file's grams are accumulated from its raw text inside the read, not row by row. */
  @Override
  public Collector collector(BuildContext ctx) {
    return Collector.NONE;
  }

  /** Nothing of its own: {@link TermIndexType} publishes this tier's leaf and its coverage. */
  @Override
  public Leaves write(PublishContext ctx) {
    return Leaves.NONE;
  }

  @Override
  public Loaded load(ReadContext ctx) {
    return ctx.grams() == null ? null : new Grams(ctx.grams());
  }

  @Override
  public List<FileScanTask> prune(Loaded loaded, Probe probe, List<FileScanTask> tasks) {
    List<IndexPruner.Candidate> candidates = probe.candidates();
    if (candidates.isEmpty()) {
      return tasks;
    }
    IndexPruner.GramProbe[] gramProbes =
        probe.gramProbes().resolve(((Grams) loaded).index(), probe.table(), candidates);
    List<FileScanTask> kept = new ArrayList<>(tasks.size());
    for (FileScanTask task : tasks) {
      if (mightMatch(task, candidates, gramProbes)) {
        kept.add(task);
      }
    }
    return kept;
  }

  private static boolean mightMatch(
      FileScanTask task, List<IndexPruner.Candidate> candidates,
      IndexPruner.GramProbe[] gramProbes) {
    String path = task.file().location();
    for (int i = 0; i < candidates.size(); i++) {
      IndexPruner.GramProbe gramProbe = gramProbes[i];
      if (gramProbe == null) {
        continue; // no layer for this candidate: the blooms answered it
      }
      Integer ordinal = gramProbe.loaded().ordinalOf().get(path);
      if (ordinal == null || ordinal < gramProbe.loaded().fromOrdinal()) {
        continue; // outside the layer's coverage: never prune
      }
      // exact over covered files: a null match is a literal class this layer cannot probe, and
      // a covered file is then kept rather than proved out
      if (gramProbe.match() != null && !gramProbe.match().contains(ordinal)) {
        return false;
      }
    }
    return true;
  }
}
