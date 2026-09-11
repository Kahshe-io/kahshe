package io.kahshe.format.type.gram;

import java.util.List;
import org.roaringbitmap.RoaringBitmap;
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

  /**
   * This tier is EXACT over the files it covers, so unlike the bloom it can prove presence as well
   * as absence — and until the seam had a {@code hits} set to put it in, that proof was computed
   * and then thrown away, because a kept file and a proven file were the same list entry.
   */
  @Override
  public Partition partition(Loaded loaded, Probe probe, FileSet files) {
    List<IndexPruner.Candidate> candidates = probe.candidates();
    if (candidates.isEmpty()) {
      return Partition.allUnknown(files);
    }
    IndexPruner.GramProbe[] gramProbes =
        probe.gramProbes().resolve(((Grams) loaded).index(), probe.table(), candidates);
    RoaringBitmap hits = new RoaringBitmap();
    RoaringBitmap absent = new RoaringBitmap();
    for (int ordinal : files.inPlay()) {
      switch (verdict(files.pathOf(ordinal), candidates, gramProbes)) {
        case HIT -> hits.add(ordinal);
        case ABSENT -> absent.add(ordinal);
        case UNKNOWN -> { }
      }
    }
    return Partition.of(files, hits, absent);
  }

  /** One file against the whole conjunction: absent if any clause rules it out, a hit only if
   * every clause proves it, and unknown the moment one clause cannot say. */
  private static Verdict verdict(
      String path, List<IndexPruner.Candidate> candidates, IndexPruner.GramProbe[] gramProbes) {
    boolean everyClauseProved = true;
    for (int i = 0; i < candidates.size(); i++) {
      IndexPruner.GramProbe gramProbe = gramProbes[i];
      if (gramProbe == null) {
        everyClauseProved = false; // no layer for this candidate: the blooms answered it
        continue;
      }
      Integer ordinal = gramProbe.loaded().ordinalOf().get(path);
      if (ordinal == null || ordinal < gramProbe.loaded().fromOrdinal()) {
        everyClauseProved = false; // outside the layer's coverage: never prune
        continue;
      }
      // exact over covered files: a null match is a literal class this layer cannot probe, and
      // a covered file is then kept rather than proved out
      if (gramProbe.match() == null) {
        everyClauseProved = false;
      } else if (!gramProbe.match().contains(ordinal)) {
        return Verdict.ABSENT;
      }
    }
    return everyClauseProved ? Verdict.HIT : Verdict.UNKNOWN;
  }

  private enum Verdict { HIT, ABSENT, UNKNOWN }
}
