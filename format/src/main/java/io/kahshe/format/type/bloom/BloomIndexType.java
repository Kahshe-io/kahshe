package io.kahshe.format.type.bloom;

import java.io.IOException;
import java.util.List;
import org.apache.iceberg.Table;
import org.roaringbitmap.RoaringBitmap;
import io.kahshe.format.IndexPruner;
import io.kahshe.format.type.IndexType;
import io.kahshe.format.type.gram.GramIndex;

/**
 * The n-gram bloom tier behind {@link IndexType}: {@link BloomLeaf} writes it, {@link IndexStore}
 * reads it, {@link NgramBloom} answers it.
 *
 * <p>It holds the gram reader as well as its own, because the gram layer is exact over the files it
 * covers and those files are never probed against a bloom — the two tiers answer one loop between
 * them.
 */
public final class BloomIndexType implements IndexType {

  /**
   * This type's identity in the registry — unique across it, and what
   * {@link io.kahshe.format.type.IndexTypes#COST_ORDER} ranks. Unlike the other two tiers it names
   * nothing on disk: this tier's directory is {@link IndexMeta#dir} and its leaves are listed by
   * name inside its own metadata document.
   */
  public static final String KEY = "bloom";

  /** The blooms and the gram answers a covered file is decided by instead. */
  record Blooms(IndexStore store, GramIndex grams) implements Loaded {}

  @Override
  public String key() {
    return KEY;
  }

  /** Nothing: a file's bloom is built from its raw text inside the read, not row by row. */
  @Override
  public Collector collector(BuildContext ctx) {
    return Collector.NONE;
  }

  @Override
  public Leaves write(PublishContext ctx) throws IOException {
    long bytes =
        BloomLeaf.write(
            ctx.table(), ctx.indexIo(), ctx.indexRoot(), ctx.column(), ctx.fieldId(),
            ctx.snapshotId(), ctx.blooms(), ctx.incremental() ? ctx.priorBloomMeta() : null,
            ctx.priorBloomUuid(), ctx.bloomFpp(), ctx.gramRule());
    // the leaf list lives in this tier's own IndexMeta, which BloomLeaf.write has just published
    return new Leaves(List.of(), bytes, List.of());
  }

  @Override
  public Loaded load(ReadContext ctx) {
    return ctx.blooms() == null ? null : new Blooms(ctx.blooms(), ctx.grams());
  }

  /**
   * Absences only, and never a hit: a bloom's positive "means nothing" ({@link NgramBloom}), so
   * this tier can prove a file out and can never prove one in. Its whole answer is
   * {@code absent} and {@code unknown} — which is the case that decides the three-way shape,
   * because a two-way return leaves it spelling "I cannot say" with the same word the exact tiers
   * use for a proof.
   */
  @Override
  public Partition partition(Loaded loaded, Probe probe, FileSet files) {
    List<IndexPruner.Candidate> candidates = probe.candidates();
    if (candidates.isEmpty()) {
      return Partition.allUnknown(files);
    }
    Blooms blooms = (Blooms) loaded;
    IndexPruner.GramProbe[] gramProbes =
        probe.gramProbes().resolve(blooms.grams(), probe.table(), candidates);
    RoaringBitmap absent = new RoaringBitmap();
    for (int ordinal : files.inPlay()) {
      if (!mightMatch(
          blooms.store(), probe.table(), files.pathOf(ordinal), candidates, gramProbes)) {
        absent.add(ordinal);
      }
    }
    return Partition.of(files, new RoaringBitmap(), absent);
  }

  private static boolean mightMatch(
      IndexStore store, Table table, String path, List<IndexPruner.Candidate> candidates,
      IndexPruner.GramProbe[] gramProbes) {
    for (int i = 0; i < candidates.size(); i++) {
      IndexPruner.Candidate candidate = candidates.get(i);
      IndexPruner.GramProbe gramProbe = gramProbes[i];
      if (gramProbe != null) {
        Integer ordinal = gramProbe.loaded().ordinalOf().get(path);
        if (ordinal != null && ordinal >= gramProbe.loaded().fromOrdinal()) {
          // the gram layer is exact over its covered files, so its answer is final for this
          // candidate: a bloom could only re-approve what it already kept
          continue;
        }
      }
      IndexStore.LoadedIndex index = store.forColumn(table, candidate.column());
      if (index == null) {
        continue;
      }
      NgramBloom bloom = index.blooms().get(path);
      if (bloom == null) {
        continue; // unindexed file: never prune
      }
      // conjunctive candidate: for IN, any literal may match; all-absent proves the file out
      boolean any = false;
      for (String literal : candidate.literals()) {
        if (bloom.mightContain(literal, candidate.mode())) {
          any = true;
          break;
        }
      }
      if (!any) {
        return false;
      }
    }
    return true;
  }
}
