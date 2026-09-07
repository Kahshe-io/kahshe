package io.kahshe.format.type.term;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import org.apache.iceberg.FileScanTask;
import org.apache.iceberg.Table;
import org.roaringbitmap.RoaringBitmap;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import io.kahshe.format.FormatConfig;
import io.kahshe.format.IndexPruner;
import io.kahshe.analysis.analyzer.Analyzer;
import io.kahshe.format.type.IndexType;
import io.kahshe.format.type.bloom.NgramBloom;

/**
 * The term dictionary and its aggregate range leaves behind {@link IndexType}: {@link RunBuffer}
 * and {@link RunMerger} build it, {@link TermIndexWriter} writes it, {@link TermIndex} reads it.
 *
 * <p>Its write also publishes the gram tier's leaf and coverage, because both live under this
 * type's directory and metadata.
 */
public final class TermIndexType implements IndexType {

  /** This type's key under {@code snapshots[0].leaves} of the term metadata. */
  public static final String KEY = "aggregate";

  private static final Logger LOG = LoggerFactory.getLogger(TermIndexType.class);

  /** The term reader for one plan; it resolves a field's index itself, and caches it. */
  record Terms(TermIndex index) implements Loaded {}

  @Override
  public String key() {
    return KEY;
  }

  /** Nothing: terms stream into the read pass's per-reader arenas, not through a collector. */
  @Override
  public Collector collector(BuildContext ctx) {
    return Collector.NONE;
  }

  @Override
  public Leaves write(PublishContext ctx) throws IOException {
    TermIndexWriter.finish(
        ctx.table(), ctx.indexIo(), ctx.indexRoot(), ctx.fieldId(), ctx.column(), ctx.snapshotId(),
        ctx.coverage(), ctx.termEnabled(), ctx.contract(), ctx.gramRule(), ctx.runs(),
        ctx.runStore(), ctx.priorAggregates(), ctx.rowsTotal(), ctx.tokenTotal(),
        ctx.priorTermUuid(), ctx.gramMap(), ctx.gramFromOrdinal(), ctx.ordinalRemap(),
        ctx.countsExact(), ctx.mergeThreads(), ctx.priorTermsPerRange(), ctx.partial());
    return new Leaves(List.of(), 0L, ctx.coverage());
  }

  @Override
  public Loaded load(ReadContext ctx) {
    return ctx.terms() == null ? null : new Terms(ctx.terms());
  }

  /** The cost order within this tier: values, then value prefixes, ranges, then terms. */
  @Override
  public List<FileScanTask> prune(Loaded loaded, Probe probe, List<FileScanTask> tasks) {
    TermIndex termIndex = ((Terms) loaded).index();
    Table table = probe.table();
    FormatConfig config = probe.config();
    List<FileScanTask> kept = tasks;
    for (IndexPruner.Candidate candidate : probe.candidates()) {
      if (candidate.mode() == NgramBloom.Mode.EQ) {
        kept = pruneByValues(termIndex, table, candidate, kept);
      } else if (candidate.mode() == NgramBloom.Mode.STARTS_WITH) {
        kept = pruneByValuePrefix(termIndex, config, table, candidate, kept);
      }
    }
    for (IndexPruner.RangeCandidate range : probe.ranges()) {
      kept = pruneByValueRange(termIndex, config, table, range, kept);
    }
    for (IndexPruner.ContainsHint hint : probe.hints()) {
      if (hint.kind() == IndexPruner.HintKind.MATCH) {
        kept = pruneByTerms(termIndex, table, hint, kept);
      } else if (hint.kind() == IndexPruner.HintKind.PREFIX) {
        kept = pruneByTermPrefix(termIndex, config, table, hint, kept);
      }
    }
    return kept;
  }

  /**
   * Token pruning via the aggregate term layer: the query value is tokenized server-side with the
   * pinned analyzer; multi-token values prune conjunctively (conservative). Files outside the
   * index's coverage are always kept; a non-indexable token disables pruning entirely.
   */
  private static List<FileScanTask> pruneByTerms(
      TermIndex termIndex, Table table, IndexPruner.ContainsHint hint, List<FileScanTask> tasks) {
    int fieldId = hint.fieldId();
    if (fieldId < 0) {
      var field = table.schema().findField(hint.column());
      if (field == null) {
        return tasks;
      }
      fieldId = field.fieldId();
    }
    TermIndex.Loaded index = termIndex.forField(table, fieldId);
    if (index == null) {
      return tasks;
    }
    List<String> tokens = index.contract().queryTerms(hint.value());
    if (tokens.isEmpty()) {
      return tasks;
    }
    for (String token : tokens) {
      if (!index.contract().isIndexable(token)) {
        // The rule comes from the index, not from this proxy's configuration: probing for a token
        // the build never wrote would find it absent, and absence prunes. A v1 index is read
        // under v1's rule for the same reason.
        return tasks; // not admitted by this index's analyzer; no pruning
      }
    }
    // One lookup for the whole query, before the file loop. The aggregate is partitioned by term
    // range, so this reads only the leaves these tokens fall in rather than the vocabulary. It is
    // hoisted out of the loop because it is IO: inside, it would repeat per data file.
    Map<String, TermIndex.Entry> entries;
    try {
      entries = termIndex.entriesFor(table, index, tokens);
    } catch (IOException | RuntimeException e) {
      // advisory-keep: a leaf we cannot read is not an absent term. Treating it as absent would
      // prune every file, and the files pruned are exactly the ones that match.
      LOG.warn("term lookup failed for {}; keeping every file", tokens, e);
      return tasks;
    }
    List<FileScanTask> kept = new ArrayList<>(tasks.size());
    for (FileScanTask task : tasks) {
      Integer ordinal = index.ordinalOf().get(task.file().location());
      if (ordinal == null) {
        kept.add(task); // outside index coverage: never prune
        continue;
      }
      boolean all = true;
      for (String token : tokens) {
        TermIndex.Entry entry = entries.get(token);
        if (entry == null || !entry.ordinals().contains(ordinal)) {
          all = false;
          break;
        }
      }
      if (all) {
        kept.add(task);
      }
    }
    return kept;
  }

  /**
   * Plain {@code =}/{@code IN} on a column whose index names the whole-value analyzer: each
   * literal is one exact term and the files holding any literal are the union. A tokens-kind index
   * is not probed here: equality on it is already served as a substring by the gram and bloom
   * tiers, so a conjunctive token probe would add a term lookup to every equality plan on every
   * indexed column for no proven gain.
   */
  private static List<FileScanTask> pruneByValues(
      TermIndex termIndex, Table table, IndexPruner.Candidate candidate, List<FileScanTask> tasks) {
    var field = table.schema().findField(candidate.column());
    if (field == null) {
      return tasks;
    }
    TermIndex.Loaded index = termIndex.forField(table, field.fieldId());
    if (index == null || index.contract().kind() != Analyzer.Kind.VALUE) {
      return tasks;
    }
    for (String literal : candidate.literals()) {
      // the empty value has no term, and a value over the cap was never written: no probe, no prune
      if (literal.isEmpty() || !index.contract().isIndexable(literal)) {
        return tasks;
      }
    }
    Map<String, TermIndex.Entry> entries;
    try {
      entries = termIndex.entriesFor(table, index, candidate.literals());
    } catch (IOException | RuntimeException e) {
      LOG.warn("term lookup failed for {}; keeping every file", candidate.literals(), e);
      return tasks;
    }
    List<FileScanTask> kept = new ArrayList<>(tasks.size());
    for (FileScanTask task : tasks) {
      Integer ordinal = index.ordinalOf().get(task.file().location());
      if (ordinal == null) {
        kept.add(task); // outside index coverage: never prune
        continue;
      }
      for (String literal : candidate.literals()) {
        TermIndex.Entry entry = entries.get(literal);
        if (entry != null && entry.ordinals().contains(ordinal)) {
          kept.add(task);
          break;
        }
      }
    }
    return kept;
  }

  /**
   * {@code STARTS_WITH} on a whole-value column -- {@code LIKE 'x%'} in stock SQL -- is a range of
   * the term dictionary: every address, id or key under the prefix, joined. Union per literal,
   * like {@code IN}. A tokens column is not served here: a value starting with a text does not
   * mean any of its terms does.
   */
  private static List<FileScanTask> pruneByValuePrefix(
      TermIndex termIndex, FormatConfig config, Table table, IndexPruner.Candidate candidate,
      List<FileScanTask> tasks) {
    var field = table.schema().findField(candidate.column());
    if (field == null) {
      return tasks;
    }
    TermIndex.Loaded index = termIndex.forField(table, field.fieldId());
    if (index == null || index.contract().kind() != Analyzer.Kind.VALUE) {
      return tasks;
    }
    RoaringBitmap union = new RoaringBitmap();
    for (String literal : candidate.literals()) {
      TermIndex.PrefixEntries run = prefixRun(termIndex, config, table, index, literal);
      if (run == null) {
        return tasks;
      }
      union.or(run.ordinals());
    }
    return keepByOrdinals(tasks, index, union);
  }

  /**
   * A string comparison range on a whole-value column -- {@code >=}, {@code <}, {@code BETWEEN},
   * and {@code LIKE 'x%'} as Trino pushes it down (a range from the prefix to its successor) --
   * is a run of the term dictionary in the engine's own string order. The two sides of a
   * conjunction on one column are tightened into one range first, so the run is what the
   * engine keeps, not the union of two halves. STRING columns only: decimal text does not
   * order like the number it spells.
   */
  private static List<FileScanTask> pruneByValueRange(
      TermIndex termIndex, FormatConfig config, Table table, IndexPruner.RangeCandidate range,
      List<FileScanTask> tasks) {
    var field = table.schema().findField(range.column());
    if (field == null || field.type().typeId() != org.apache.iceberg.types.Type.TypeID.STRING) {
      return tasks;
    }
    TermIndex.Loaded index = termIndex.forField(table, field.fieldId());
    if (index == null || index.contract().kind() != Analyzer.Kind.VALUE) {
      return tasks;
    }
    int cap = config == null ? 100_000 : config.prefixMaxTerms();
    TermIndex.PrefixEntries run;
    try {
      run = termIndex.entriesForRange(
          table, index, range.lower(), range.lowerInclusive(), range.upper(), range.upperInclusive(), cap);
    } catch (IOException | RuntimeException e) {
      LOG.warn("range lookup failed on {}; keeping every file", range.column(), e);
      return tasks;
    }
    if (run == null) {
      LOG.warn(
          "range on {} matches more than {} terms (KAHSHE_PREFIX_MAX_TERMS); keeping every file",
          range.column(), cap);
      return tasks;
    }
    return keepByOrdinals(tasks, index, run.ordinals());
  }

  /**
   * A {@code match_prefix} hint: every term starting with the value, on any indexed column. Under
   * a tokens contract the prefix is lowercased first and must be made of characters a term can
   * start with; otherwise nothing is probed and every file is kept.
   */
  private static List<FileScanTask> pruneByTermPrefix(
      TermIndex termIndex, FormatConfig config, Table table, IndexPruner.ContainsHint hint,
      List<FileScanTask> tasks) {
    int fieldId = hint.fieldId();
    if (fieldId < 0) {
      var field = table.schema().findField(hint.column());
      if (field == null) {
        return tasks;
      }
      fieldId = field.fieldId();
    }
    TermIndex.Loaded index = termIndex.forField(table, fieldId);
    if (index == null) {
      return tasks;
    }
    String prefix =
        index.contract().kind() == Analyzer.Kind.VALUE
            ? hint.value()
            : hint.value().toLowerCase(java.util.Locale.ROOT);
    if (!index.contract().prefixable(prefix)) {
      return tasks;
    }
    TermIndex.PrefixEntries run = prefixRun(termIndex, config, table, index, prefix);
    return run == null ? tasks : keepByOrdinals(tasks, index, run.ordinals());
  }

  /** The dictionary run under a prefix, or null when it is empty, capped, or unreadable. */
  private static TermIndex.PrefixEntries prefixRun(
      TermIndex termIndex, FormatConfig config, Table table, TermIndex.Loaded index, String prefix) {
    if (prefix.isEmpty()) {
      return null;
    }
    int cap = config == null ? 100_000 : config.prefixMaxTerms();
    try {
      TermIndex.PrefixEntries run = termIndex.entriesForPrefix(table, index, prefix, cap);
      if (run == null) {
        LOG.warn(
            "prefix '{}' matches more than {} terms (KAHSHE_PREFIX_MAX_TERMS); keeping every file",
            prefix, cap);
      }
      return run;
    } catch (IOException | RuntimeException e) {
      LOG.warn("prefix lookup failed for '{}'; keeping every file", prefix, e);
      return null;
    }
  }

  private static List<FileScanTask> keepByOrdinals(
      List<FileScanTask> tasks, TermIndex.Loaded index, RoaringBitmap ordinals) {
    List<FileScanTask> kept = new ArrayList<>(tasks.size());
    for (FileScanTask task : tasks) {
      Integer ordinal = index.ordinalOf().get(task.file().location());
      if (ordinal == null || ordinals.contains(ordinal)) {
        kept.add(task); // outside index coverage: never prune
      }
    }
    return kept;
  }
}
