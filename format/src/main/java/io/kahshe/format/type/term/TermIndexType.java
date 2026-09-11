package io.kahshe.format.type.term;

import java.io.IOException;
import java.util.List;
import java.util.Map;
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

  /**
   * The cost order within this tier: values, then value prefixes, ranges, then terms.
   *
   * <p>Every clause is put to the SAME file set rather than to the survivors of the clause before
   * it. The two are equivalent — a clause judges each file on its own, and the dictionary lookups
   * are hoisted out of the file loop already — and threading a narrowed set through would optimise
   * the rare multi-clause plan (item 35 measured one two-indexed-column conjunction in 68
   * benchmark queries) at the cost of making the fold conditional.
   */
  @Override
  public Partition partition(Loaded loaded, Probe probe, FileSet files) {
    TermIndex termIndex = ((Terms) loaded).index();
    Table table = probe.table();
    FormatConfig config = probe.config();
    Partition answer = null;
    for (IndexPruner.Candidate candidate : probe.candidates()) {
      if (candidate.mode() == NgramBloom.Mode.EQ) {
        answer = merge(answer, partitionByValues(termIndex, table, candidate, files));
      } else if (candidate.mode() == NgramBloom.Mode.STARTS_WITH) {
        answer = merge(answer, partitionByValuePrefix(termIndex, config, table, candidate, files));
      }
    }
    for (IndexPruner.RangeCandidate range : probe.ranges()) {
      answer = merge(answer, partitionByValueRange(termIndex, config, table, range, files));
    }
    for (IndexPruner.ContainsHint hint : probe.hints()) {
      if (hint.kind() == IndexPruner.HintKind.MATCH) {
        answer = merge(answer, partitionByTerms(termIndex, table, hint, files));
      } else if (hint.kind() == IndexPruner.HintKind.PREFIX) {
        answer = merge(answer, partitionByTermPrefix(termIndex, config, table, hint, files));
      }
    }
    // Nothing this tier could be asked: it has proved nothing, which is not the same as proving
    // every file matches, and only the three-way answer can say so.
    return answer == null ? Partition.allUnknown(files) : answer;
  }

  /** One more clause of the conjunction, or the first. */
  private static Partition merge(Partition soFar, Partition clause) {
    return soFar == null ? clause : soFar.and(clause);
  }

  /**
   * Token pruning via the aggregate term layer: the query value is tokenized server-side with the
   * pinned analyzer; multi-token values must ALL be present, so their postings intersect. Files
   * outside the index's coverage are unknown; a non-indexable token makes the whole clause
   * unanswerable.
   */
  private static Partition partitionByTerms(
      TermIndex termIndex, Table table, IndexPruner.ContainsHint hint, FileSet files) {
    int fieldId = hint.fieldId();
    if (fieldId < 0) {
      var field = table.schema().findField(hint.column());
      if (field == null) {
        return Partition.allUnknown(files);
      }
      fieldId = field.fieldId();
    }
    TermIndex.Loaded index = termIndex.forField(table, fieldId);
    if (index == null) {
      return Partition.allUnknown(files);
    }
    List<String> tokens = index.contract().queryTerms(hint.value());
    if (tokens.isEmpty()) {
      return Partition.allUnknown(files);
    }
    for (String token : tokens) {
      if (!index.contract().isIndexable(token)) {
        // The rule comes from the index, not from this proxy's configuration: probing for a token
        // the build never wrote would find it absent, and absence prunes. A v1 index is read
        // under v1's rule for the same reason.
        return Partition.allUnknown(files); // not admitted by this index's analyzer
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
      // prune every file, and the files pruned are exactly the ones that match. Unknown is that
      // rule said once, in the place every caller has to read it.
      LOG.warn("term lookup failed for {}; keeping every file", tokens, e);
      return Partition.allUnknown(files);
    }
    RoaringBitmap holdingAll = null;
    for (String token : tokens) {
      TermIndex.Entry entry = entries.get(token);
      RoaringBitmap holding = entry == null ? new RoaringBitmap() : entry.ordinals();
      if (holdingAll == null) {
        holdingAll = holding.clone();
      } else {
        holdingAll.and(holding);
      }
    }
    return partitionByCoverage(files, index, holdingAll);
  }

  /**
   * Plain {@code =}/{@code IN} on a column whose index names the whole-value analyzer: each
   * literal is one exact term and the files holding any literal are the union. A tokens-kind index
   * is not probed here: equality on it is already served as a substring by the gram and bloom
   * tiers, so a conjunctive token probe would add a term lookup to every equality plan on every
   * indexed column for no proven gain.
   */
  private static Partition partitionByValues(
      TermIndex termIndex, Table table, IndexPruner.Candidate candidate, FileSet files) {
    var field = table.schema().findField(candidate.column());
    if (field == null) {
      return Partition.allUnknown(files);
    }
    TermIndex.Loaded index = termIndex.forField(table, field.fieldId());
    if (index == null || index.contract().kind() != Analyzer.Kind.VALUE) {
      return Partition.allUnknown(files);
    }
    for (String literal : candidate.literals()) {
      // the empty value has no term, and a value over the cap was never written: no probe, no prune
      if (literal.isEmpty() || !index.contract().isIndexable(literal)) {
        return Partition.allUnknown(files);
      }
    }
    Map<String, TermIndex.Entry> entries;
    try {
      entries = termIndex.entriesFor(table, index, candidate.literals());
    } catch (IOException | RuntimeException e) {
      LOG.warn("term lookup failed for {}; keeping every file", candidate.literals(), e);
      return Partition.allUnknown(files);
    }
    RoaringBitmap holdingAny = new RoaringBitmap();
    for (String literal : candidate.literals()) {
      TermIndex.Entry entry = entries.get(literal);
      if (entry != null) {
        holdingAny.or(entry.ordinals());
      }
    }
    return partitionByCoverage(files, index, holdingAny);
  }

  /**
   * {@code STARTS_WITH} on a whole-value column -- {@code LIKE 'x%'} in stock SQL -- is a range of
   * the term dictionary: every address, id or key under the prefix, joined. Union per literal,
   * like {@code IN}. A tokens column is not served here: a value starting with a text does not
   * mean any of its terms does.
   */
  private static Partition partitionByValuePrefix(
      TermIndex termIndex, FormatConfig config, Table table, IndexPruner.Candidate candidate,
      FileSet files) {
    var field = table.schema().findField(candidate.column());
    if (field == null) {
      return Partition.allUnknown(files);
    }
    TermIndex.Loaded index = termIndex.forField(table, field.fieldId());
    if (index == null || index.contract().kind() != Analyzer.Kind.VALUE) {
      return Partition.allUnknown(files);
    }
    RoaringBitmap union = new RoaringBitmap();
    for (String literal : candidate.literals()) {
      TermIndex.PrefixEntries run = prefixRun(termIndex, config, table, index, literal);
      if (run == null) {
        return Partition.allUnknown(files);
      }
      union.or(run.ordinals());
    }
    return partitionByCoverage(files, index, union);
  }

  /**
   * A string comparison range on a whole-value column -- {@code >=}, {@code <}, {@code BETWEEN},
   * and {@code LIKE 'x%'} as Trino pushes it down (a range from the prefix to its successor) --
   * is a run of the term dictionary in the engine's own string order. The two sides of a
   * conjunction on one column are tightened into one range first, so the run is what the
   * engine keeps, not the union of two halves. STRING columns only: decimal text does not
   * order like the number it spells.
   */
  private static Partition partitionByValueRange(
      TermIndex termIndex, FormatConfig config, Table table, IndexPruner.RangeCandidate range,
      FileSet files) {
    var field = table.schema().findField(range.column());
    if (field == null || field.type().typeId() != org.apache.iceberg.types.Type.TypeID.STRING) {
      return Partition.allUnknown(files);
    }
    TermIndex.Loaded index = termIndex.forField(table, field.fieldId());
    if (index == null || index.contract().kind() != Analyzer.Kind.VALUE) {
      return Partition.allUnknown(files);
    }
    int cap = config == null ? 100_000 : config.prefixMaxTerms();
    TermIndex.PrefixEntries run;
    try {
      run = termIndex.entriesForRange(
          table, index, range.lower(), range.lowerInclusive(), range.upper(), range.upperInclusive(), cap);
    } catch (IOException | RuntimeException e) {
      LOG.warn("range lookup failed on {}; keeping every file", range.column(), e);
      return Partition.allUnknown(files);
    }
    if (run == null) {
      LOG.warn(
          "range on {} matches more than {} terms (KAHSHE_PREFIX_MAX_TERMS); keeping every file",
          range.column(), cap);
      return Partition.allUnknown(files);
    }
    return partitionByCoverage(files, index, run.ordinals());
  }

  /**
   * A {@code match_prefix} hint: every term starting with the value, on any indexed column. Under
   * a tokens contract the prefix is lowercased first and must be made of characters a term can
   * start with; otherwise nothing is probed and the clause is unanswerable.
   */
  private static Partition partitionByTermPrefix(
      TermIndex termIndex, FormatConfig config, Table table, IndexPruner.ContainsHint hint,
      FileSet files) {
    int fieldId = hint.fieldId();
    if (fieldId < 0) {
      var field = table.schema().findField(hint.column());
      if (field == null) {
        return Partition.allUnknown(files);
      }
      fieldId = field.fieldId();
    }
    TermIndex.Loaded index = termIndex.forField(table, fieldId);
    if (index == null) {
      return Partition.allUnknown(files);
    }
    String prefix =
        index.contract().kind() == Analyzer.Kind.VALUE
            ? hint.value()
            : hint.value().toLowerCase(java.util.Locale.ROOT);
    if (!index.contract().prefixable(prefix)) {
      return Partition.allUnknown(files);
    }
    TermIndex.PrefixEntries run = prefixRun(termIndex, config, table, index, prefix);
    return run == null
        ? Partition.allUnknown(files)
        : partitionByCoverage(files, index, run.ordinals());
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

  /**
   * The one place a coverage ordinal becomes a plan ordinal, and the whole tier's verdict rule in
   * four lines: a file this index covers is a HIT when the clause's postings name it and an
   * ABSENCE when they do not — the dictionary is exact over what it covers — and a file it does
   * not cover is UNKNOWN, never an absence, because a term missing from the dictionary is missing
   * from the COVERED files and only those. That last branch is the invariant — never prune a file
   * this index cannot speak for — and it used to be spelled with the same {@code add} as the hit
   * above it.
   *
   * <p>Public because it has more than one caller and the rule must not exist twice. One folds
   * the result into a conjunction and collapses it back to survivors; another keeps the three
   * verdicts apart, because reporting an unexamined file as a match is a claim about a file
   * nobody looked at. The FETCH stays with each caller, because callers disagree about failure —
   * one swallows an unreadable leaf and keeps every file, another refuses rather than answer — but
   * the READING of what was fetched is here, once.
   *
   * @param matching the coverage ordinals this clause's postings name; empty is a valid answer and
   *     means every covered file is a proven absence
   */
  public static Partition partitionByCoverage(
      FileSet files, TermIndex.Loaded index, RoaringBitmap matching) {
    RoaringBitmap hits = new RoaringBitmap();
    RoaringBitmap absent = new RoaringBitmap();
    for (int ordinal : files.inPlay()) {
      Integer covered = index.ordinalOf().get(files.pathOf(ordinal));
      if (covered == null) {
        continue; // outside index coverage: not spoken for
      }
      if (matching.contains(covered)) {
        hits.add(ordinal);
      } else {
        absent.add(ordinal);
      }
    }
    return Partition.of(files, hits, absent);
  }
}
