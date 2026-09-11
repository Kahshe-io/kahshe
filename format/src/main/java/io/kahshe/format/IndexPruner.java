package io.kahshe.format;

import io.kahshe.analysis.Canonical;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import org.apache.iceberg.FileScanTask;
import org.apache.iceberg.Table;
import org.apache.iceberg.expressions.And;
import org.apache.iceberg.expressions.Expression;
import org.apache.iceberg.expressions.Literal;
import org.apache.iceberg.expressions.NamedReference;
import org.apache.iceberg.expressions.UnboundPredicate;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import io.kahshe.format.type.IndexType;
import io.kahshe.format.type.IndexTypes;
import io.kahshe.format.type.bloom.IndexStore;
import io.kahshe.format.type.bloom.NgramBloom;
import io.kahshe.format.type.gram.GramIndex;
import io.kahshe.format.type.term.TermIndex;

/**
 * Drops file-scan-tasks whose indexes prove they cannot match the filter: the plan is put to every
 * registered {@link IndexType} in {@link IndexTypes#inCostOrder()} — the exact gram layer is final
 * for the files it covers, the bloom index answers everything else, and the term dictionary
 * follows.
 *
 * <p>Candidates are taken only from conjunctive positions of the expression tree (a predicate
 * under OR/NOT cannot prune on its own). Files without an index entry are always kept, and so is
 * every file of a type that cannot be loaded at all: a stale, partial or unreadable index makes
 * planning slower, never wrong.
 */
public final class IndexPruner {
  private static final Logger LOG = LoggerFactory.getLogger(IndexPruner.class);

  /** Which text question a {@link ContainsHint} asks; the kind decides which tier can answer it. */
  public enum HintKind {
    /** Substring semantics, served by the n-gram bloom and the exact gram layer. */
    CONTAINS,
    /** Token equality under the index's own analyzer contract, served by the term dictionary. */
    MATCH,
    /** Every term starting with the value: a range of the term dictionary. */
    PREFIX
  }

  /**
   * A text predicate carried by kahshe's filter extensions. The column arrives by name (the
   * legacy contains node) or by field id (the draft expressions spec's apply/reference form,
   * which mandates ID references); {@code fieldId} is negative when a name was given.
   */
  public record ContainsHint(String column, int fieldId, String value, HintKind kind) {
    public ContainsHint(String column, String value, HintKind kind) {
      this(column, -1, value, kind);
    }
  }

  /** One conjunctive predicate in canonical form: the literals to look for, and how. */
  public record Candidate(String column, List<String> literals, NgramBloom.Mode mode) {}

  /** A comparison range on a STRING column, in the engine's own string order; a null bound is open. */
  public record RangeCandidate(
      String column, String lower, boolean lowerInclusive, String upper, boolean upperInclusive) {
    RangeCandidate tighten(RangeCandidate other) {
      java.util.Comparator<CharSequence> order = org.apache.iceberg.types.Comparators.charSequences();
      String lo = lower;
      boolean loInc = lowerInclusive;
      if (other.lower != null && (lo == null || order.compare(other.lower, lo) > 0
          || (order.compare(other.lower, lo) == 0 && !other.lowerInclusive))) {
        lo = other.lower;
        loInc = other.lowerInclusive;
      }
      String hi = upper;
      boolean hiInc = upperInclusive;
      if (other.upper != null && (hi == null || order.compare(other.upper, hi) < 0
          || (order.compare(other.upper, hi) == 0 && !other.upperInclusive))) {
        hi = other.upper;
        hiInc = other.upperInclusive;
      }
      return new RangeCandidate(column, lo, loInc, hi, hiInc);
    }
  }

  /**
   * A candidate's gram-layer view, resolved once per plan: the match bitmap answers every covered
   * file. A null match means the literal class cannot be probed (short CONTAINS/STARTS_WITH) —
   * covered files are kept without consulting the bloom, which could only agree.
   */
  public record GramProbe(GramIndex.Loaded loaded, org.roaringbitmap.RoaringBitmap match) {}

  private final IndexStore store;
  private final TermIndex termIndex;
  private final FormatConfig config;
  private final io.kahshe.common.Metrics metrics;
  // injectable for tests; null when KAHSHE_GRAM_INDEX=false — bloom-only behavior
  GramIndex gramIndex;

  public IndexPruner(TermIndex termIndex, io.kahshe.common.Metrics metrics, FormatConfig config) {
    this.termIndex = termIndex;
    this.config = config;
    this.metrics = metrics;
    this.store = new IndexStore(config, metrics);
    this.gramIndex = config.gramIndexEnabled() ? new GramIndex(config, metrics) : null;
  }

  /**
   * The plan path: the tasks that survive every registered tier's attempt to prove they cannot
   * match.
   *
   * <p>The predicate arrives in two pieces because the two travel differently. {@code filter} is
   * the standard Iceberg expression, and only its conjunctive positions are read. {@code hints} are
   * kahshe's text extensions — contains, match, match_prefix — which Iceberg's expression algebra
   * cannot express at all, so a caller strips them out of the filter before parsing it and hands
   * them over here; the proxy's {@code ContainsExtractor} is the one that does this.
   *
   * <p>The result is a subset of {@code tasks}, in the order given; the argument list itself is
   * never modified.
   */
  public List<FileScanTask> prune(
      Table table, Expression filter, List<ContainsHint> hints, List<FileScanTask> tasks) {
    List<Candidate> candidates = new ArrayList<>();
    Map<String, RangeCandidate> ranges = new java.util.LinkedHashMap<>();
    if (filter != null) {
      collect(filter, table.schema(), candidates, ranges);
    }
    List<ContainsHint> resolved = new ArrayList<>(hints.size());
    for (ContainsHint hint : hints) {
      resolved.add(resolveColumn(table, hint));
    }
    for (ContainsHint hint : resolved) {
      if (hint.kind() == HintKind.CONTAINS) {
        candidates.add(new Candidate(hint.column(), List.of(hint.value()), NgramBloom.Mode.CONTAINS));
      }
    }
    IndexType.Probe probe =
        new IndexType.Probe(
            table, List.copyOf(candidates), List.copyOf(ranges.values()), List.copyOf(resolved),
            config, new IndexType.GramProbes());
    IndexType.ReadContext read =
        new IndexType.ReadContext(table, config, metrics, store, gramIndex, termIndex);

    List<String> paths = new ArrayList<>(tasks.size());
    for (FileScanTask task : tasks) {
      paths.add(task.file().location());
    }
    IndexType.FileSet files = IndexType.FileSet.of(paths);

    org.roaringbitmap.RoaringBitmap kept = files.inPlay();
    for (IndexType type : IndexTypes.inCostOrder()) {
      // Counted per tier, not just once per plan: which tier did the pruning, and whether a second
      // predicate narrowed anything at all, is otherwise unanswerable from a running process.
      int before = kept.getCardinality();
      kept = partitionWith(type, read, probe, files.narrowedTo(kept)).kept();
      metrics.prunePass(type.key(), before, kept.getCardinality());
    }

    // Where the seam's three verdicts collapse into this caller's two. Planning is the one
    // consumer for which "the index matched it" and "the index has never heard of it" call for
    // the same action, so it takes kept = hits + unknown and asks no more.
    List<FileScanTask> survivors = new ArrayList<>(kept.getCardinality());
    for (int ordinal : kept) {
      survivors.add(tasks.get(ordinal));
    }
    if (survivors.size() < tasks.size()) {
      LOG.info(
          "index pruned {} of {} file-scan-tasks", tasks.size() - survivors.size(), tasks.size());
    }
    return survivors;
  }

  /**
   * One type's turn. A type that cannot be loaded or that fails while answering is treated as
   * absent, and absence is unknown for every file -- the same rule each reader applies to a leaf
   * it refuses, and the one the three-way answer now states rather than implies.
   */
  private static IndexType.Partition partitionWith(
      IndexType type, IndexType.ReadContext read, IndexType.Probe probe, IndexType.FileSet files) {
    try {
      IndexType.Loaded loaded = type.load(read);
      if (loaded == null) {
        return IndexType.Partition.allUnknown(files);
      }
      IndexType.Partition answer = type.partition(loaded, probe, files);
      return answer == null ? IndexType.Partition.allUnknown(files) : answer;
    } catch (RuntimeException e) {
      LOG.warn("index type '{}' failed; keeping every file", type.key(), e);
      return IndexType.Partition.allUnknown(files);
    }
  }

  /**
   * ID references (the spec form) resolve to a column name here, where the schema is at hand. The
   * full column path is used (findColumnName), so a nested field's reference resolves to
   * "attrs.msg" rather than the leaf name "msg" of a different indexed column. Legacy name-form
   * hints pass through as-is.
   */
  private static ContainsHint resolveColumn(Table table, ContainsHint hint) {
    if (hint.fieldId() < 0) {
      return hint;
    }
    String column = table.schema().findColumnName(hint.fieldId());
    if (column == null) {
      throw new IllegalArgumentException("unknown field id in text predicate: " + hint.fieldId());
    }
    return new ContainsHint(column, hint.fieldId(), hint.value(), hint.kind());
  }

  /**
   * The gram layer for one candidate, or null when it cannot serve it (disabled, unknown column,
   * no layer). IN unions per-literal match bitmaps, mirroring the bloom's any-literal rule.
   */
  public static GramProbe gramProbe(GramIndex gramIndex, Table table, Candidate candidate) {
    if (gramIndex == null) {
      return null;
    }
    var field = table.schema().findField(candidate.column());
    if (field == null) {
      return null;
    }
    GramIndex.Loaded loaded = gramIndex.forField(table, field.fieldId());
    if (loaded == null) {
      return null;
    }
    org.roaringbitmap.RoaringBitmap union = null;
    for (String literal : candidate.literals()) {
      org.roaringbitmap.RoaringBitmap match = loaded.matches(literal, candidate.mode());
      if (match == null) {
        return new GramProbe(loaded, null);
      }
      union = union == null ? match : org.roaringbitmap.RoaringBitmap.or(union, match);
    }
    return new GramProbe(loaded, union);
  }

  // package-private so the repeated-path guard can be tested on its own
  static void collect(
      Expression expr, org.apache.iceberg.Schema schema, List<Candidate> out,
      Map<String, RangeCandidate> ranges) {
    if (expr instanceof And and) {
      collect(and.left(), schema, out, ranges);
      collect(and.right(), schema, out, ranges);
      return;
    }
    if (expr instanceof UnboundPredicate<?> comparison
        && comparison.term() instanceof NamedReference<?> ref
        && comparison.literals() != null
        && comparison.literals().size() == 1) {
      org.apache.iceberg.types.Types.NestedField field = schema.findField(ref.name());
      boolean rangeOp =
          switch (comparison.op()) {
            case GT, GT_EQ, LT, LT_EQ -> true;
            default -> false;
          };
      if (rangeOp && field != null
          && field.type().typeId() == org.apache.iceberg.types.Type.TypeID.STRING
          && !IcebergKinds.repeatedPath(schema, field.fieldId())) {
        // literals().get(0), never literal(): an unbound IN with one value -- which is how Trino
        // pushes `col = 'x'` -- has a literal list but throws from literal(), so the operator has
        // to be inspected before the literal is.
        String value = comparison.literals().get(0).value().toString();
        RangeCandidate half =
            switch (comparison.op()) {
              case GT -> new RangeCandidate(ref.name(), value, false, null, false);
              case GT_EQ -> new RangeCandidate(ref.name(), value, true, null, false);
              case LT -> new RangeCandidate(ref.name(), null, false, value, false);
              default -> new RangeCandidate(ref.name(), null, false, value, true);
            };
        ranges.merge(ref.name(), half, RangeCandidate::tighten);
        return;
      }
    }
    if (expr instanceof UnboundPredicate<?> predicate
        && predicate.term() instanceof NamedReference<?> ref) {
      NgramBloom.Mode mode =
          switch (predicate.op()) {
            case EQ, IN -> NgramBloom.Mode.EQ;
            case STARTS_WITH -> NgramBloom.Mode.STARTS_WITH;
            default -> null;
          };
      if (mode == null) {
        return;
      }
      // Canonical form, the same one the build wrote (Canonical): a bigint's decimal text, a
      // UUID's undashed hex, a binary's hex. A literal in a form this index does not write --
      // an unindexable type, unparseable text -- makes the predicate not ours: keep every file.
      org.apache.iceberg.types.Types.NestedField field = schema.findField(ref.name());
      // A leaf under a list or map is refused on both paths: see IcebergKinds.repeatedPath.
      if (field == null || IcebergKinds.repeatedPath(schema, field.fieldId())) {
        return;
      }
      List<String> literals = new ArrayList<>();
      for (Literal<?> literal : predicate.literals()) {
        String form = Canonical.form(IcebergKinds.of(field.type()), literal.value());
        if (form == null) {
          return;
        }
        literals.add(form);
      }
      if (!literals.isEmpty()) {
        out.add(new Candidate(ref.name(), literals, mode));
      }
    }
    // OR / NOT subtrees contribute no candidates
  }
}
