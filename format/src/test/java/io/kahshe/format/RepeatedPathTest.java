package io.kahshe.format;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.apache.iceberg.types.Types.NestedField.optional;

import org.apache.iceberg.Schema;
import org.apache.iceberg.expressions.Expressions;
import org.apache.iceberg.types.Types;
import org.junit.jupiter.api.Test;

/**
 * A leaf under a list or map yields no pruning candidate on either of
 * {@code IndexPruner.collect}'s branches. {@link IcebergKinds#repeatedPath} says why a
 * file-ordinal bitmap cannot answer for such a leaf; what this pins is that the guard sits in the
 * pruner, because {@code schema.findField("tags.element")} answers a STRING the type system cannot
 * tell from a top-level one.
 */
class RepeatedPathTest {

  private static final Schema SCHEMA = new Schema(
      optional(1, "msg", Types.StringType.get()),
      optional(2, "tags", Types.ListType.ofOptional(3, Types.StringType.get())),
      optional(4, "props", Types.MapType.ofOptional(
          5, 6, Types.StringType.get(), Types.StringType.get())),
      optional(7, "attrs", Types.StructType.of(optional(8, "msg", Types.StringType.get()))));

  @Test
  void aLeafUnderAListOrMapIsRecognisedAsRepeatedAndAStructPathIsNot() {
    assertFalse(IcebergKinds.repeatedPath(SCHEMA, SCHEMA.findField("msg").fieldId()),
        "a top-level column is not repeated");
    assertFalse(IcebergKinds.repeatedPath(SCHEMA, SCHEMA.findField("attrs.msg").fieldId()),
        "a struct path is one value per row: that is why it is already indexable");
    assertTrue(IcebergKinds.repeatedPath(SCHEMA, SCHEMA.findField("tags.element").fieldId()),
        "a list element is many values per row");
    assertTrue(IcebergKinds.repeatedPath(SCHEMA, SCHEMA.findField("props.key").fieldId()));
    assertTrue(IcebergKinds.repeatedPath(SCHEMA, SCHEMA.findField("props.value").fieldId()));
  }

  @Test
  void anEqualityOnAListElementYieldsNoCandidate() {
    assertEquals(0, candidates(Expressions.equal("tags.element", "x")),
        "a point probe on a list element would prune every covered file that does not hold the "
            + "term, which is sound only if the term set is complete per element -- and the guard "
            + "is what makes that not the pruner's problem");
    assertEquals(0, candidates(Expressions.equal("props.value", "x")));
    assertEquals(1, candidates(Expressions.equal("msg", "x")), "the ordinary case still prunes");
    assertEquals(1, candidates(Expressions.equal("attrs.msg", "x")), "and so does a struct path");
  }

  @Test
  void aRangeOnAListElementYieldsNoCandidateEitherHalf() {
    // The decisive one: two halves of a conjunction are tightened into a single contiguous run,
    // so a row satisfying them through two different elements is pruned away.
    assertEquals(0, candidates(Expressions.and(
        Expressions.greaterThanOrEqual("tags.element", "a"),
        Expressions.lessThan("tags.element", "b"))));
    assertEquals(1, candidates(Expressions.and(
        Expressions.greaterThanOrEqual("msg", "a"),
        Expressions.lessThan("msg", "b"))), "the same shape on a scalar still prunes");
  }

  /** How many pruning candidates the pruner would derive from this expression, both kinds. */
  private static int candidates(org.apache.iceberg.expressions.Expression expr) {
    java.util.List<IndexPruner.Candidate> out = new java.util.ArrayList<>();
    java.util.Map<String, IndexPruner.RangeCandidate> ranges = new java.util.HashMap<>();
    IndexPruner.collect(expr, SCHEMA, out, ranges);
    return out.size() + ranges.size();
  }
}
