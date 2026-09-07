package io.kahshe.format;

import io.kahshe.analysis.ValueKind;
import org.apache.iceberg.Schema;
import org.apache.iceberg.types.Type;
import org.apache.iceberg.types.TypeUtil;
import org.apache.iceberg.types.Types;

/**
 * The binding from Iceberg's type system to {@link ValueKind}, which is the whole of what
 * analysis needs to know about a column.
 *
 * <p>One switch, and the only place Iceberg's types meet the canonical forms. Keeping it to one
 * switch is deliberate: a second table format brings its own such function and reuses everything
 * else.
 */
public final class IcebergKinds {
  private IcebergKinds() {}

  /** The kind of an Iceberg column type; {@link ValueKind#OTHER} for one no form is defined for. */
  public static ValueKind of(Type type) {
    if (type == null) {
      return ValueKind.OTHER;
    }
    return switch (type.typeId()) {
      case STRING -> ValueKind.STRING;
      case INTEGER, LONG -> ValueKind.INTEGRAL;
      case DECIMAL -> ValueKind.DECIMAL;
      case FLOAT, DOUBLE -> ValueKind.FLOATING;
      case UUID -> ValueKind.UUID;
      // Fixed-width and variable-width bytes share a canonical form, so they share a kind.
      case BINARY, FIXED -> ValueKind.BINARY;
      case BOOLEAN -> ValueKind.BOOLEAN;
      case DATE -> ValueKind.DATE;
      // Iceberg's one type id covers both spellings; shouldAdjustToUTC is what separates
      // an instant (timestamptz) from a wall clock (timestamp), and a window bound rendered
      // for the wrong one selects a different window on the reader's engine.
      case TIMESTAMP -> type instanceof Types.TimestampType ts && ts.shouldAdjustToUTC()
          ? ValueKind.TIMESTAMPTZ
          : ValueKind.TIMESTAMP;
      default -> ValueKind.OTHER;
    };
  }

  /**
   * Whether this field is reached through a list or a map, rather than through structs alone.
   *
   * <p>Such a field is many values per row, and every tier stores one bitmap of file ordinals per
   * term, so a set bit proves only that some row has some element emitting the term — an
   * existential over one atomic comparison. A range tightens two halves of a conjunction into one
   * contiguous run and asks whether a single element falls in it; a row whose elements are
   * {@code ['z','A']} satisfies {@code >= 'a' AND < 'b'} through two different elements and would
   * be pruned. The build refuses to write such a leaf and the pruner refuses to read one; the
   * pruner needs this test because Iceberg hands it an ordinary {@code NestedField}:
   * {@code tags.element} answers STRING, indistinguishable from a top-level string.
   *
   * <p>One schema walk per call, nothing beside the artifact reads a prune goes on to do.
   */
  public static boolean repeatedPath(Schema schema, int fieldId) {
    java.util.Map<Integer, Integer> parents = TypeUtil.indexParents(schema.asStruct());
    for (Integer parent = parents.get(fieldId); parent != null; parent = parents.get(parent)) {
      if (!schema.findType(parent).isStructType()) {
        return true;
      }
    }
    return false;
  }

  /** How many values of a column one row carries. */
  public enum Repetition {
    /** One, reached through structs alone. */
    SCALAR,
    /** Many: a list's elements. */
    LIST,
    /** Many: a map's values. Keys are not indexed -- see FORMAT.md. */
    MAP
  }

  /**
   * A column's indexing shape: how many values a row carries, and the kind of the values that are
   * actually analyzed.
   *
   * <p>Deliberately NOT a twelfth {@link ValueKind}. Repetition is orthogonal to canonical form --
   * a {@code list<long>}'s elements are canonicalised exactly as a {@code long} is -- so folding it
   * into the kind would make every switch over the eleven a change site and would still leave the
   * element's own kind to carry separately.
   */
  public record Shape(Repetition repetition, ValueKind kind) {}

  /**
   * The shape of an Iceberg column type: the container it is, and the kind of what it holds.
   *
   * <p>A container of anything without a canonical form -- {@code list<struct>}, {@code list<list>},
   * a map to a struct -- reports {@link ValueKind#OTHER}, so the one admission test callers already
   * apply ({@code Canonical.indexable}) refuses it without knowing containers exist.
   */
  public static Shape shapeOf(Type type) {
    if (type instanceof Types.ListType list) {
      return new Shape(Repetition.LIST, of(list.elementType()));
    }
    if (type instanceof Types.MapType map) {
      return new Shape(Repetition.MAP, of(map.valueType()));
    }
    return new Shape(Repetition.SCALAR, of(type));
  }
}
