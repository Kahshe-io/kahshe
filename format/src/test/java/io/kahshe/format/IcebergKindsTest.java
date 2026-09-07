package io.kahshe.format;

import static org.junit.jupiter.api.Assertions.assertEquals;

import io.kahshe.analysis.ValueKind;
import org.apache.iceberg.types.Types;
import org.junit.jupiter.api.Test;

/** The one place Iceberg's types meet {@link ValueKind}. */
class IcebergKindsTest {

  /**
   * Iceberg spells both {@code timestamp} and {@code timestamptz} with ONE type id, and only
   * {@code shouldAdjustToUTC} separates a wall clock from an instant. Collapsing them was a real
   * defect rather than a tidiness one: a window rule's confirmation SQL renders its bound
   * differently for the two, and the wrong spelling is read by the engine in the reader's session
   * zone — an alert that confirms in London and not in New York. Verified red by returning
   * TIMESTAMP for both.
   */
  @Test
  void aZoneCarryingTimestampIsNotTheSameKindAsAWallClock() {
    assertEquals(ValueKind.TIMESTAMP, IcebergKinds.of(Types.TimestampType.withoutZone()));
    assertEquals(ValueKind.TIMESTAMPTZ, IcebergKinds.of(Types.TimestampType.withZone()));
  }

  @Test
  void theRestOfTheVocabulary() {
    assertEquals(ValueKind.STRING, IcebergKinds.of(Types.StringType.get()));
    assertEquals(ValueKind.INTEGRAL, IcebergKinds.of(Types.IntegerType.get()));
    assertEquals(ValueKind.INTEGRAL, IcebergKinds.of(Types.LongType.get()));
    assertEquals(ValueKind.DECIMAL, IcebergKinds.of(Types.DecimalType.of(9, 2)));
    assertEquals(ValueKind.FLOATING, IcebergKinds.of(Types.DoubleType.get()));
    assertEquals(ValueKind.UUID, IcebergKinds.of(Types.UUIDType.get()));
    assertEquals(ValueKind.BINARY, IcebergKinds.of(Types.BinaryType.get()));
    assertEquals(ValueKind.BINARY, IcebergKinds.of(Types.FixedType.ofLength(4)));
    assertEquals(ValueKind.BOOLEAN, IcebergKinds.of(Types.BooleanType.get()));
    assertEquals(ValueKind.DATE, IcebergKinds.of(Types.DateType.get()));
    // a type no canonical form is defined for, and the null a caller may hand over
    assertEquals(ValueKind.OTHER, IcebergKinds.of(Types.TimeType.get()));
    assertEquals(ValueKind.OTHER, IcebergKinds.of(null));
  }
}
