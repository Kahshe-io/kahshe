package io.kahshe.watch.scan;

import static org.apache.iceberg.types.Types.NestedField.optional;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

import io.kahshe.analysis.ValueKind;
import io.kahshe.watch.rules.WatchRule;
import java.util.ArrayList;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Set;
import org.apache.iceberg.Schema;
import org.apache.iceberg.data.GenericRecord;
import org.apache.iceberg.data.Record;
import org.apache.iceberg.types.Types;
import org.junit.jupiter.api.Test;

/**
 * A rule reads a list or a map member by member, never as Java's rendering of the collection.
 *
 * <p>Through {@code String.valueOf} a list is the text {@code [alpha, bravo]}, and every operator
 * then compares against that: {@code contains 'alpha, bravo'} matches though no value contains
 * it, {@code equals 'alpha'} misses though a member is exactly that — both wrong, neither visible
 * in a log.
 *
 * <p>The semantics are the index's: an operator is applied to each member and the field is
 * satisfied if ANY member satisfies it (FORMAT.md §6.7). The quantifier is per FIELD, which is
 * exactly why {@link #twoConditionsOnOneContainerColumnAreRefusedRatherThanAnsweredWrongly}
 * exists.
 */
class RepeatedColumnRowTest {

  private static final Schema SCHEMA = new Schema(
      optional(1, "msg", Types.StringType.get()),
      optional(2, "tags", Types.ListType.ofOptional(3, Types.StringType.get())),
      optional(4, "props", Types.MapType.ofOptional(
          5, 6, Types.StringType.get(), Types.StringType.get())));

  private static final Map<String, ValueKind> KINDS = kinds();
  private static final Set<String> REPEATED = Set.of("tags", "props");

  @Test
  void aListIsReadMemberByMemberAndNeverAsItsJavaRendering() {
    Row row = rowOf("m", List.of("alpha", "bravo"), null);

    assertEquals(2, row.arity("tags"));
    assertEquals("alpha", row.canonical("tags", 0));
    assertEquals("bravo", row.canonical("tags", 1));

    assertTrue(matches(field("tags", WatchRule.Op.CONTAINS, "alpha"), row));
    assertTrue(matches(field("tags", WatchRule.Op.EQUALS, "bravo"), row),
        "a member equals the literal; the rendering of the whole list never does");
    assertFalse(matches(field("tags", WatchRule.Op.CONTAINS, "alpha, bravo"), row),
        "the separator is Java's, not the data's: no value in this row contains it");
    assertFalse(matches(field("tags", WatchRule.Op.EQUALS, "[alpha, bravo]"), row));
    assertTrue(matches(field("tags", WatchRule.Op.MATCH, "bravo"), row),
        "tokens are cut per member too");
  }

  @Test
  void aMapIsReadThroughItsValuesTheSameHalfTheIndexWrites() {
    Map<String, String> props = new LinkedHashMap<>();
    props.put("env", "production");
    Row row = rowOf("m", null, props);

    assertEquals(1, row.arity("props"));
    assertTrue(matches(field("props", WatchRule.Op.EQUALS, "production"), row));
    assertFalse(matches(field("props", WatchRule.Op.EQUALS, "env"), row),
        "a KEY is not a value here, exactly as it is not a term in the index");
  }

  @Test
  void aNullOrEmptyContainerSatisfiesNothingAndDoesNotThrow() {
    assertEquals(0, rowOf("m", null, null).arity("tags"));
    assertEquals(0, rowOf("m", List.of(), null).arity("tags"));
    assertFalse(matches(field("tags", WatchRule.Op.CONTAINS, "alpha"), rowOf("m", null, null)));
    assertFalse(matches(field("tags", WatchRule.Op.CONTAINS, ""), rowOf("m", List.of(), null)));
  }

  /**
   * The reader hands back ONE collection instance per column and clears it for the next row, so a
   * row that did not copy would answer the following row's members — and a scanner holding a value
   * across rows would too.
   */
  @Test
  void membersAreCopiedOutOfTheReadersOwnCollection() {
    ProjectedRow row = new ProjectedRow(SCHEMA);
    List<Object> reused = new ArrayList<>(List.of("alpha", "bravo"));
    row.read(record("m", reused, null));
    Object held = row.value("tags");

    reused.clear();
    reused.add("zulu");

    assertEquals(List.of("alpha", "bravo"), held,
        "what a scanner kept must not change when the reader reuses its collection");
    assertEquals("alpha", row.canonical("tags", 0));
  }

  /**
   * The quantifier is per field: two fields ask ∃m:p ∧ ∃m:q, while the rule reads — and its
   * confirmation SQL would claim — ∃m:(p ∧ q). They differ exactly when the literals match
   * different members, so the rule is refused rather than answered wrongly.
   */
  @Test
  void twoConditionsOnOneContainerColumnAreRefusedRatherThanAnsweredWrongly() {
    WatchRule crossMember = new WatchRule("r-cross", "t", WatchRule.Severity.LOW, "p",
        "logs.events",
        List.of(new WatchRule.Field("tags", WatchRule.Op.CONTAINS, List.of("powershell")),
            new WatchRule.Field("tags", WatchRule.Op.CONTAINS, List.of("-enc"))),
        WatchRule.Condition.ALL_OF, 1);

    PreparedRule.UnsupportedRule e = assertThrows(PreparedRule.UnsupportedRule.class,
        () -> PreparedRule.of(crossMember, KINDS, REPEATED));
    assertTrue(e.getMessage().contains("tags"), e.getMessage());

    // The row it would have fired on: neither member is the command the rule describes.
    Row row = rowOf("m", List.of("/bin/sh -c echo powershell", "notepad.exe -enc AAA"), null);
    assertTrue(matches(field("tags", WatchRule.Op.CONTAINS, "powershell"), row));
    assertTrue(matches(field("tags", WatchRule.Op.CONTAINS, "-enc"), row));

    // Two fields on a SCALAR column are unaffected: one value, so the two quantifiers agree.
    WatchRule scalar = new WatchRule("r-scalar", "t", WatchRule.Severity.LOW, "p", "logs.events",
        List.of(new WatchRule.Field("msg", WatchRule.Op.CONTAINS, List.of("a")),
            new WatchRule.Field("msg", WatchRule.Op.CONTAINS, List.of("b"))),
        WatchRule.Condition.ALL_OF, 1);
    PreparedRule.of(scalar, KINDS, REPEATED);

    // And one field carrying both values is the supported spelling the message points at.
    PreparedRule.of(new WatchRule("r-one", "t", WatchRule.Severity.LOW, "p", "logs.events",
        List.of(new WatchRule.Field("tags", WatchRule.Op.CONTAINS, List.of("powershell", "-enc"))),
        WatchRule.Condition.ANY_OF, 1), KINDS, REPEATED);
  }

  @Test
  void aContainersKindIsItsMembersKindRatherThanUnindexable() {
    assertEquals(ValueKind.STRING, rowOf("m", List.of("x"), null).kind("tags"),
        "OTHER would make every operator compare as text and never as the member's own type");
  }

  private static boolean matches(WatchRule.Field field, Row row) {
    WatchRule rule = new WatchRule("r", "t", WatchRule.Severity.LOW, "p", "logs.events",
        List.of(field), WatchRule.Condition.ANY_OF, 1);
    return PreparedRule.of(rule, KINDS, REPEATED).matches(row, new boolean[1]);
  }

  private static WatchRule.Field field(String column, WatchRule.Op op, String value) {
    return new WatchRule.Field(column, op, List.of(value));
  }

  private static Row rowOf(String msg, Object tags, Object props) {
    ProjectedRow row = new ProjectedRow(SCHEMA);
    row.read(record(msg, tags, props));
    return row;
  }

  private static Record record(String msg, Object tags, Object props) {
    Record record = GenericRecord.create(SCHEMA);
    record.setField("msg", msg);
    record.setField("tags", tags);
    record.setField("props", props);
    return record;
  }

  private static Map<String, ValueKind> kinds() {
    Map<String, ValueKind> kinds = new LinkedHashMap<>();
    kinds.put("msg", ValueKind.STRING);
    kinds.put("tags", ValueKind.STRING);
    kinds.put("props", ValueKind.STRING);
    return Map.copyOf(kinds);
  }
}
