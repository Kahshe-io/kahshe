package io.kahshe.indexer.build;

import io.kahshe.format.IndexPruner;
import io.kahshe.format.type.term.TermIndex;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

import io.kahshe.common.Metrics;
import java.nio.file.Path;
import java.util.List;
import java.util.Set;
import java.util.stream.Collectors;
import org.apache.iceberg.FileScanTask;
import org.apache.iceberg.Schema;
import org.apache.iceberg.Table;
import org.apache.iceberg.data.GenericRecord;
import org.apache.iceberg.data.Record;
import org.apache.iceberg.types.Types;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;
import io.kahshe.indexer.BuildConfig;
import io.kahshe.indexer.LocalTableFixture;

/**
 * Nested STRUCT columns index by dotted path, keyed by the leaf's field id; a path through a
 * list or map is refused, loudly and by identity.
 *
 * <p>The load-bearing property is the first test's: the index for {@code attrs.msg} must be built
 * from the VALUES REACHED BY THAT PATH and consulted only for that field id. The schema declares a
 * same-named top-level string beside the nested one, and every file's top-level value is a decoy
 * holding a token the nested value lacks. Read the wrong column and the decoys prune the wrong
 * files; read nothing — the single-level {@code getField} answers null for a path it does not
 * know — and every value is null, the index covers every file while holding no tokens, and a
 * {@code match} prunes them all (the forbidden class). Verified red both ways: with the accessor
 * replaced by {@code getField} the first two tests fail on the file that should have been kept,
 * and with the list/map refusal removed only the refusal test fails.
 */
class NestedColumnTest {
  @TempDir Path tmp;

  private static final String NESTED = "attrs." + LocalTableFixture.COLUMN;

  private static final Schema SCHEMA =
      new Schema(
          Types.NestedField.required(1, LocalTableFixture.COLUMN, Types.StringType.get()),
          Types.NestedField.optional(
              2,
              "attrs",
              Types.StructType.of(
                  Types.NestedField.optional(3, LocalTableFixture.COLUMN, Types.StringType.get()))),
          Types.NestedField.optional(4, "n", Types.LongType.get()),
          Types.NestedField.optional(
              5, "tags", Types.ListType.ofOptional(6, Types.StringType.get())),
          Types.NestedField.optional(
              7,
              "props",
              Types.MapType.ofOptional(8, 9, Types.StringType.get(), Types.StringType.get())));

  /** One row: the top-level column holds {@code top}; {@code attrs.msg} holds {@code nested}, or the struct is null. */
  private static Record row(Schema schema, String top, String nested) {
    GenericRecord record = GenericRecord.create(schema);
    record.setField(LocalTableFixture.COLUMN, top);
    if (nested != null) {
      GenericRecord attrs = GenericRecord.create(schema.findField("attrs").type().asStructType());
      attrs.setField(LocalTableFixture.COLUMN, nested);
      record.setField("attrs", attrs);
    }
    return record;
  }

  private static Set<String> keptPaths(
      Table table, BuildConfig config, IndexPruner.HintKind kind, String value)
      throws Exception {
    Metrics metrics = new Metrics();
    IndexPruner pruner = new IndexPruner(new TermIndex(config.format(), metrics), metrics, config.format());
    List<FileScanTask> kept =
        pruner.prune(
            table,
            null,
            List.of(new IndexPruner.ContainsHint(NESTED, value, kind)),
            LocalTableFixture.planTasks(table));
    return kept.stream().map(t -> t.file().location()).collect(Collectors.toSet());
  }

  @Test
  void aStructLeafIsIndexedByItsOwnPathNotByTheSameNamedTopLevelColumn() throws Exception {
    BuildConfig config = LocalTableFixture.config();
    // f1 (from createTable) has a top-level value and a NULL struct: covered, no nested tokens.
    Table table = LocalTableFixture.createTable(tmp, SCHEMA, "seed");
    Schema schema = table.schema();
    // Each top-level value is a decoy holding the token the OTHER file's nested value has.
    String f2 =
        LocalTableFixture.appendRecords(
            table, "f2.parquet", row(schema, "charlie decoy", "alpha bravo"));
    String f3 =
        LocalTableFixture.appendRecords(
            table, "f3.parquet", row(schema, "alpha decoy", "charlie delta"));
    table.refresh();
    IndexBuilder.buildColumn(table, NESTED, config);

    assertEquals(
        Set.of(f3),
        keptPaths(table, config, IndexPruner.HintKind.MATCH, "charlie"),
        "match on the nested path keeps the file whose NESTED value holds the token -- not f2, "
            + "whose top-level decoy holds it, and not the null-struct file");
    assertEquals(
        Set.of(f2), keptPaths(table, config, IndexPruner.HintKind.MATCH, "alpha"));
    assertEquals(
        Set.of(f2),
        keptPaths(table, config, IndexPruner.HintKind.CONTAINS, "lpha"),
        "the gram and bloom tiers are built from the path too");
  }

  @Test
  void aNullStructRowIsSkippedAndTheValuedRowBesideItIsStillFound() throws Exception {
    BuildConfig config = LocalTableFixture.config();
    Table table = LocalTableFixture.createTable(tmp, SCHEMA, "seed");
    Schema schema = table.schema();
    // One file mixing a null struct and a valued one: the accessor must answer null for the
    // first row rather than throw, and index the second.
    String f2 =
        LocalTableFixture.appendRecords(
            table, "f2.parquet", row(schema, "x", null), row(schema, "y", "echo foxtrot"));
    table.refresh();
    IndexBuilder.buildColumn(table, NESTED, config);

    assertEquals(Set.of(f2), keptPaths(table, config, IndexPruner.HintKind.MATCH, "echo"));
    assertTrue(
        keptPaths(table, config, IndexPruner.HintKind.MATCH, "zulu").isEmpty(),
        "a token in neither file prunes both: the null-struct file is covered and holds nothing");
  }

  @Test
  void aPathThroughAListOrMapIsRefusedByIdentity() throws Exception {
    Table table = LocalTableFixture.createTable(tmp, SCHEMA, "seed");
    for (String column : new String[] {"tags.element", "props.value"}) {
      IllegalArgumentException e =
          assertThrows(
              IllegalArgumentException.class,
              () -> IndexBuilder.buildColumn(table, column, LocalTableFixture.config()),
              column + " is a string reached through a repeated field");
      assertTrue(
          e.getMessage().contains("repeated fields (list or map) are not supported yet"),
          "refused by the repeated-field guard, not some other IllegalArgumentException: "
              + e.getMessage());
    }
  }

  @Test
  void aMissingOrWholeStructColumnIsRefused() throws Exception {
    // `n` (bigint) is deliberately NOT in this list: non-string ids index by canonical form
    // (NonStringIdTest). A whole struct and a missing column stay refused.
    Table table = LocalTableFixture.createTable(tmp, SCHEMA, "seed");
    for (String column : new String[] {"no_such_column", "attrs"}) {
      IllegalArgumentException e =
          assertThrows(
              IllegalArgumentException.class,
              () -> IndexBuilder.buildColumn(table, column, LocalTableFixture.config()));
      assertTrue(
          e.getMessage().contains("must exist and be a string"),
          column + " must be refused by the schema guard: " + e.getMessage());
    }
  }
}
