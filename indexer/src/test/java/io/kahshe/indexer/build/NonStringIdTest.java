package io.kahshe.indexer.build;

import io.kahshe.analysis.Canonical;
import io.kahshe.format.IndexPruner;
import io.kahshe.format.type.term.TermIndex;
import static org.junit.jupiter.api.Assertions.assertEquals;

import io.kahshe.common.Metrics;
import java.nio.ByteBuffer;
import java.nio.file.Path;
import java.util.List;
import java.util.Set;
import java.util.UUID;
import java.util.stream.Collectors;
import org.apache.iceberg.Schema;
import org.apache.iceberg.Table;
import org.apache.iceberg.data.GenericRecord;
import org.apache.iceberg.data.Record;
import org.apache.iceberg.expressions.Expression;
import org.apache.iceberg.expressions.Expressions;
import org.apache.iceberg.types.Types;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;
import io.kahshe.indexer.BuildConfig;
import io.kahshe.indexer.LocalTableFixture;

/**
 * Non-string id columns — bigint, uuid, binary, decimal — index by their canonical string form, and a plain
 * equality predicate on them (the no-plugin path: stock engines push {@code =} into the plan
 * filter) prunes through the same tiers. One form on both sides is the whole property: the build
 * writes {@code Canonical.form(value)} and the pruner probes {@code Canonical.form(literal)}, with
 * a UUID literal accepted as a UUID or as the dashed text a JSON plan filter carries.
 *
 * <p>Verified red with the pruner's literal collection reverted to strings-only: every non-string
 * predicate is ignored and both files are kept.
 */
class NonStringIdTest {
  @TempDir Path tmp;

  private static final UUID U1 = UUID.fromString("11111111-2222-3333-4444-555555555555");
  private static final UUID U2 = UUID.fromString("aaaaaaaa-bbbb-cccc-dddd-eeeeeeeeeeee");

  private static final Schema SCHEMA =
      new Schema(
          Types.NestedField.required(1, LocalTableFixture.COLUMN, Types.StringType.get()),
          Types.NestedField.optional(2, "id", Types.LongType.get()),
          Types.NestedField.optional(3, "uid", Types.UUIDType.get()),
          Types.NestedField.optional(4, "blob", Types.BinaryType.get()),
          Types.NestedField.optional(5, "dec", Types.DecimalType.of(12, 2)));

  private static Record row(Schema schema, long id, UUID uid, byte[] blob, String dec) {
    GenericRecord r = GenericRecord.create(schema);
    r.setField(LocalTableFixture.COLUMN, "x");
    r.setField("id", id);
    r.setField("uid", uid);
    r.setField("blob", ByteBuffer.wrap(blob));
    r.setField("dec", new java.math.BigDecimal(dec));
    return r;
  }

  private static Set<String> kept(Table table, BuildConfig config, Expression filter)
      throws Exception {
    Metrics metrics = new Metrics();
    IndexPruner pruner = new IndexPruner(new TermIndex(config.format(), metrics), metrics, config.format());
    return pruner.prune(table, filter, List.of(), LocalTableFixture.planTasks(table)).stream()
        .map(t -> t.file().location())
        .collect(Collectors.toSet());
  }

  @Test
  void equalityOnBigintUuidAndBinaryColumnsPrunesByCanonicalForm() throws Exception {
    BuildConfig config = LocalTableFixture.config();
    Table table = LocalTableFixture.createTable(tmp, SCHEMA, "seed");
    String f1 =
        LocalTableFixture.appendRecords(
            table, "f1.parquet", row(table.schema(), 42L, U1, new byte[] {(byte) 0xde, (byte) 0xad}, "12.50"));
    String f2 =
        LocalTableFixture.appendRecords(
            table, "f2.parquet", row(table.schema(), 7L, U2, new byte[] {1, 2}, "7.00"));
    table.refresh();
    for (String column : new String[] {"id", "uid", "blob", "dec"}) {
      IndexBuilder.buildColumn(table, column, config);
    }

    assertEquals(Set.of(f1), kept(table, config, Expressions.equal("id", 42L)), "bigint");
    assertEquals(Set.of(f2), kept(table, config, Expressions.equal("uid", U2)), "uuid literal");
    assertEquals(
        Set.of(f2),
        kept(table, config, Expressions.equal("uid", U2.toString())),
        "uuid as the dashed text a JSON plan filter carries");
    assertEquals(
        Set.of(f2),
        kept(table, config, Expressions.equal("blob", ByteBuffer.wrap(new byte[] {1, 2}))),
        "binary");
    assertEquals(
        Set.of(f1), kept(table, config, Expressions.in("id", 42L, 99L)), "IN unions per literal");
    assertEquals(
        Set.of(f1),
        kept(table, config, Expressions.equal("dec", new java.math.BigDecimal("12.50"))),
        "decimal at the column's scale");
    assertEquals(
        Set.of(f2),
        kept(table, config, Expressions.equal("dec", "7")),
        "decimal as a literal typed without the scale's zeros");
  }
}
