package io.kahshe.indexer.build;

import io.kahshe.format.IndexPruner;
import io.kahshe.format.type.term.TermIndex;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertNull;

import io.kahshe.common.Metrics;
import java.nio.file.Path;
import java.util.List;
import java.util.Set;
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
 * A prefix is a range of the sorted term dictionary: every term under it, joined by OR over the
 * file bitmaps, with no new tokens and no reindex. Stock SQL reaches it as {@code LIKE 'x%'} on a
 * whole-value column (Iceberg {@code STARTS_WITH}); text columns through a {@code match_prefix}
 * hint over their terms, compounds included under v3. A run longer than the cap keeps every file.
 *
 * <p>Verified red three times: with the per-row prefix check removed, the coarse row-group read
 * returns the neighbouring address and its file is kept ({@code 71.162.} keeps {@code 71.5.0.1});
 * with the cap check removed, the capped query prunes instead of keeping every file; and with the
 * {@code match_prefix} hint unrecognised by the extractor, no hint reaches the pruner.
 */
class PrefixSearchTest {
  @TempDir Path tmp;

  private static final Schema SCHEMA =
      new Schema(
          Types.NestedField.required(1, LocalTableFixture.COLUMN, Types.StringType.get()),
          Types.NestedField.optional(2, "ip", Types.StringType.get()));

  private static Record row(Schema schema, String msg, String ip) {
    GenericRecord r = GenericRecord.create(schema);
    r.setField(LocalTableFixture.COLUMN, msg);
    r.setField("ip", ip);
    return r;
  }

  private static Set<String> kept(Table table, BuildConfig config, Expression filter,
      List<IndexPruner.ContainsHint> hints) throws Exception {
    Metrics metrics = new Metrics();
    IndexPruner pruner = new IndexPruner(new TermIndex(config.format(), metrics), metrics, config.format());
    return pruner.prune(table, filter, hints, LocalTableFixture.planTasks(table)).stream()
        .map(t -> t.file().location())
        .collect(Collectors.toSet());
  }

  private static IndexPruner.ContainsHint prefix(String column, String value) {
    return new IndexPruner.ContainsHint(column, value, IndexPruner.HintKind.PREFIX);
  }

  @Test
  void aPrefixIsARangeOfTheDictionaryOnBothKindsOfColumn() throws Exception {
    BuildConfig config = LocalTableFixture.config();
    Table table = LocalTableFixture.createTable(tmp, SCHEMA, "seed");
    String seed = LocalTableFixture.planTasks(table).get(0).file().location(); // f1.parquet
    String f1 = LocalTableFixture.appendRecords(table, "f2.parquet", row(table.schema(), "peer 71.162.18.0 ok", "71.162.18.0"));
    String f2 = LocalTableFixture.appendRecords(table, "f3.parquet", row(table.schema(), "peer 71.162.200.5", "71.162.200.5"));
    String f3 = LocalTableFixture.appendRecords(table, "f4.parquet", row(table.schema(), "x 71.5.0.1", "71.5.0.1"));
    String f4 = LocalTableFixture.appendRecords(table, "f5.parquet", row(table.schema(), "y 10.0.0.1", "10.0.0.1"));
    table.updateProperties().set("kahshe.index.ip.analyzer", "value").commit();
    table.refresh();
    IndexBuilder.buildColumn(table, "ip", config);
    IndexBuilder.buildColumn(table, LocalTableFixture.COLUMN, config);
    Set<String> all = Set.of(seed, f1, f2, f3, f4);

    assertEquals(Set.of(f1, f2), kept(table, config, Expressions.startsWith("ip", "71.162."), List.of()),
        "a /16 as a string prefix: both addresses under it, not the neighbour 71.5.0.1");
    assertEquals(Set.of(f1, f2, f3), kept(table, config, Expressions.startsWith("ip", "71."), List.of()));
    assertEquals(Set.of(f1), kept(table, config, Expressions.startsWith("ip", "71.162.18"), List.of()));
    assertEquals(Set.of(), kept(table, config, Expressions.startsWith("ip", "9"), List.of()), "nothing under the prefix");
    assertEquals(all, kept(table, config, Expressions.startsWith("ip", ""), List.of()), "the empty prefix is not a prefix query");

    String c = LocalTableFixture.COLUMN;
    assertEquals(Set.of(f1, f2), kept(table, config, null, List.of(prefix(c, "71.162."))),
        "a text column: the v3 compounds under the prefix");
    assertEquals(Set.of(f1, f2), kept(table, config, null, List.of(prefix(c, "PEE"))), "lowercased, over plain tokens");
    assertEquals(all, kept(table, config, null, List.of(prefix(c, "peer="))),
        "'=' starts no term under a tokens contract: nothing probed, every file kept");

    int fieldId = table.schema().findField("ip").fieldId();
    TermIndex termIndex = new TermIndex(config.format(), new Metrics());
    TermIndex.Loaded loaded = termIndex.forField(table, fieldId);
    TermIndex.PrefixEntries run = termIndex.entriesForPrefix(table, loaded, "71.", 10);
    assertNotNull(run);
    assertEquals(3, run.terms());
    assertEquals(3L, run.totalCount(), "one occurrence per address");
    assertNull(termIndex.entriesForPrefix(table, loaded, "71.", 2), "a run past the cap is refused");

    // Trino pushes LIKE 'x%' down as a range, not as STARTS_WITH; any string range on a whole-value
    // column is the same run of the dictionary. Verified red with the range routing removed: the
    // substring tiers cannot probe a comparison, so every file is kept.
    assertEquals(
        Set.of(f1),
        kept(table, config, Expressions.and(
            Expressions.greaterThanOrEqual("ip", "71.162.18"), Expressions.lessThan("ip", "71.162.19")), List.of()),
        "a prefix as Trino sends it: the two halves tightened into one run");
    assertEquals(Set.of(f3), kept(table, config, Expressions.greaterThanOrEqual("ip", "71.5"), List.of()), "open above");
    assertEquals(Set.of(f4), kept(table, config, Expressions.lessThan("ip", "2"), List.of()), "open below");
    assertEquals(Set.of(), kept(table, config, Expressions.and(
        Expressions.greaterThan("ip", "9"), Expressions.lessThan("ip", "8")), List.of()), "an empty range");

    // A one-character prefix is shorter than a gram, so the substring tiers cannot probe it and
    // only the dictionary prunes: the case where the cap's "keep every file" is observable.
    assertEquals(Set.of(f1, f2, f3), kept(table, config, Expressions.startsWith("ip", "7"), List.of()),
        "a one-char prefix: the dictionary range prunes where the substring tiers cannot");
    BuildConfig capped = LocalTableFixture.withPrefixMaxTerms(config, 1);
    assertEquals(all, kept(table, capped, Expressions.startsWith("ip", "7"), List.of()),
        "past KAHSHE_PREFIX_MAX_TERMS the proxy keeps every file rather than read the run");
  }
}
