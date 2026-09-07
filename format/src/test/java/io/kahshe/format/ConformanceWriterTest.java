package io.kahshe.format;

import static org.junit.jupiter.api.Assertions.assertEquals;

import io.kahshe.indexer.BuildConfig;
import io.kahshe.indexer.build.IndexBuilder;
import io.kahshe.indexer.LocalTableFixture;
import java.nio.file.Path;
import java.util.Map;
import org.apache.iceberg.Table;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

/**
 * The writers against the committed bytes: a fresh build of the fixture's rows decodes to the
 * same content as the golden artifact -- both documents with their volatile fields normalized,
 * every aggregate range's rows, every gram's bitmap, every file's bloom bytes. Parquet bytes are
 * never compared. Verified red with the writer's file count off by one.
 */
class ConformanceWriterTest {
  @TempDir Path tmp;

  @Test
  void aFreshBuildDecodesToTheGoldenContent() throws Exception {
    Table table = ConformanceSupport.table(tmp);
    String golden = "file:" + tmp.resolve("golden");
    Conformance.materialize(Conformance.FIXTURE.resolve("index"), table, golden);
    String fresh = "file:" + tmp.resolve("fresh");
    BuildConfig config = ConformanceSupport.config(fresh);
    IndexBuilder.buildColumn(table, LocalTableFixture.COLUMN, config);
    IndexBuilder.buildColumn(table, ConformanceSupport.LIST_COLUMN, config);

    // Both columns: the list column's two build decisions (ConformanceSupport.LIST_QUERIES,
    // LIST_COUNTS) show only in the dictionary these snapshots decode to, not in any query answer.
    for (int fieldId
        : new int[] {ConformanceSupport.fieldId(table), ConformanceSupport.listFieldId(table)}) {
      Map<String, Object> want = Conformance.snapshot(table, golden, fieldId);
      Map<String, Object> got = Conformance.snapshot(table, fresh, fieldId);
      for (String part : want.keySet()) {
        assertEquals(want.get(part), got.get(part), "f" + fieldId + " " + part);
      }
      assertEquals(want.keySet(), got.keySet(), "f" + fieldId);
    }
  }
}
