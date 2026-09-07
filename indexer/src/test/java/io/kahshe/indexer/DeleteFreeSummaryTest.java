package io.kahshe.indexer;

import static org.junit.jupiter.api.Assertions.assertEquals;

import java.nio.file.Path;
import org.apache.iceberg.Table;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

/**
 * Pins the assumption two refusals are built on.
 *
 * <p>{@code PlanService.refuseIfDeleteBearing} and {@code CountRoutes} both fail CLOSED: a snapshot
 * whose summary does not state {@code total-delete-files} is treated as possibly delete-bearing and
 * refused, because "we could not prove it is delete-free" and "it is delete-free" are different
 * claims and only one is safe to serve.
 *
 * <p>That is the right default and it has a sharp edge: if Iceberg ever stopped emitting the key
 * for ordinary append-only tables, both endpoints would refuse EVERYTHING, and the failure would
 * look like a kahshe outage rather than like a changed assumption. So the assumption is asserted
 * rather than trusted — this test is the thing that fails first, with a message that says what
 * actually happened.
 */
class DeleteFreeSummaryTest {
  @TempDir Path tmp;

  @Test
  void anAppendOnlyTableStatesThatItHasNoDeleteFiles() throws Exception {
    Table table = LocalTableFixture.createTable(tmp, "alpha alpha");
    LocalTableFixture.appendFile(table, "f2.parquet", "bravo bravo");
    table.refresh();

    assertEquals(
        "0",
        table.currentSnapshot().summary().get("total-delete-files"),
        "an append-only snapshot no longer reports total-delete-files=0. Both /plan and /_count "
            + "fail closed on that key, so they will now refuse every request on every table. "
            + "Summary was: " + table.currentSnapshot().summary());
  }
}
