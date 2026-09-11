package io.kahshe.indexer;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertTrue;

import java.io.IOException;
import java.nio.file.Path;
import java.util.ArrayList;
import java.util.LinkedHashSet;
import java.util.List;
import java.util.Set;
import org.apache.iceberg.DeleteFile;
import org.apache.iceberg.FileContent;
import org.apache.iceberg.FileScanTask;
import org.apache.iceberg.Table;
import org.apache.iceberg.data.Record;
import org.apache.iceberg.data.parquet.GenericParquetReaders;
import org.apache.iceberg.io.CloseableIterable;
import org.apache.iceberg.io.DeleteSchemaUtil;
import org.apache.iceberg.parquet.Parquet;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

/**
 * THE INSTRUMENT, NOT THE TIER. Measure the quantity a feature's value equals before building the
 * feature; if the measurement says zero, the feature was never worth its weight.
 *
 * <p>The question a delete-file pruning tier's entire value equals, and which nobody here had
 * counted: <b>when a query plans a delete-bearing table, how many delete files does Iceberg BIND to
 * each data file, and how many of those actually delete a row in it?</b> Iceberg does that binding
 * server-side — which is kahshe's side, since kahshe is what plans — through {@code DeleteFileIndex}, so
 * {@link FileScanTask#deletes()} is already the product of Iceberg's own matching and a kahshe tier
 * could only prune what survives it. Measuring anything else measures a strawman.
 *
 * <p><b>The answer, measured 2026-09-10 against Iceberg 1.11.0, was neither of the two expected.</b>
 * There IS fan-out — a position delete naming one data file binds to all three in a three-file table
 * — but its cause is not that the binding is naive. Iceberg prunes position deletes by the delete
 * file's {@code file_path} bounds and does it correctly; the default
 * {@code write.metadata.metrics.default} is {@code truncate(16)}, and sixteen characters of a data
 * file path is entirely common prefix — a warehouse root, a database and a table name stand
 * before the first byte that could tell two files apart — so the recorded bounds differ only in
 * their final character and match every file in the table. Write the same delete with full
 * {@code file_path} bounds and the binding is exact. {@code referenced_data_file} was null here and
 * makes it exact when set, which is what an Iceberg v3 deletion vector does by construction.
 *
 * <p><b>So this closes the question rather than justifying the tier,</b> and closing it is the
 * benefit: the headroom is real and is recoverable by one table property on the writer, or by
 * moving to v3. An index over delete files would be an elaborate answer to a problem we would then
 * have to build, ship, version and explain. A real gap, and the cheap lever wins.
 *
 * <p>Position deletes only, and the honest reason: they name {@code (file_path, pos)}, so relevance
 * is exactly decidable by reading the delete file. Equality deletes need a value join against the
 * data file to decide relevance, which is a larger measurement; {@link #probe} reports them as
 * unmeasured rather than assuming either answer, and they remain the one place stage 3 could still
 * have headroom nobody has counted.
 */
class DeleteFanoutProbe {

  @TempDir Path tmp;

  /** One data file's delete fan-out: what Iceberg bound to it, and what actually touches it. */
  record Fanout(String dataFile, int boundPosition, int relevantPosition, int boundEquality) {
    int wastedPosition() {
      return boundPosition - relevantPosition;
    }
  }

  /**
   * The measurement, over whatever table it is handed. Plans the table as a reader would and, for
   * every data file, separates the position-delete files Iceberg bound from the ones that name it.
   */
  static List<Fanout> measure(Table table) throws IOException {
    List<Fanout> out = new ArrayList<>();
    try (CloseableIterable<FileScanTask> tasks = table.newScan().planFiles()) {
      for (FileScanTask task : tasks) {
        String dataFile = task.file().location();
        int boundPosition = 0;
        int relevantPosition = 0;
        int boundEquality = 0;
        for (DeleteFile delete : task.deletes()) {
          if (delete.content() == FileContent.EQUALITY_DELETES) {
            boundEquality++;
            continue;
          }
          boundPosition++;
          if (pathsNamedBy(table, delete).contains(dataFile)) {
            relevantPosition++;
          }
        }
        out.add(new Fanout(dataFile, boundPosition, relevantPosition, boundEquality));
      }
    }
    return out;
  }


  /** A position delete naming one data file, written under a chosen metrics config. */
  private static void positionDelete(Table table, String dataFilePath, long pos, String pathMetrics)
      throws IOException {
    String path = table.location() + "/data/pd-" + pathMetrics + "-"
        + Integer.toHexString(dataFilePath.hashCode()) + ".parquet";
    org.apache.iceberg.Schema deleteSchema = DeleteSchemaUtil.pathPosSchema();
    org.apache.iceberg.MetricsConfig metrics =
        org.apache.iceberg.MetricsConfig.fromProperties(
            java.util.Map.of("write.metadata.metrics.column.file_path", pathMetrics));
    org.apache.iceberg.deletes.PositionDeleteWriter<Record> writer =
        Parquet.writeDeletes(table.io().newOutputFile(path))
            .withSpec(table.spec())
            .createWriterFunc(
                type -> org.apache.iceberg.data.parquet.GenericParquetWriter.create(
                    deleteSchema, type))
            .metricsConfig(metrics)
            .overwrite()
            .buildPositionWriter();
    try (writer) {
      org.apache.iceberg.deletes.PositionDelete<Record> d =
          org.apache.iceberg.deletes.PositionDelete.create();
      writer.write(d.set(dataFilePath, pos, null));
    }
    table.newRowDelta().addDeletes(writer.toDeleteFile()).commit();
  }

  /** Every data file path a position-delete file names. Read, not inferred from bounds. */
  private static Set<String> pathsNamedBy(Table table, DeleteFile delete) throws IOException {
    Set<String> paths = new LinkedHashSet<>();
    org.apache.iceberg.Schema schema = DeleteSchemaUtil.pathPosSchema();
    try (CloseableIterable<Record> rows =
        Parquet.read(table.io().newInputFile(delete.location()))
            .project(schema)
            .createReaderFunc(type -> GenericParquetReaders.buildReader(schema, type))
            .build()) {
      for (Record row : rows) {
        paths.add(String.valueOf(row.getField("file_path")));
      }
    }
    return paths;
  }

  /**
   * THE STRUCTURAL ANSWER, and it is not the one the question assumed. A table of three data files
   * with one position delete naming exactly one of them, written twice: once under Iceberg's
   * DEFAULT metrics config, and once with full bounds on {@code file_path}.
   *
   * <p><b>Measured 2026-09-10, Iceberg 1.11.0.</b> Under the default the delete binds to all three
   * data files — two of the three bindings cannot touch the file they are attached to — and under
   * full bounds it binds to one. The cause is therefore NOT that Iceberg's binding is naive: it
   * prunes position deletes by the delete file's {@code file_path} bounds, and does it correctly.
   * The cause is that {@code write.metadata.metrics.default} is {@code truncate(16)}, and sixteen
   * characters of a data file path is entirely common prefix, so the recorded lower and upper
   * bounds differ only in their final character and match every file in the table.
   *
   * <p><b>Which is why this closes the question rather than justifying the tier.</b> The
   * headroom a delete-file pruning tier would recover is real, and it is recoverable by setting one
   * table property on the writer, or by Iceberg v3's {@code referenced_data_file} (null here, and
   * exact when set — a deletion vector names one data file by construction). An index over delete
   * files would be an elaborate answer to a problem with a one-line fix that we do not have to
   * build, ship, version or explain.
   *
   * <p>These assertions are the record. If a later Iceberg changes either behaviour they go red,
   * and the decision is reopened by the failure rather than by anyone remembering to re-ask.
   */
  @Test
  void positionDeleteFanoutIsCausedByBoundTruncationAndNotByTheBinding() throws Exception {
    Table table = LocalTableFixture.createTable(tmp, "alpha");
    LocalTableFixture.appendFile(table, "second.parquet", "bravo");
    LocalTableFixture.appendFile(table, "third.parquet", "charlie");
    table.refresh();

    List<String> dataFiles = new ArrayList<>();
    try (CloseableIterable<FileScanTask> tasks = table.newScan().planFiles()) {
      for (FileScanTask task : tasks) {
        dataFiles.add(task.file().location());
      }
    }
    assertEquals(3, dataFiles.size(), "precondition: three data files to fan out across");
    String deleted = dataFiles.get(0);

    positionDelete(table, deleted, 0L, "truncate(16)");
    table.refresh();
    List<Fanout> truncated = measure(table);
    assertEquals(1, relevant(truncated), "the delete names one data file: one binding is real");
    assertEquals(3, bound(truncated),
        "MEASURED: under default truncate(16) bounds the delete binds to every data file, because "
            + "sixteen characters of a path is common prefix. If this changes, re-measure before "
            + "acting on item 46 stage 3.");

    // Same table, same delete, bounds not truncated.
    Table full = LocalTableFixture.createTable(tmp.resolve("full"), "alpha");
    LocalTableFixture.appendFile(full, "second.parquet", "bravo");
    LocalTableFixture.appendFile(full, "third.parquet", "charlie");
    full.refresh();
    List<String> fullFiles = new ArrayList<>();
    try (CloseableIterable<FileScanTask> tasks = full.newScan().planFiles()) {
      for (FileScanTask task : tasks) {
        fullFiles.add(task.file().location());
      }
    }
    positionDelete(full, fullFiles.get(0), 0L, "full");
    full.refresh();
    List<Fanout> exact = measure(full);
    assertEquals(1, relevant(exact));
    assertEquals(1, bound(exact),
        "MEASURED: with full file_path bounds Iceberg binds the delete to the one file it names. "
            + "The binding is not naive; the default metadata is. A kahshe delete tier would be "
            + "recovering what one table property recovers for free.");
    assertEquals(0, exact.stream().mapToInt(Fanout::wastedPosition).sum());
  }

  private static int bound(List<Fanout> f) {
    return f.stream().mapToInt(Fanout::boundPosition).sum();
  }

  private static int relevant(List<Fanout> f) {
    return f.stream().mapToInt(Fanout::relevantPosition).sum();
  }

  /**
   * The same measurement against a REAL table, for the case the structural test cannot settle:
   * many delete files, many partitions, and equality deletes, where binding is by partition and
   * sequence number and the waste is a question of degree.
   *
   * <p>Runs only with {@code -Dkahshe.probe.deleteFanout=<warehouse>} and
   * {@code -Dkahshe.probe.table=<namespace.table>}. It prints; it asserts nothing, because what it
   * would assert is the thing being measured.
   */
  @Test
  void probe() throws Exception {
    String warehouse = System.getProperty("kahshe.probe.deleteFanout");
    org.junit.jupiter.api.Assumptions.assumeTrue(warehouse != null,
        "set -Dkahshe.probe.deleteFanout=<warehouse> -Dkahshe.probe.table=<ns.table>");
    String name = System.getProperty("kahshe.probe.table");
    assertTrue(name != null && name.contains("."), "set -Dkahshe.probe.table=<namespace.table>");

    org.apache.iceberg.hadoop.HadoopCatalog catalog = new org.apache.iceberg.hadoop.HadoopCatalog();
    catalog.setConf(new org.apache.hadoop.conf.Configuration());
    catalog.initialize("probe", java.util.Map.of("warehouse", warehouse));
    Table table = catalog.loadTable(org.apache.iceberg.catalog.TableIdentifier.of(
        name.substring(0, name.lastIndexOf('.')), name.substring(name.lastIndexOf('.') + 1)));

    List<Fanout> fanout = measure(table);
    int bound = fanout.stream().mapToInt(Fanout::boundPosition).sum();
    int relevant = fanout.stream().mapToInt(Fanout::relevantPosition).sum();
    int equality = fanout.stream().mapToInt(Fanout::boundEquality).sum();
    System.out.printf(
        "delete fan-out on %s: %d data files, %d position-delete bindings, %d of them relevant, "
            + "%d wasted (%.1f%%), %d equality-delete bindings UNMEASURED (relevance needs a value "
            + "join)%n",
        name, fanout.size(), bound, relevant, bound - relevant,
        bound == 0 ? 0.0 : 100.0 * (bound - relevant) / bound, equality);
    for (Fanout f : fanout) {
      if (f.wastedPosition() > 0) {
        System.out.printf("  %s: %d bound, %d relevant%n",
            f.dataFile(), f.boundPosition(), f.relevantPosition());
      }
    }
  }
}
