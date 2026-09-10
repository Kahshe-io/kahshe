package io.kahshe.indexer;

import io.kahshe.analysis.analyzer.Analyzer;
import io.kahshe.format.FormatConfig;
import java.io.IOException;
import java.nio.file.Path;
import org.apache.hadoop.conf.Configuration;
import org.apache.iceberg.DataFile;
import org.apache.iceberg.DataFiles;
import org.apache.iceberg.FileFormat;
import org.apache.iceberg.PartitionSpec;
import org.apache.iceberg.Schema;
import org.apache.iceberg.Table;
import org.apache.iceberg.data.GenericRecord;
import org.apache.iceberg.data.Record;
import org.apache.iceberg.data.parquet.GenericParquetWriter;
import org.apache.iceberg.hadoop.HadoopTables;
import org.apache.iceberg.io.FileAppender;
import org.apache.iceberg.parquet.Parquet;
import org.apache.iceberg.types.Types;
import io.kahshe.indexer.build.BuildBudget;

/** A tiny local Iceberg table (one string column, one data file) for index cache tests. */
public final class LocalTableFixture {
  public static final String COLUMN = "msg";
  /** The wide table's int column — a numeric {@code equals} needs a column that is not a string. */
  public static final String INT_COLUMN = "num";
  /** The wide table's second string column, so a rule can span two of them. */
  public static final String SECOND_COLUMN = "other";

  private LocalTableFixture() {}

  /** No REST catalog needed: HadoopTables over a temp dir, table IO covers the index root. */
  public static Table createTable(Path dir, String... values) throws IOException {
    Schema schema = new Schema(Types.NestedField.required(1, COLUMN, Types.StringType.get()));
    Table table = new HadoopTables(new Configuration()).create(schema, dir.resolve("tbl").toString());
    appendFile(table, "f1.parquet", values);
    return table;
  }

  /**
   * A table whose schema the caller chooses — it must still declare the flat {@link #COLUMN},
   * which is where the values land. One real data file is committed so a snapshot exists:
   * {@code buildColumn} answers null for a snapshotless table before it ever looks at the schema,
   * and the file must be REAL because a zero-record append commits a data-file entry whose parquet
   * is never physically written (the writer creates it lazily, on the first row) — a phantom any
   * test that reads past the schema would trip over.
   */
  public static Table createTable(Path dir, Schema schema, String... values) throws IOException {
    Table table = new HadoopTables(new Configuration()).create(schema, dir.resolve("tbl").toString());
    appendFile(table, "f1.parquet", values);
    return table;
  }

  /**
   * A three-column table — {@link #COLUMN}, {@link #INT_COLUMN}, {@link #SECOND_COLUMN} — with no
   * data file yet, for a test that needs rows spanning columns.
   *
   * <p>The single-column {@link #createTable} cannot express the case a cross-column rule exists
   * for: one row satisfying two fields against two rows satisfying one each. Append with
   * {@link #appendRows}.
   */
  public static Table createWideTable(Path dir) throws IOException {
    Schema schema = new Schema(
        Types.NestedField.optional(1, COLUMN, Types.StringType.get()),
        Types.NestedField.optional(2, INT_COLUMN, Types.IntegerType.get()),
        Types.NestedField.optional(3, SECOND_COLUMN, Types.StringType.get()));
    return new HadoopTables(new Configuration()).create(schema, dir.resolve("tbl").toString());
  }

  /** The time column of {@link #createTimedTable}. */
  public static final String TS_COLUMN = "ts";

  /**
   * A table with a TIMESTAMP column. Iceberg's generic data model hands a timestamp back as a
   * {@code LocalDateTime}, not as the {@code Long} of microseconds its internal representation
   * uses, and a fixture without one never exercises that path.
   */
  public static Table createTimedTable(Path dir) throws IOException {
    Schema schema = new Schema(
        Types.NestedField.optional(1, COLUMN, Types.StringType.get()),
        Types.NestedField.optional(2, INT_COLUMN, Types.IntegerType.get()),
        Types.NestedField.optional(3, TS_COLUMN, Types.TimestampType.withoutZone()));
    return new HadoopTables(new Configuration()).create(schema, dir.resolve("tbl").toString());
  }

  /** One data file of the timed table, a row per {@code {msg, num, LocalDateTime}} triple. */
  public static String appendTimedRows(Table table, String fileName, Object[]... rows)
      throws IOException {
    GenericRecord record = GenericRecord.create(table.schema());
    Record[] records = new Record[rows.length];
    for (int i = 0; i < rows.length; i++) {
      Record row = record.copy();
      row.setField(COLUMN, rows[i][0]);
      row.setField(INT_COLUMN, rows[i][1]);
      row.setField(TS_COLUMN, rows[i][2]);
      records[i] = row;
    }
    return appendRecords(table, fileName, records);
  }

  /** The list column of {@link #createCollectionTable}. */
  public static final String LIST_COLUMN = "tags";
  /** The map column of {@link #createCollectionTable}. */
  public static final String MAP_COLUMN = "props";

  /**
   * A table with a scalar, a {@code list<string>} and a {@code map<string,string>} — the shapes a
   * repeated-field index has to tell apart. No data file yet, like {@link #createWideTable}: the
   * caller writes real ones, because a zero-record append leaves a committed entry whose parquet
   * was never physically written.
   */
  public static Table createCollectionTable(Path dir) throws IOException {
    Schema schema = new Schema(
        Types.NestedField.optional(1, COLUMN, Types.StringType.get()),
        Types.NestedField.optional(2, LIST_COLUMN,
            Types.ListType.ofOptional(3, Types.StringType.get())),
        Types.NestedField.optional(4, MAP_COLUMN,
            Types.MapType.ofOptional(5, 6, Types.StringType.get(), Types.StringType.get())));
    return new HadoopTables(new Configuration()).create(schema, dir.resolve("tbl").toString());
  }

  /**
   * One data file of the collection table, a row per {@code {msg, List<String>, Map<String,String>}}
   * triple. Any of the three may be null, and the container may be empty.
   */
  public static String appendCollectionRows(Table table, String fileName, Object[]... rows)
      throws IOException {
    GenericRecord record = GenericRecord.create(table.schema());
    Record[] records = new Record[rows.length];
    for (int i = 0; i < rows.length; i++) {
      Record row = record.copy();
      row.setField(COLUMN, rows[i][0]);
      row.setField(LIST_COLUMN, rows[i][1]);
      row.setField(MAP_COLUMN, rows[i][2]);
      records[i] = row;
    }
    return appendRecords(table, fileName, records);
  }

  /**
   * Writes and commits one data file of the wide table, a row per {@code rows} triple of
   * {@code {msg, num, other}} — {@code num} an Integer, the other two Strings, any of them null.
   */
  public static String appendRows(Table table, String fileName, Object[]... rows)
      throws IOException {
    GenericRecord record = GenericRecord.create(table.schema());
    Record[] records = new Record[rows.length];
    for (int i = 0; i < rows.length; i++) {
      Record row = record.copy();
      row.setField(COLUMN, rows[i][0]);
      row.setField(INT_COLUMN, rows[i][1]);
      row.setField(SECOND_COLUMN, rows[i][2]);
      records[i] = row;
    }
    return appendRecords(table, fileName, records);
  }

  /** Writes and commits one more data file; returns its path as planning will report it. */
  public static String appendFile(Table table, String fileName, String... values)
      throws IOException {
    GenericRecord record = GenericRecord.create(table.schema());
    Record[] records = new Record[values.length];
    for (int i = 0; i < values.length; i++) {
      records[i] = record.copy(COLUMN, values[i]);
    }
    return appendRecords(table, fileName, records);
  }

  /**
   * A PARTITIONED table, identity on {@link #SECOND_COLUMN}; every other fixture here is
   * unpartitioned. {@code PartitionedTableTest} says what that leaves unproven.
   */
  public static Table createPartitionedTable(Path dir) throws IOException {
    Schema schema = new Schema(
        Types.NestedField.required(1, COLUMN, Types.StringType.get()),
        Types.NestedField.optional(2, INT_COLUMN, Types.IntegerType.get()),
        Types.NestedField.optional(3, SECOND_COLUMN, Types.StringType.get()));
    PartitionSpec spec = PartitionSpec.builderFor(schema).identity(SECOND_COLUMN).build();
    return new HadoopTables(new Configuration())
        .create(schema, spec, dir.resolve("tbl").toString());
  }

  /**
   * Appends one data file into the partition {@code partitionValue} names.
   *
   * <p>The partition tuple is built from the table's CURRENT spec, so the same call keeps working
   * across a spec change — which is what a partition-evolution test needs.
   */
  public static String appendPartitioned(
      Table table, String fileName, String partitionValue, Object[]... rows) throws IOException {
    GenericRecord record = GenericRecord.create(table.schema());
    Record[] records = new Record[rows.length];
    for (int i = 0; i < rows.length; i++) {
      Record row = record.copy();
      row.setField(COLUMN, rows[i][0]);
      row.setField(INT_COLUMN, rows[i][1]);
      row.setField(SECOND_COLUMN, rows[i][2]);
      records[i] = row;
    }
    Schema schema = table.schema();
    String path = table.location() + "/data/" + fileName;
    FileAppender<Record> appender =
        Parquet.write(table.io().newOutputFile(path))
            .schema(schema)
            .createWriterFunc(type -> GenericParquetWriter.create(schema, type))
            .overwrite()
            .build();
    try (appender) {
      for (Record row : records) {
        appender.add(row);
      }
    }
    PartitionSpec spec = table.spec();
    org.apache.iceberg.PartitionKey key = new org.apache.iceberg.PartitionKey(spec, schema);
    key.partition(records.length > 0 ? records[0] : record.copy(SECOND_COLUMN, partitionValue));
    DataFile file =
        DataFiles.builder(spec)
            .withPath(path)
            .withFormat(FileFormat.PARQUET)
            .withFileSizeInBytes(appender.length())
            .withMetrics(appender.metrics())
            .withPartition(key)
            .build();
    table.newAppend().appendFile(file).commit();
    return path;
  }

  /**
   * As above, with caller-built records — the shape a nested-column test needs, since
   * {@link #appendFile} fills only the flat {@link #COLUMN}. Build them against
   * {@code table.schema()}; a struct value is a {@code GenericRecord} over the struct's type.
   */
  public static String appendRecords(Table table, String fileName, Record... records)
      throws IOException {
    Schema schema = table.schema();
    String path = table.location() + "/data/" + fileName;
    FileAppender<Record> appender =
        Parquet.write(table.io().newOutputFile(path))
            .schema(schema)
            .createWriterFunc(type -> GenericParquetWriter.create(schema, type))
            .overwrite()
            .build();
    try (appender) {
      for (Record record : records) {
        appender.add(record);
      }
    }
    DataFile file =
        DataFiles.builder(table.spec())
            .withPath(path)
            .withFormat(FileFormat.PARQUET)
            .withFileSizeInBytes(appender.length())
            .withMetrics(appender.metrics())
            .build();
    table.newAppend().appendFile(file).commit();
    return path;
  }

  /**
   * Commits a POSITION DELETE of one row of {@code dataFilePath}, making the current snapshot
   * delete-bearing ({@code total-delete-files} &gt; 0) without changing the data file set.
   *
   * <p>The one thing no other fixture here can produce. Every reader in this project reads raw
   * data files and applies no delete file, so its counts on a merge-on-read snapshot are upper
   * bounds and its evidence says {@code advisory} rather than {@code exact}; until this existed,
   * that label could be tested only against a hand-made {@code Snapshot} summary, never against
   * a table that really carries a delete. Needs a format-v2 table, which is what
   * {@code HadoopTables.create} makes by default.
   *
   * @return the delete file's path
   */
  public static String appendPositionDelete(Table table, String dataFilePath, long position)
      throws IOException {
    String path = table.location() + "/data/delete-" + position + "-"
        + Integer.toHexString(dataFilePath.hashCode()) + ".parquet";
    Schema deleteSchema = org.apache.iceberg.io.DeleteSchemaUtil.pathPosSchema();
    org.apache.iceberg.deletes.PositionDeleteWriter<Record> writer =
        // withSpec, not forTable: forTable also adopts the table's schema as the delete's ROW
        // schema, which makes the Parquet schema (file_path, pos, row) and asks for a row this
        // fixture does not carry. The two-column path-and-position form is the whole point.
        Parquet.writeDeletes(table.io().newOutputFile(path))
            .withSpec(table.spec())
            .createWriterFunc(type -> GenericParquetWriter.create(deleteSchema, type))
            .overwrite()
            .buildPositionWriter();
    try (writer) {
      org.apache.iceberg.deletes.PositionDelete<Record> delete =
          org.apache.iceberg.deletes.PositionDelete.create();
      writer.write(delete.set(dataFilePath, position, null));
    }
    table.newRowDelta().addDeletes(writer.toDeleteFile()).commit();
    return path;
  }

  public static BuildConfig config() {
    return config(true);
  }

  public static BuildConfig config(boolean gramIndex) {
    return config(gramIndex, defaultTermBuildDir());
  }

  /** As above, with the directory the term build's sorted runs live in. */
  public static BuildConfig config(boolean gramIndex, String termBuildDir) {
    return config(gramIndex, true, 16L * 1024 * 1024 * 1024, termBuildDir);
  }

  /** Blooms and grams, no term dictionary: what KAHSHE_TERM_INDEX=false builds. */
  public static BuildConfig configWithoutTermIndex() {
    return config(true, false, 16L * 1024 * 1024 * 1024, defaultTermBuildDir());
  }

  /** As above, with the term build's local-disk budget for its sorted runs. */
  public static BuildConfig config(
      boolean gramIndex,
      boolean termIndex,
      long termBuildMaxSpillBytes,
      String termBuildDir) {
    // A 1 MiB arena, not the 64 MiB default. The arena is ALLOCATED rather than grown, so the
    // production default would commit 512 MiB across eight readers in every test JVM -- which is
    // the design working as intended and no reason for the suite to pay it. Tests that care about
    // the flush boundary set it explicitly.
    return config(gramIndex, termIndex, termBuildMaxSpillBytes, termBuildDir,
        8, 1L * 1024 * 1024);
  }

  /**
   * As above, with the reader count and the per-reader term buffer size.
   *
   * <p>These are the two knobs a build's concurrency and its flush boundary turn on, and until
   * they moved into Config neither could be varied from a test — so the two tests that claimed to
   * vary them were building both halves of their comparison identically.
   */
  public static BuildConfig config(
      boolean gramIndex,
      boolean termIndex,
      long termBuildMaxSpillBytes,
      String termBuildDir,
      int indexThreads,
      long termBufferBytes) {
    FormatConfig format = new FormatConfig(
        "", "", "", "", "us-east-1", "", java.util.Map.of(), "table",
        64L * 1024 * 1024,
        gramIndex,
        termIndex,
        100_000);
    BuildConfig build = new BuildConfig(
        format,
        io.kahshe.analysis.analyzer.Analyzer.DEFAULT_MAX_TOKEN_LEN,
        io.kahshe.format.type.gram.Grams.DEFAULT_SIZE,
        // 32 MiB gram cap, not 1 GiB. The fixture used to declare a ceiling four times the test
        // JVM's whole heap -- the same contradiction BuildBudget now refuses in production, hidden
        // here only because a three-row table never approaches it.
        32L * 1024 * 1024,
        termBuildMaxSpillBytes,
        termBuildDir,
        indexThreads,
        termBufferBytes,
        300_000);
    return build;
  }

  /** A table source for tests that never load: the indexer is off, or the table is handed in. */
  public static TableSource noTables() {
    return new TableSource() {
      @Override
      public org.apache.iceberg.Table load(String prefix, org.apache.iceberg.catalog.TableIdentifier ident) {
        throw new IllegalStateException("this test's table source loads nothing: " + prefix + " " + ident);
      }

      @Override
      public void invalidate(String prefix, org.apache.iceberg.catalog.TableIdentifier ident) {}
    };
  }

  /**
   * A source whose load never returns: for a test that wants a LIVE indexer (its worker thread
   * running, so the process reads as one that builds) but no build to race its assertions.
   */
  public static TableSource parked() {
    return new TableSource() {
      @Override
      public org.apache.iceberg.Table load(String prefix, org.apache.iceberg.catalog.TableIdentifier ident) {
        try {
          new java.util.concurrent.CountDownLatch(1).await();
        } catch (InterruptedException e) {
          Thread.currentThread().interrupt();
        }
        throw new IllegalStateException("parked source interrupted: " + prefix + " " + ident);
      }

      @Override
      public void invalidate(String prefix, org.apache.iceberg.catalog.TableIdentifier ident) {}
    };
  }

  private static String defaultTermBuildDir() {
    return System.getProperty("java.io.tmpdir") + java.io.File.separator + "kahshe-term-build";
  }

  /** The current file-scan-tasks, as the pruner receives them from planning. */
  public static java.util.List<org.apache.iceberg.FileScanTask> planTasks(Table table) throws IOException {
    return planTasks(table, table.currentSnapshot().snapshotId());
  }

  /**
   * The file-scan-tasks of one specific snapshot -- what a TIME TRAVEL request plans.
   *
   * <p>{@code PlanService} takes this branch whenever the request pins a snapshot other than the
   * current one, and the pruner runs on the result either way, so a test that only ever plans the
   * current snapshot never exercises the case where the tasks include files the index has since
   * tombstoned.
   */
  public static java.util.List<org.apache.iceberg.FileScanTask> planTasks(Table table, long snapshotId)
      throws IOException {
    java.util.List<org.apache.iceberg.FileScanTask> tasks = new java.util.ArrayList<>();
    try (org.apache.iceberg.io.CloseableIterable<org.apache.iceberg.FileScanTask> planned =
        table.newScan().useSnapshot(snapshotId).planFiles()) {
      planned.forEach(tasks::add);
    }
    return tasks;
  }

  /** {@code base} with the prefix cap replaced; the reader keeps every file past it. */
  public static BuildConfig withPrefixMaxTerms(BuildConfig base, int cap) {
    return io.kahshe.common.Records.with(base, java.util.Map.of("prefixMaxTerms", cap));
  }

  /** {@code base} with one component replaced, at whatever depth the record tree holds it. */
  public static <T extends java.lang.Record> T with(T base, String component, Object value) {
    return io.kahshe.common.Records.with(base, java.util.Map.of(component, value));
  }
}
