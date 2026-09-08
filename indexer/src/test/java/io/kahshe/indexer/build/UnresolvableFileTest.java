package io.kahshe.indexer.build;

import static org.junit.jupiter.api.Assertions.assertInstanceOf;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

import io.kahshe.indexer.BuildConfig;
import io.kahshe.indexer.LocalTableFixture;
import java.io.IOException;
import java.nio.file.Path;
import org.apache.iceberg.DataFile;
import org.apache.iceberg.DataFiles;
import org.apache.iceberg.FileFormat;
import org.apache.iceberg.Table;
import org.apache.parquet.example.data.Group;
import org.apache.parquet.example.data.simple.SimpleGroupFactory;
import org.apache.parquet.hadoop.ParquetWriter;
import org.apache.parquet.hadoop.example.ExampleParquetWriter;
import org.apache.parquet.io.LocalOutputFile;
import org.apache.parquet.schema.LogicalTypeAnnotation;
import org.apache.parquet.schema.MessageType;
import org.apache.parquet.schema.PrimitiveType;
import org.apache.parquet.schema.Types;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

/**
 * The refusal in {@code DataFileIds} through a whole build, rather than on its own. Its unit tests
 * prove the check answers correctly; this one proves it is wired into the read, which is the half
 * that can silently stop being true.
 */
class UnresolvableFileTest {

  @TempDir Path tmp;

  /** The add_files shape: a Parquet with no field ids at all, registered into a real table. */
  private static void registerFileWithoutIds(Table table, String name) throws IOException {
    MessageType noIds =
        Types.buildMessage()
            .required(PrimitiveType.PrimitiveTypeName.BINARY)
            .as(LogicalTypeAnnotation.stringType())
            .named(LocalTableFixture.COLUMN)
            .named("row");
    String path = table.location() + "/data/" + name;
    java.io.File out = new java.io.File(java.net.URI.create(
        path.startsWith("file:") ? path : "file://" + path));
    SimpleGroupFactory rows = new SimpleGroupFactory(noIds);
    try (ParquetWriter<Group> writer =
        ExampleParquetWriter.builder(new LocalOutputFile(out.toPath())).withType(noIds).build()) {
      writer.write(rows.newGroup().append(LocalTableFixture.COLUMN, "charlie delta"));
    }
    DataFile file =
        DataFiles.builder(table.spec())
            .withPath(path)
            .withFormat(FileFormat.PARQUET)
            .withFileSizeInBytes(out.length())
            .withRecordCount(1)
            .build();
    table.newAppend().appendFile(file).commit();
    table.refresh();
  }

  @Test
  void aRegisteredFileWithNoFieldIdsFailsTheBuildInsteadOfBeingIndexedByPosition() throws Exception {
    BuildConfig config = LocalTableFixture.config();
    Table table = LocalTableFixture.createTable(tmp, "alpha bravo");
    // Control: the same build succeeds while every file in the table carries ids.
    IndexBuilder.buildColumn(table, LocalTableFixture.COLUMN, config);

    registerFileWithoutIds(table, "no-ids.parquet");

    // The refusal is raised inside a reader task, so collect() wraps it as a read failure. What
    // matters is that it is raised at all: the alternative is a build that succeeds and publishes
    // an index covering a file whose values it resolved by column position.
    IOException e =
        assertThrows(
            IOException.class,
            () -> IndexBuilder.buildColumn(table, LocalTableFixture.COLUMN, config),
            "a file that cannot be resolved by field id must stop the build");
    Throwable cause = e.getCause() == null ? e : e.getCause();
    assertInstanceOf(IllegalStateException.class, cause, "refused, not merely unreadable");
    assertTrue(cause.getMessage().contains("POSITION"), cause.getMessage());
    assertTrue(
        cause.getMessage().contains("schema.name-mapping.default"),
        "the message names the property that fixes it: " + cause.getMessage());
  }
}
