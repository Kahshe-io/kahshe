package io.kahshe.format;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

import java.nio.file.Files;
import java.nio.file.Path;
import java.util.Map;
import org.apache.iceberg.Table;
import org.apache.iceberg.mapping.NameMapping;
import org.apache.iceberg.mapping.NameMappingParser;
import org.apache.parquet.example.data.Group;
import org.apache.parquet.example.data.simple.SimpleGroupFactory;
import org.apache.parquet.hadoop.ParquetWriter;
import org.apache.parquet.hadoop.example.ExampleParquetWriter;
import org.apache.parquet.io.LocalOutputFile;
import org.apache.parquet.schema.MessageType;
import org.apache.parquet.schema.PrimitiveType;
import org.apache.parquet.schema.Types;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

/**
 * {@link DataFileIds}: the mapping is read from Iceberg's own property, absent is a real answer,
 * and a malformed one is refused rather than left to the positional fallback — that class says
 * why the fallback is the silent wrong answer.
 *
 * <p>The refusal itself is exercised against a real Parquet file written WITHOUT field ids, which
 * is what {@code add_files} and a Hive {@code migrate} leave behind, because a check that has
 * never seen such a file is a check that has never run.
 */
class DataFileIdsTest {

  private static final String MAPPING =
      "[{\"field-id\":1,\"names\":[\"msg\"]},{\"field-id\":2,\"names\":[\"num\"]}]";

  @Test
  void aTableWithNoMappingHasNone() {
    assertNull(DataFileIds.mappingOf(table(Map.of())),
        "absent is a real answer: a file carrying its own ids needs no mapping");
    assertNull(DataFileIds.mappingOf(table(Map.of(DataFileIds.NAME_MAPPING, "   "))));
  }

  @Test
  void theMappingIsReadFromIcebergsOwnProperty() {
    NameMapping mapping = DataFileIds.mappingOf(table(Map.of(DataFileIds.NAME_MAPPING, MAPPING)));
    assertNotNull(mapping);
    assertNotNull(mapping.find("msg"), "the mapping must resolve the column names it declares");
    assertEquals(1, mapping.find("msg").id());
    assertEquals(2, mapping.find("num").id());
  }

  /**
   * A malformed mapping fails loudly. Returning null would send the read to the positional
   * fallback — the silent wrong answer this class exists to prevent — so the noisier outcome is
   * the correct one.
   */
  @Test
  void aMalformedMappingIsRefusedRatherThanIgnored() {
    IllegalStateException e = assertThrows(IllegalStateException.class,
        () -> DataFileIds.mappingOf(table(Map.of(DataFileIds.NAME_MAPPING, "{not json"))));
    assertTrue(e.getMessage().contains("COLUMN POSITION"), e.getMessage());
  }

  /** The property name is Iceberg's, not kahshe's; getting it wrong silently disables the fix. */
  @Test
  void thePropertyNameIsTheSpecs() {
    assertEquals("schema.name-mapping.default", DataFileIds.NAME_MAPPING);
    assertEquals(org.apache.iceberg.TableProperties.DEFAULT_NAME_MAPPING,
        DataFileIds.NAME_MAPPING,
        "it must be Iceberg's own constant, so a rename upstream is caught here");
  }

  private static Table table(Map<String, String> properties) {
    return (Table) java.lang.reflect.Proxy.newProxyInstance(
        DataFileIdsTest.class.getClassLoader(), new Class<?>[] {Table.class},
        (proxy, method, args) -> "properties".equals(method.getName()) ? properties : null);
  }

  /** A Parquet file with no field ids at all: what add_files and Hive migrate leave behind. */
  private static java.io.File withoutFieldIds(Path dir) throws Exception {
    MessageType schema =
        Types.buildMessage()
            .required(PrimitiveType.PrimitiveTypeName.BINARY)
            .as(org.apache.parquet.schema.LogicalTypeAnnotation.stringType())
            .named("msg")
            .named("row");
    java.io.File out = dir.resolve("no-ids.parquet").toFile();
    Files.deleteIfExists(out.toPath());
    SimpleGroupFactory rows = new SimpleGroupFactory(schema);
    try (ParquetWriter<Group> writer =
        ExampleParquetWriter.builder(new LocalOutputFile(out.toPath())).withType(schema).build()) {
      writer.write(rows.newGroup().append("msg", "disk pressure"));
    }
    return out;
  }

  @Test
  void aFileWithNoFieldIdsAndNoMappingIsRefused(@TempDir Path dir) throws Exception {
    java.io.File file = withoutFieldIds(dir);
    IllegalStateException e =
        assertThrows(
            IllegalStateException.class,
            () -> DataFileIds.requireResolvable(org.apache.iceberg.Files.localInput(file), null),
            "resolving this file by POSITION is the silent wrong answer the format exists to "
                + "prevent, so it must be refused rather than read");
    assertTrue(e.getMessage().contains("POSITION"), e.getMessage());
    assertTrue(e.getMessage().contains(DataFileIds.NAME_MAPPING), e.getMessage());
  }

  @Test
  void theSameFileIsAllowedWhenTheTableDeclaresAMapping(@TempDir Path dir) throws Exception {
    java.io.File file = withoutFieldIds(dir);
    // The mapping is exactly what resolves a file carrying no ids, so the refusal must not fire.
    DataFileIds.requireResolvable(
        org.apache.iceberg.Files.localInput(file), NameMappingParser.fromJson(MAPPING));
  }

  @Test
  void aMappingMeansTheFileIsNeverOpened() {
    // Pointed at nothing: reading a footer here would fail, so passing proves the footer read is
    // skipped when a mapping resolves the file. That skip is why a mapped table pays nothing.
    DataFileIds.requireResolvable(
        org.apache.iceberg.Files.localInput("/nonexistent/kahshe/never-opened.parquet"),
        NameMappingParser.fromJson(MAPPING));
  }

  @Test
  void aFileCarryingItsOwnIdsIsAllowedWithNoMapping(@TempDir Path dir) throws Exception {
    org.apache.iceberg.Table t =
        io.kahshe.indexer.LocalTableFixture.createTable(dir, "disk pressure", "ok");
    String path = t.currentSnapshot().addedDataFiles(t.io()).iterator().next().location();
    // An Iceberg-written file carries ids, so it resolves with no mapping -- and this is the case
    // that proves the footer read and the InputFile adapter work against a real file.
    DataFileIds.requireResolvable(org.apache.iceberg.Files.localInput(path), null);
  }

  /**
   * The nested case, and why the check is stricter than Iceberg's: {@code hasIds} is an ANY test,
   * so a file whose OUTER group carries an id and whose leaf does not passes it — and a read of
   * that file does not throw either. {@code convert} invents an id for the leaf and
   * {@code convertAndPrune}, which is what a read schema is built from, drops the column entirely.
   *
   * <p>A column that is not there reads as null, a null contributes no terms (§3.5), and the file
   * is still recorded as covered — so a probe for a term that file holds finds it absent and
   * prunes it. That is the forbidden class reached without an error, which is why the refusal
   * walks to the leaves rather than asking {@code hasIds}.
   */
  @Test
  void aFileWithIdsOnTheGroupButNotTheLeafIsRefused(@TempDir Path dir) throws Exception {
    MessageType partialIds =
        Types.buildMessage()
            .requiredGroup()
            .required(PrimitiveType.PrimitiveTypeName.BINARY)
            .as(org.apache.parquet.schema.LogicalTypeAnnotation.stringType())
            .named("inner")
            .id(1)
            .named("outer")
            .named("row");
    assertTrue(
        org.apache.iceberg.parquet.ParquetSchemaUtil.hasIds(partialIds),
        "hasIds is an ANY test: one id anywhere makes the whole file look resolvable");
    // Iceberg does not fail on it either -- it prunes the column out of the read schema.
    assertTrue(
        org.apache.iceberg.parquet.ParquetSchemaUtil.convertAndPrune(partialIds).columns().isEmpty(),
        "the column a read would project is dropped, so it reads as null rather than throwing");

    java.io.File out = dir.resolve("partial-ids.parquet").toFile();
    SimpleGroupFactory rows = new SimpleGroupFactory(partialIds);
    try (ParquetWriter<Group> writer =
        ExampleParquetWriter.builder(new LocalOutputFile(out.toPath()))
            .withType(partialIds)
            .build()) {
      Group row = rows.newGroup();
      row.addGroup("outer").append("inner", "disk pressure");
      writer.write(row);
    }
    IllegalStateException e =
        assertThrows(
            IllegalStateException.class,
            () -> DataFileIds.requireResolvable(org.apache.iceberg.Files.localInput(out), null),
            "hasIds passes this file; the leaf walk is what must not");
    assertTrue(e.getMessage().contains("outer.inner"), e.getMessage());
    assertTrue(
        e.getMessage().contains("not on every column"),
        "the partial case is named apart from the no-ids case, because the fix differs: "
            + e.getMessage());
  }

  /**
   * The check must not refuse a correct file. Parquet's three-level list and map encodings put a
   * synthetic REPEATED group between a field and its element and Iceberg does not identify those,
   * so a rule that asked groups for ids would refuse every table with a list or a map in it.
   */
  @Test
  void aListAndAMapWrittenByIcebergAreAllowed(@TempDir Path dir) throws Exception {
    org.apache.iceberg.Table t = io.kahshe.indexer.LocalTableFixture.createCollectionTable(dir);
    io.kahshe.indexer.LocalTableFixture.appendCollectionRows(
        t,
        "collections.parquet",
        new Object[] {"disk pressure", java.util.List.of("a", "b"), java.util.Map.of("k", "v")});
    t.refresh();
    String path = t.currentSnapshot().addedDataFiles(t.io()).iterator().next().location();
    DataFileIds.requireResolvable(org.apache.iceberg.Files.localInput(path), null);
  }
}
