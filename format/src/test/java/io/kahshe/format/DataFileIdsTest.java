package io.kahshe.format;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

import java.util.Map;
import org.apache.iceberg.Table;
import org.apache.iceberg.mapping.NameMapping;
import org.junit.jupiter.api.Test;

/**
 * {@link DataFileIds}: the mapping is read from Iceberg's own property, absent is a real answer,
 * and a malformed one is refused rather than left to the positional fallback — that class says
 * why the fallback is the silent wrong answer.
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
}
