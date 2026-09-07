package io.kahshe.proxy.catalog;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNull;

import java.nio.charset.StandardCharsets;
import java.util.List;
import org.junit.jupiter.api.Test;

class IndexPolicyTest {
  private static Mutations.IndexPolicy of(String json) {
    return Mutations.indexPolicy(json.getBytes(StandardCharsets.UTF_8));
  }

  @Test
  void extractsColumnsAndSnapshot() {
    var policy =
        of("{\"metadata\":{\"current-snapshot-id\":42,\"properties\":{\"kahshe.index\":\"msg, detail\"}}}");
    assertEquals(List.of("msg", "detail"), policy.columns());
    assertEquals(42, policy.snapshotId());
  }

  @Test
  void absentPropertyMeansNoPolicy() {
    assertNull(of("{\"metadata\":{\"current-snapshot-id\":42,\"properties\":{}}}"));
  }

  @Test
  void aSearchPropertyAloneNoLongerTriggersAnything() {
    // kahshe.search is a leftover from a search tier that no longer exists: alone it names nothing
    // this proxy indexes, so no policy and nothing observed.
    assertNull(
        of("{\"metadata\":{\"current-snapshot-id\":42,\"properties\":{\"kahshe.search\":\"msg\"}}}"));
  }

  @Test
  void aSearchPropertyBesideAnIndexPolicyIsIgnoredRatherThanRejected() {
    // an existing table may still carry the property; it must not stop the index policy working
    var policy =
        of("{\"metadata\":{\"current-snapshot-id\":42,\"properties\":"
            + "{\"kahshe.index\":\"msg\",\"kahshe.search\":\"detail\"}}}");
    assertEquals(List.of("msg"), policy.columns());
    assertEquals(42, policy.snapshotId());
  }

  @Test
  void snapshotlessTableMeansNoPolicy() {
    assertNull(of("{\"metadata\":{\"properties\":{\"kahshe.index\":\"msg\"}}}"));
  }

  @Test
  void malformedBodyMeansNoPolicy() {
    assertNull(of("not json"));
  }
}
