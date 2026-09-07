package io.kahshe.format.type.bloom;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.junit.jupiter.api.Assertions.assertTrue;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import java.util.List;
import org.junit.jupiter.api.Test;

class IndexMetaTest {
  private static final ObjectMapper MAPPER = new ObjectMapper();

  @Test
  void roundTripsSpecVocabularyDocument() throws Exception {
    IndexMeta meta = new IndexMeta();
    meta.uuid = "9c12d441-03fe-4693-9a96-a0705ddf69c1";
    meta.tableUuid = "fb072c92-a02b-11e9-ae9c-1bb7bc9eca94";
    meta.location = "s3://bucket/table/_index/ngram-bloom-msg";
    meta.column = "msg";
    meta.ngram = 3;
    meta.fpp = 0.01;
    meta.snapshotId = 42L;
    meta.timestampMs = 1_700_000_000_000L;
    meta.leafFiles = new java.util.ArrayList<>(List.of("leaf-41.parquet", "leaf-42.parquet"));
    meta.filesCovered = 52;
    meta.indexBytes = 1234;

    JsonNode json = meta.toJson();
    assertEquals(42L, json.path("snapshots").path(0).path("source-table-snapshot-id").asLong());
    assertEquals("ngram-bloom", json.path("type").asText());
    assertEquals("3", json.path("properties").path("ngram").asText());

    IndexMeta back = IndexMeta.parse(json);
    assertEquals(meta.uuid, back.uuid);
    assertEquals(meta.tableUuid, back.tableUuid);
    assertEquals("msg", back.column);
    assertEquals(3, back.ngram);
    assertEquals(0.01, back.fpp);
    assertEquals(42L, back.snapshotId);
    assertEquals(List.of("leaf-41.parquet", "leaf-42.parquet"), back.leafFiles);
    assertEquals(52, back.filesCovered);
    assertEquals(1234, back.indexBytes);
  }

  @Test
  void everyFieldTheDraftMakesRequiredIsEmitted() throws Exception {
    // format/index.md in apache/iceberg#17426 lists eight REQUIRED fields, transform-function and
    // key-column-ids among them. A document missing a required field is not conformant however
    // good the rest of it is.
    //
    // The list is written HERE, transcribed from the draft, rather than derived from kahshe's own
    // documentation. A prose summary of a spec drifts from it -- naming these two fields
    // "index-values" and "index-keys", say, which appear nowhere in the draft -- and emitting the
    // summary's names produces a document that satisfies the summary and no reader anywhere.
    IndexMeta meta = new IndexMeta();
    meta.uuid = "9c12d441-03fe-4693-9a96-a0705ddf69c1";
    meta.tableUuid = "fb072c92-a02b-11e9-ae9c-1bb7bc9eca94";
    meta.location = "s3://bucket/table/_index/ngram-bloom-f7";
    meta.column = "msg";
    meta.fieldId = 7;
    meta.ngram = 3;
    meta.fpp = 0.01;
    meta.snapshotId = 42L;

    JsonNode json = meta.toJson();
    for (String required :
        List.of(
            "format-version", "uuid", "table-uuid", "location", "type",
            "transform-function", "key-column-ids", "snapshots")) {
      assertTrue(json.has(required), "the draft makes " + required + " required, and it is missing");
    }

    // HASH, because a bloom tier is organized by hashing the key column's n-grams. IDENTITY would
    // claim the index stores the terms themselves, which is what the term dictionary does.
    assertEquals("HASH", json.path("transform-function").asText());

    // A LIST because the spec allows composite keys, holding the FIELD ID and not the column name.
    // A renamed column keeps its id; a dropped-and-re-added name is a different column entirely,
    // so keying anything about the index by column name is the same identity bug.
    assertTrue(json.path("key-column-ids").isArray());
    assertEquals(1, json.path("key-column-ids").size());
    assertEquals(7, json.path("key-column-ids").get(0).asInt());

    // and the id survives a round trip, from the spec field rather than from properties
    assertEquals(7, IndexMeta.parse(MAPPER.readTree(MAPPER.writeValueAsString(json))).fieldId);

    // kahshe does NOT claim the spec's reserved TERM type for this, and that is deliberate: TERM
    // is reserved and undefined, so writing it would claim a slot the community has not specified.
    assertEquals("ngram-bloom", json.path("type").asText());
  }

  @Test
  void readsLegacyFlatDocument() throws Exception {
    String legacy = "{\"formatVersion\":1,\"indexType\":\"ngram-bloom-v0\",\"column\":\"msg\","
        + "\"ngram\":3,\"fpp\":0.01,\"snapshotId\":7,\"leafFile\":\"leaf-7.parquet\","
        + "\"filesCovered\":50,\"dataBytes\":100,\"indexBytes\":10}";
    IndexMeta meta = IndexMeta.parse(MAPPER.readTree(legacy));
    assertEquals("msg", meta.column);
    assertEquals(7L, meta.snapshotId);
    assertEquals(List.of("leaf-7.parquet"), meta.leafFiles);
    assertEquals(50, meta.filesCovered);
    assertNull(meta.uuid);
  }

  @Test
  void readsLegacyMultiLeafDocument() throws Exception {
    String legacy = "{\"formatVersion\":1,\"column\":\"msg\",\"snapshotId\":9,"
        + "\"leafFile\":\"leaf-7.parquet\",\"leafFiles\":[\"leaf-7.parquet\",\"leaf-9.parquet\"]}";
    IndexMeta meta = IndexMeta.parse(MAPPER.readTree(legacy));
    assertEquals(List.of("leaf-7.parquet", "leaf-9.parquet"), meta.leafFiles);
  }
}
