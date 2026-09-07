package io.kahshe.format.type.bloom;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.node.ArrayNode;
import com.fasterxml.jackson.databind.node.JsonNodeFactory;
import com.fasterxml.jackson.databind.node.ObjectNode;
import java.util.ArrayList;
import java.util.List;
import io.kahshe.format.type.gram.Grams;
import io.kahshe.format.type.term.TermIndexWriter;

/**
 * Bloom index metadata JSON, stored beside the leaf files. The document uses the draft Iceberg
 * index spec's vocabulary (apache/iceberg#16961: {@code format-version}, {@code uuid},
 * {@code table-uuid}, {@code location}, {@code type}, {@code snapshots} with
 * {@code source-table-snapshot-id}), so migration to the ratified model is mechanical; kahshe keeps
 * only the latest index snapshot and collapses the spec's tracking file into an inline
 * {@code leaf-files} list. Legacy (pre-vocabulary) documents are still read. The full field list
 * and every divergence from the draft are in docs/FORMAT.md §3 and §9.
 */
public class IndexMeta {
  /** The bloom tier's on-disk format version; see {@code TermIndexWriter.FORMAT_VERSION}. */
  public static final int FORMAT_VERSION = 1;

  public int formatVersion = FORMAT_VERSION;
  public String uuid;
  public String tableUuid;
  public String location;
  public String indexType = "ngram-bloom";
  public String column;

  /**
   * The Iceberg field ID of the indexed column, which is what the spec's {@code key-column-ids}
   * carries. The column name lives in properties for humans; the id is the identity, because a
   * column can be renamed and a dropped-then-re-added name is a different column entirely.
   */
  public int fieldId;

  public int ngram;
  public double fpp;
  /** The gram rule id (Grams.Contract); null in a document written before the rule had one: v1. */
  public String grams;
  public long snapshotId; // source-table-snapshot-id
  public long timestampMs;
  public List<String> leafFiles = new ArrayList<>();
  public int filesCovered;
  public long dataBytes;
  public long indexBytes;

  public static IndexMeta parse(JsonNode root) {
    IndexMeta meta = new IndexMeta();
    if (root.has("snapshots")) {
      meta.formatVersion = root.path("format-version").asInt(1);
      if (meta.formatVersion > FORMAT_VERSION) {
        // Newer than this reader. Thrown rather than nulled: the read path catches it and keeps
        // every file, and the build path lets it through so an older kahshe never builds over a
        // newer index -- which would downgrade it in place.
        throw new IllegalStateException(
            "bloom index format-version " + meta.formatVersion + " is newer than this kahshe's "
                + FORMAT_VERSION + "; refusing to read or overwrite it");
      }
      meta.uuid = root.path("uuid").asText(null);
      meta.tableUuid = root.path("table-uuid").asText(null);
      meta.location = root.path("location").asText(null);
      meta.indexType = root.path("type").asText("ngram-bloom");
      JsonNode properties = root.path("properties");
      meta.column = properties.path("column").asText(null);
      // key-column-ids is the spec field; properties.field-id is where older documents kept it
      JsonNode keyColumns = root.path("key-column-ids");
      meta.fieldId =
          keyColumns.isArray() && !keyColumns.isEmpty()
              ? keyColumns.get(0).asInt()
              : Integer.parseInt(properties.path("field-id").asText("0"));
      meta.ngram = Integer.parseInt(properties.path("ngram").asText("0"));
      meta.grams = properties.path("grams").asText(null);
      meta.fpp = Double.parseDouble(properties.path("fpp").asText("0"));
      JsonNode snapshot = root.path("snapshots").path(0);
      meta.snapshotId = snapshot.path("source-table-snapshot-id").asLong();
      meta.timestampMs = snapshot.path("timestamp-ms").asLong();
      snapshot.path("leaf-files").forEach(leaf -> meta.leafFiles.add(leaf.asText()));
      meta.filesCovered = snapshot.path("files-covered").asInt();
      meta.dataBytes = snapshot.path("data-bytes").asLong();
      meta.indexBytes = snapshot.path("index-bytes").asLong();
      return meta;
    }
    // legacy flat document
    meta.formatVersion = root.path("formatVersion").asInt(1);
    meta.indexType = root.path("indexType").asText("ngram-bloom-v0");
    meta.column = root.path("column").asText(null);
    meta.ngram = root.path("ngram").asInt();
    meta.fpp = root.path("fpp").asDouble();
    meta.snapshotId = root.path("snapshotId").asLong();
    if (root.hasNonNull("leafFiles")) {
      root.path("leafFiles").forEach(leaf -> meta.leafFiles.add(leaf.asText()));
    } else if (root.hasNonNull("leafFile")) {
      meta.leafFiles.add(root.path("leafFile").asText());
    }
    meta.filesCovered = root.path("filesCovered").asInt();
    meta.dataBytes = root.path("dataBytes").asLong();
    meta.indexBytes = root.path("indexBytes").asLong();
    return meta;
  }

  public ObjectNode toJson() {
    JsonNodeFactory nodes = JsonNodeFactory.instance;
    ObjectNode root = nodes.objectNode();
    root.put("format-version", formatVersion);
    root.put("uuid", uuid);
    root.put("table-uuid", tableUuid);
    root.put("location", location);
    root.put("type", indexType);
    // Required by the draft (format/index.md, apache/iceberg#17426). HASH is the honest one for a
    // bloom tier: the index is organized by hashing n-grams of the key column, not by storing them.
    root.put("transform-function", "HASH");
    // Required, and a list because the spec allows composite keys. kahshe declares exactly one
    // column per index, so it is always a single element.
    root.putArray("key-column-ids").add(fieldId);
    ObjectNode properties = root.putObject("properties");
    properties.put("column", column);
    properties.put("field-id", String.valueOf(fieldId));
    properties.put("ngram", String.valueOf(ngram));
    if (grams != null) {
      properties.put("grams", grams);
    }
    properties.put("fpp", String.valueOf(fpp));
    ObjectNode snapshot = nodes.objectNode();
    snapshot.put("snapshot-id", 1);
    snapshot.put("source-table-snapshot-id", snapshotId);
    snapshot.put("timestamp-ms", timestampMs);
    ArrayNode leaves = snapshot.putArray("leaf-files");
    leafFiles.forEach(leaves::add);
    snapshot.put("files-covered", filesCovered);
    snapshot.put("data-bytes", dataBytes);
    snapshot.put("index-bytes", indexBytes);
    root.putArray("snapshots").add(snapshot);
    return root;
  }

  /**
   * The bloom tier's directory, keyed by Iceberg field id — never by column name.
   *
   * <p>A name is not a stable identity in Iceberg: rename a column and the name moves; drop it and
   * re-add it and a different field wears the old label. Blooms are keyed inside by data-file path,
   * which survives both, so a name-keyed directory would hand the new column the old column's
   * blooms and prune files that genuinely match. Field ids are assigned once and never reused.
   *
   * <p>Artifacts left under an older name-keyed path are simply not found: no index means every
   * file is kept, and the next build writes the field-keyed one.
   */
  public static String dir(String indexRoot, int fieldId) {
    return indexRoot + "/ngram-bloom-f" + fieldId;
  }

  public static String metaPath(String indexRoot, int fieldId) {
    return dir(indexRoot, fieldId) + "/index-metadata.json";
  }

  /**
   * A leaf's path. The nonce makes each write a new object rather than an overwrite, which is
   * load-bearing twice over.
   *
   * <p>Staleness: {@code IndexStore} revalidates from a fingerprint of (snapshot id, leaf files) and
   * the index uuid is stable across rebuilds, so without the nonce a rebuild at the same snapshot
   * would fingerprint identically and a reader holding the old index would never reload.
   * Clobbering: two builds racing on one snapshot would otherwise write the same object; distinct
   * names leave the loser's leaf an orphan rather than a half-overwritten file a reader is mid-way
   * through.
   */
  public static String leafPath(String indexRoot, int fieldId, long snapshotId, String nonce) {
    return dir(indexRoot, fieldId) + "/leaf-" + snapshotId + "-" + nonce + ".parquet";
  }
}
