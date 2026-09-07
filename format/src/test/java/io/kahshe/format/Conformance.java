package io.kahshe.format;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.node.ObjectNode;
import java.io.IOException;
import java.io.UncheckedIOException;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.ArrayList;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.TreeMap;
import java.util.stream.Stream;
import org.apache.iceberg.Table;
import org.apache.iceberg.data.Record;
import org.apache.iceberg.data.parquet.GenericParquetReaders;
import org.apache.iceberg.io.CloseableIterable;
import org.apache.iceberg.io.FileIO;
import org.apache.iceberg.parquet.Parquet;
import org.roaringbitmap.RoaringBitmap;
import io.kahshe.format.type.bloom.BloomLeaf;
import io.kahshe.format.type.bloom.IndexMeta;
import io.kahshe.format.type.bloom.IndexStore;
import io.kahshe.format.type.bloom.NgramBloom;
import io.kahshe.format.type.gram.GramIndexWriter;
import io.kahshe.format.type.gram.Grams;
import io.kahshe.format.type.term.TermIndexWriter;

/**
 * The conformance fixture: a golden index cut once and committed under
 * {@code src/test/resources/conformance/<version>}, and the two operations every later change
 * is held to. {@link #export} writes an artifact into the fixture with its volatile parts
 * replaced by placeholders (the index root, the table location and uuid, the snapshot id, the
 * document uuid, timestamps, leaf nonces); {@link #materialize} puts one back at a real root for
 * a real table; {@link #snapshot} decodes an artifact into content a second implementation can
 * be compared on -- Parquet bytes are never compared, only what they decode to.
 */
final class Conformance {
  static final Path FIXTURE = Path.of("src/test/resources/conformance/v1");
  /**
   * Escapes non-ASCII: the v1 gram vectors hold lone surrogate halves, which UTF-8 cannot encode,
   * and an ASCII manifest with {@code \\u} escapes is the more portable shape anyway.
   */
  static final ObjectMapper MAPPER = new ObjectMapper()
      .enable(com.fasterxml.jackson.core.json.JsonWriteFeature.ESCAPE_NON_ASCII.mappedFeature());
  static final String ROOT = "INDEXROOT";
  static final String TABLE = "TABLELOCATION";
  static final String TABLE_UUID = "TABLEUUID";
  static final String SNAP = "SNAP";
  static final String NONCE = "NONCE";
  static final String UUID = "DOCUUID";
  static final String TS = "TIMESTAMP";
  static final String BUILD_ID = "BUILDID";

  private Conformance() {}

  /** The index directory of {@code table}'s column under {@code indexRoot}, as the build names it. */
  static String indexDir(Table table, String indexRoot) {
    return IndexPaths.root(table, indexRoot);
  }

  /** The substitutions a real table and root give the placeholders. */
  static Map<String, String> bindings(Table table, String indexRoot) {
    Map<String, String> b = new LinkedHashMap<>();
    b.put(ROOT, indexDir(table, indexRoot));
    b.put(TABLE, table.location());
    b.put(TABLE_UUID, String.valueOf(table.uuid()));
    b.put(SNAP, String.valueOf(table.currentSnapshot().snapshotId()));
    return b;
  }

  /** Replaces every bound value in {@code text} by its placeholder (export) or the reverse (materialize). */
  static String swap(String text, Map<String, String> bindings, boolean toPlaceholder) {
    String out = text;
    // longest values first, so a root that contains the table location is swapped whole
    List<Map.Entry<String, String>> entries = new ArrayList<>(bindings.entrySet());
    entries.sort((x, y) -> Integer.compare(y.getValue().length(), x.getValue().length()));
    for (Map.Entry<String, String> e : entries) {
      out = toPlaceholder ? out.replace(e.getValue(), e.getKey()) : out.replace(e.getKey(), e.getValue());
    }
    return out;
  }

  /** The two documents' random and clock-driven fields, replaced by placeholders. */
  static JsonNode normalizeDocument(JsonNode doc, Map<String, String> bindings) {
    try {
      // the snapshot id is a NUMBER in the documents; as a placeholder it must be a string
      ObjectNode copy = doc.deepCopy();
      long snap = Long.parseLong(bindings.get(SNAP));
      if (copy.path("snapshotId").asLong() == snap) {
        copy.put("snapshotId", SNAP);
      }
      for (JsonNode s : copy.path("snapshots")) {
        if (s instanceof ObjectNode o) {
          for (String key : List.of("source-table-snapshot-id", "snapshotId")) {
            if (o.path(key).asLong() == snap) {
              o.put(key, SNAP);
            }
          }
        }
      }
      String text = swap(MAPPER.writeValueAsString(copy), bindings, true);
      ObjectNode out = (ObjectNode) MAPPER.readTree(text);
      for (String key : List.of("uuid", "timestamp-ms", "timestampMs")) {
        if (out.has(key)) {
          out.put(key, key.equals("uuid") ? UUID : TS);
        }
      }
      JsonNode snapshots = out.path("snapshots");
      if (snapshots.isArray()) {
        for (JsonNode s : snapshots) {
          if (s instanceof ObjectNode o) {
            if (o.has("timestamp-ms")) {
              o.put("timestamp-ms", TS);
            }
          }
        }
      }
      // leaf names carry a per-write nonce
      String again = MAPPER.writeValueAsString(out)
          .replaceAll("(aggregate-" + SNAP + "-r\\d\\d-)[0-9a-f]+\\.parquet", "$1" + NONCE + ".parquet")
          .replaceAll("(grams-" + SNAP + "-)[0-9a-f]+\\.parquet", "$1" + NONCE + ".parquet")
          .replaceAll("(leaf-" + SNAP + "-)[0-9a-f]+\\.parquet", "$1" + NONCE + ".parquet");
      return MAPPER.readTree(again);
    } catch (IOException e) {
      throw new UncheckedIOException(e);
    }
  }

  /**
   * The build report's volatile parts: its own id, the clock fields, the timer, the paths; the
   * previous id is absent on the fixture's single build and stays so.
   */
  static JsonNode normalizeReport(JsonNode doc, Map<String, String> bindings) {
    try {
      ObjectNode copy = doc.deepCopy();
      if (copy.path("snapshot-id").asLong() == Long.parseLong(bindings.get(SNAP))) {
        copy.put("snapshot-id", SNAP);
      }
      ObjectNode metadata = (ObjectNode) copy.path("metadata");
      metadata.put("build-id", BUILD_ID);
      metadata.remove("previous-build-id");
      metadata.put("started-ms", TS);
      metadata.put("published-ms", TS);
      ((ObjectNode) copy.path("metrics").path("build")).put("total-duration", 0);
      String text = swap(MAPPER.writeValueAsString(copy), bindings, true)
          .replaceAll("(aggregate-" + SNAP + "-r\\d\\d-)[0-9a-f]+\\.parquet", "$1" + NONCE + ".parquet")
          .replaceAll("(grams-" + SNAP + "-)[0-9a-f]+\\.parquet", "$1" + NONCE + ".parquet");
      return MAPPER.readTree(text);
    } catch (IOException e) {
      throw new UncheckedIOException(e);
    }
  }

  /** A leaf's committed name: snapshot and nonce replaced. */
  static String normalizeName(String name, String snapshotId) {
    return name.replace(snapshotId, SNAP)
        .replaceAll("(aggregate-" + SNAP + "-r\\d\\d-)[0-9a-f]+\\.parquet", "$1" + NONCE + ".parquet")
        .replaceAll("(grams-" + SNAP + "-)[0-9a-f]+\\.parquet", "$1" + NONCE + ".parquet")
        .replaceAll("(leaf-" + SNAP + "-)[0-9a-f]+\\.parquet", "$1" + NONCE + ".parquet");
  }

  /** Writes the artifact of {@code fieldId} under {@code indexRoot} into {@code out}, normalized. */
  static void export(Table table, String indexRoot, int fieldId, Path out) throws IOException {
    Map<String, String> b = bindings(table, indexRoot);
    String dir = indexDir(table, indexRoot);
    for (String tier : List.of(TermIndexWriter.dir(dir, fieldId), IndexMeta.dir(dir, fieldId))) {
      Path src = Path.of(tier.replace("file:", ""));
      Path dst = out.resolve(src.getFileName().toString());
      Files.createDirectories(dst);
      try (Stream<Path> files = Files.list(src)) {
        for (Path f : (Iterable<Path>) files::iterator) {
          String name = f.getFileName().toString();
          if (name.startsWith(".") && name.endsWith(".crc")) {
            continue;
          }
          if (name.equals("index-metadata.json")) {
            JsonNode doc = MAPPER.readTree(Files.readString(f, StandardCharsets.UTF_8));
            Files.writeString(dst.resolve(name), MAPPER.writerWithDefaultPrettyPrinter()
                .writeValueAsString(normalizeDocument(doc, b)) + "\n", StandardCharsets.UTF_8);
          } else if (name.equals(BuildReport.FILE_NAME)) {
            JsonNode doc = MAPPER.readTree(Files.readString(f, StandardCharsets.UTF_8));
            Files.writeString(dst.resolve(name), MAPPER.writerWithDefaultPrettyPrinter()
                .writeValueAsString(normalizeReport(doc, b)) + "\n", StandardCharsets.UTF_8);
          } else if (name.startsWith("leaf-")) {
            rewriteBloomLeaf(table.io(), f.toString(), dst.resolve(normalizeName(name, b.get(SNAP))).toString(),
                path -> swap(path, b, true), b);
          } else {
            Files.copy(f, dst.resolve(normalizeName(name, b.get(SNAP))));
          }
        }
      }
    }
  }

  /**
   * The bloom leaf names each data file by path inside the Parquet, so the leaf is decoded and
   * rewritten rather than copied; the aggregate and gram leaves hold ordinals and copy whole.
   */
  static void rewriteBloomLeaf(FileIO io, String src, String dst,
      java.util.function.UnaryOperator<String> pathMap, Map<String, String> bindings) throws IOException {
    Map<String, NgramBloom> blooms = new LinkedHashMap<>();
    // the rule only shapes probes; for a copy any rule decodes the same bits
    BloomLeaf.read(io, src, blooms, Grams.Rule.CODEPOINT_V2);
    String column = null;
    try (CloseableIterable<Record> records = Parquet.read(io.newInputFile(src))
        .project(BloomLeaf.LEAF_SCHEMA)
        .createReaderFunc(fs -> GenericParquetReaders.buildReader(BloomLeaf.LEAF_SCHEMA, fs))
        .build()) {
      for (Record r : records) {
        column = (String) r.getField("column_name");
      }
    }
    Files.deleteIfExists(Path.of(dst));
    try (org.apache.iceberg.io.FileAppender<Record> appender = Parquet.write(io.newOutputFile(dst))
        .set("write.parquet.compression-codec", "zstd")
        .schema(BloomLeaf.LEAF_SCHEMA)
        .createWriterFunc(type -> org.apache.iceberg.data.parquet.GenericParquetWriter.create(BloomLeaf.LEAF_SCHEMA, type))
        .overwrite()
        .build()) {
      for (Map.Entry<String, NgramBloom> e : blooms.entrySet()) {
        org.apache.iceberg.data.GenericRecord record = org.apache.iceberg.data.GenericRecord.create(BloomLeaf.LEAF_SCHEMA);
        record.setField("data_file_path", pathMap.apply(e.getKey()));
        record.setField("column_name", column);
        record.setField("bloom", java.nio.ByteBuffer.wrap(e.getValue().serialize()));
        appender.add(record);
      }
    }
  }

  /** Puts the committed artifact at the index directory the readers will look in for {@code table}. */
  static void materialize(Path fixtureIndex, Table table, String indexRoot) throws IOException {
    Map<String, String> b = bindings(table, indexRoot);
    b.put(UUID, java.util.UUID.randomUUID().toString());
    b.put(TS, "1700000000000");
    String dir = indexDir(table, indexRoot).replace("file:", "");
    try (Stream<Path> tiers = Files.list(fixtureIndex)) {
      for (Path tier : (Iterable<Path>) tiers::iterator) {
        Path dst = Path.of(dir).resolve(tier.getFileName().toString());
        Files.createDirectories(dst);
        try (Stream<Path> files = Files.list(tier)) {
          for (Path f : (Iterable<Path>) files::iterator) {
            String name = f.getFileName().toString().replace(SNAP, b.get(SNAP)).replace(NONCE, "00c0ffee");
            if (name.equals("index-metadata.json") || name.equals(BuildReport.FILE_NAME)) {
              String text = Files.readString(f, StandardCharsets.UTF_8).replace(NONCE, "00c0ffee")
                  .replace(BUILD_ID, "b00c0ffee");
              // the snapshot id is a number in the document and a name fragment in the leaves
              text = text.replace("\"" + SNAP + "\"", b.get(SNAP)).replace(SNAP, b.get(SNAP));
              Files.writeString(dst.resolve(name), swap(text, b, false), StandardCharsets.UTF_8);
            } else if (name.startsWith("leaf-")) {
              rewriteBloomLeaf(table.io(), f.toString(), dst.resolve(name).toString(),
                  path -> swap(path, b, false), b);
            } else {
              Files.copy(f, dst.resolve(name));
            }
          }
        }
      }
    }
  }

  /** What an artifact holds, decoded: the documents normalized, every leaf's rows, bloom bytes per file. */
  static Map<String, Object> snapshot(Table table, String indexRoot, int fieldId) throws IOException {
    Map<String, String> b = bindings(table, indexRoot);
    FileIO io = table.io();
    String dir = indexDir(table, indexRoot);
    Map<String, Object> out = new TreeMap<>();
    JsonNode term = MAPPER.readTree(readAll(io, TermIndexWriter.dir(dir, fieldId) + "/index-metadata.json"));
    JsonNode bloom = MAPPER.readTree(readAll(io, IndexMeta.metaPath(dir, fieldId)));
    // compared as structure, not text: the format leaves object key order unsettled
    out.put("term-document", canonical(normalizeDocument(term, b)));
    out.put("bloom-document", canonical(normalizeDocument(bloom, b)));
    JsonNode snapshot = TermIndexWriter.snapshotNode(term);
    Map<String, List<String>> ranges = new TreeMap<>();
    List<String> leaves = TermIndexWriter.aggregateLeaves(snapshot);
    for (int r = 0; r < leaves.size(); r++) {
      String leaf = leaves.get(r);
      if (leaf.isEmpty()) {
        continue;
      }
      List<String> rows = new ArrayList<>();
      try (CloseableIterable<Record> records = Parquet.read(io.newInputFile(leaf))
          .project(TermIndexWriter.AGGREGATE_SCHEMA)
          .createReaderFunc(fs -> GenericParquetReaders.buildReader(TermIndexWriter.AGGREGATE_SCHEMA, fs))
          .build()) {
        for (Record rec : records) {
          RoaringBitmap bits = new RoaringBitmap();
          bits.deserialize(((java.nio.ByteBuffer) rec.getField("file_ordinals")).duplicate());
          rows.add(rec.getField("term") + " files=" + rec.getField("file_count") + " total="
              + rec.getField("total_count") + " ordinals=" + bits);
        }
      }
      ranges.put(String.format("r%02d", r), rows);
    }
    out.put("aggregate", ranges.toString());
    String gramsPath = snapshot.path("leaves").path("grams").asText();
    if (!gramsPath.isEmpty()) {
      Map<GramIndexWriter.ByteKey, RoaringBitmap> grams = new java.util.HashMap<>();
      GramIndexWriter.readLeaf(io, gramsPath, grams);
      Map<String, String> sorted = new TreeMap<>();
      for (Map.Entry<GramIndexWriter.ByteKey, RoaringBitmap> e : grams.entrySet()) {
        sorted.put(new String(e.getKey().bytes, StandardCharsets.UTF_8), e.getValue().toString());
      }
      out.put("grams", sorted.toString());
    }
    JsonNode report = MAPPER.readTree(readAll(io, BuildReport.path(dir, fieldId)));
    out.put("build-report", canonical(normalizeReport(report, b)));
    IndexMeta meta = IndexMeta.parse(bloom);
    Map<String, NgramBloom> blooms = new java.util.HashMap<>();
    for (String leaf : IndexStore.leaves(meta)) {
      BloomLeaf.read(io, leaf, blooms, Grams.Contract.of(meta.grams).rule());
    }
    Map<String, String> byFile = new TreeMap<>();
    for (Map.Entry<String, NgramBloom> e : blooms.entrySet()) {
      byFile.put(swap(e.getKey(), b, true), java.util.HexFormat.of().formatHex(e.getValue().serialize()));
    }
    out.put("blooms", byFile.toString());
    return out;
  }

  /** The document with every object's keys sorted, so key order never counts as a difference. */
  static String canonical(JsonNode doc) throws IOException {
    Object tree = MAPPER.treeToValue(doc, Object.class);
    return MAPPER.writer(com.fasterxml.jackson.databind.SerializationFeature.ORDER_MAP_ENTRIES_BY_KEYS)
        .writeValueAsString(tree);
  }

  private static String readAll(FileIO io, String path) throws IOException {
    try (java.io.InputStream in = io.newInputFile(path).newStream()) {
      return new String(in.readAllBytes(), StandardCharsets.UTF_8);
    }
  }

  /** The corpus the fixture is cut from, as committed in {@code rows.json}. */
  /**
   * The fixture's list column, per file: one entry per row, each the row's members, or null for a
   * row whose container is absent. An empty list stays empty — a present container with no member,
   * which contributes no term exactly as an absent one does.
   */
  static List<Map.Entry<String, List<List<String>>>> listRows() throws IOException {
    JsonNode rows = MAPPER.readTree(Files.readString(FIXTURE.resolve("rows.json"), StandardCharsets.UTF_8));
    List<Map.Entry<String, List<List<String>>>> out = new ArrayList<>();
    for (JsonNode file : rows.path("files")) {
      List<List<String>> perRow = new ArrayList<>();
      for (JsonNode row : file.path("tags")) {
        if (row.isNull()) {
          perRow.add(null);
          continue;
        }
        List<String> members = new ArrayList<>();
        row.forEach(v -> members.add(v.asText()));
        perRow.add(members);
      }
      out.add(Map.entry(file.path("name").asText(), perRow));
    }
    return out;
  }

  /** The column names and the list column's analyzer, as the fixture declares them. */
  static String property(String key) throws IOException {
    return MAPPER.readTree(Files.readString(FIXTURE.resolve("rows.json"), StandardCharsets.UTF_8))
        .path(key).asText();
  }

  static List<Map.Entry<String, List<String>>> rows() throws IOException {
    JsonNode rows = MAPPER.readTree(Files.readString(FIXTURE.resolve("rows.json"), StandardCharsets.UTF_8));
    List<Map.Entry<String, List<String>>> out = new ArrayList<>();
    for (JsonNode file : rows.path("files")) {
      List<String> values = new ArrayList<>();
      file.path("rows").forEach(v -> values.add(v.asText()));
      out.add(Map.entry(file.path("name").asText(), values));
    }
    return out;
  }
}
