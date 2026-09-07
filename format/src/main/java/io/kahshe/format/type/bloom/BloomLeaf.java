package io.kahshe.format.type.bloom;

import com.fasterxml.jackson.databind.ObjectMapper;
import java.io.IOException;
import java.io.UncheckedIOException;
import java.nio.ByteBuffer;
import java.util.LinkedHashMap;
import java.util.Map;
import org.apache.iceberg.Schema;
import org.apache.iceberg.Table;
import org.apache.iceberg.data.GenericRecord;
import org.apache.iceberg.data.Record;
import org.apache.iceberg.data.parquet.GenericParquetReaders;
import org.apache.iceberg.data.parquet.GenericParquetWriter;
import org.apache.iceberg.io.CloseableIterable;
import org.apache.iceberg.io.FileAppender;
import org.apache.iceberg.io.OutputFile;
import org.apache.iceberg.io.PositionOutputStream;
import org.apache.iceberg.parquet.Parquet;
import org.apache.iceberg.types.Types;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import io.kahshe.format.type.gram.Grams;

/**
 * The bloom tier's leaf and metadata IO: the leaf schema, reading a leaf back into blooms, and
 * the write that appends or compacts the leaf list and publishes the tier's metadata document.
 * The build decides what to write and passes it in.
 */
public final class BloomLeaf {
  private static final Logger LOG = LoggerFactory.getLogger(BloomLeaf.class);

  private BloomLeaf() {}

  /** One row per data file: the file's path, the column, the serialized bloom. */
  public static final Schema LEAF_SCHEMA =
      new Schema(
          Types.NestedField.required(1, "data_file_path", Types.StringType.get()),
          Types.NestedField.required(2, "column_name", Types.StringType.get()),
          Types.NestedField.required(3, "bloom", Types.BinaryType.get()));

  /**
   * How many bloom leaves a tier may accumulate before the next build rewrites them into one.
   *
   * <p>A write appends, and {@link IndexStore#load} opens every entry of the list on a cold read,
   * so an unbounded list costs one object-store round trip per leaf. Every leaf in the list is
   * referenced, so no cleaner may shrink it: past this many leaves the next build reads them all
   * back, merges them with the blooms it just built and publishes one, leaving the superseded
   * leaves for an operator's collector (docs/FORMAT.md §8.5). Bounded rather than always-on because
   * compaction re-reads the whole bloom tier, which is proportional to file count.
   */
  public static final int BLOOM_MAX_LEAVES = 8;

  /** Reads one bloom leaf's (path -> bloom) rows into {@code into}. */
  public static void read(
      org.apache.iceberg.io.FileIO io, String leaf, Map<String, NgramBloom> into, Grams.Rule rule)
      throws IOException {
    try (CloseableIterable<Record> records =
        Parquet.read(io.newInputFile(leaf))
            .project(LEAF_SCHEMA)
            .createReaderFunc(fs -> GenericParquetReaders.buildReader(LEAF_SCHEMA, fs))
            .build()) {
      for (Record record : records) {
        ByteBuffer buffer = (ByteBuffer) record.getField("bloom");
        byte[] bytes = new byte[buffer.remaining()];
        buffer.duplicate().get(bytes);
        into.put((String) record.getField("data_file_path"), NgramBloom.deserialize(bytes, rule));
      }
    }
  }

  public static IndexMeta readMeta(
      org.apache.iceberg.io.FileIO io, String indexRoot, int fieldId) {
    try {
      var metaFile = io.newInputFile(IndexMeta.metaPath(indexRoot, fieldId));
      if (!metaFile.exists()) {
        return null;
      }
      try (var in = metaFile.newStream()) {
        return IndexMeta.parse(new ObjectMapper().readTree(in));
      }
    } catch (IllegalStateException e) {
      throw e; // a newer format: refuse the build rather than rebuild over it
    } catch (IOException | RuntimeException e) {
      LOG.warn(
          "bloom index metadata for f{} exists but could not be read; treating the table as "
              + "UNINDEXED, which forces a full rebuild.",
          fieldId, e);
      return null;
    }
  }

  public static void writeMeta(org.apache.iceberg.io.FileIO io, String path, IndexMeta meta) {
    try (PositionOutputStream out = io.newOutputFile(path).createOrOverwrite()) {
      out.write(new ObjectMapper().writerWithDefaultPrettyPrinter().writeValueAsBytes(meta.toJson()));
    } catch (IOException e) {
      throw new UncheckedIOException(e);
    }
  }

  public static long write(
      Table table, org.apache.iceberg.io.FileIO io, String indexRoot, String column, int fieldId,
      long snapshotId, Map<String, NgramBloom> blooms, IndexMeta prior, String priorUuid,
      double fpp, Grams.Contract grams)
      throws IOException {
    // compact rather than append once the list is long enough: the prior leaves are unreferenced
    // from the moment the new metadata is published (see BLOOM_MAX_LEAVES)
    boolean compacted = false;
    if (prior != null && IndexStore.leaves(prior).size() + 1 > BLOOM_MAX_LEAVES) {
      Map<String, NgramBloom> merged = new LinkedHashMap<>();
      try {
        for (String leaf : IndexStore.leaves(prior)) {
          read(io, leaf, merged, Grams.Contract.of(prior.grams).rule());
        }
        // this build's blooms win: same path means same immutable file, so same content, but
        // preferring the fresher object keeps a re-indexed file's bloom authoritative
        merged.putAll(blooms);
        blooms = merged;
        compacted = true;
        LOG.info("compacting {} bloom leaf/leaves into one ({} files)",
            IndexStore.leaves(prior).size(), blooms.size());
      } catch (IOException | RuntimeException e) {
        // fall back to appending: a leaf we could not read is coverage we would silently drop,
        // and a bloom that goes missing stops pruning for that file
        LOG.warn("bloom compaction failed; appending instead and leaving the prior leaves in place",
            e);
        compacted = false;
      }
    }
    // a fresh nonce per write: see IndexMeta.leafPath for why the name must move when the
    // content does, even when the snapshot does not
    String leafPath =
        IndexMeta.leafPath(
            indexRoot, fieldId, snapshotId,
            Long.toHexString(java.util.concurrent.ThreadLocalRandom.current().nextLong() >>> 32));
    OutputFile out = io.newOutputFile(leafPath);
    long bloomBytes = 0;
    try (FileAppender<Record> appender =
        Parquet.write(out)
            .set("write.parquet.compression-codec", "zstd")
            .schema(LEAF_SCHEMA)
            .createWriterFunc(type -> GenericParquetWriter.create(LEAF_SCHEMA, type))
            .overwrite()
            .build()) {
      for (Map.Entry<String, NgramBloom> entry : blooms.entrySet()) {
        GenericRecord record = GenericRecord.create(LEAF_SCHEMA);
        record.setField("data_file_path", entry.getKey());
        record.setField("column_name", column);
        record.setField("bloom", ByteBuffer.wrap(entry.getValue().serialize()));
        bloomBytes += entry.getValue().sizeBytes();
        appender.add(record);
      }
    }

    IndexMeta meta = new IndexMeta();
    meta.uuid = priorUuid != null ? priorUuid : java.util.UUID.randomUUID().toString();
    meta.tableUuid = String.valueOf(table.uuid());
    meta.location = IndexMeta.dir(indexRoot, fieldId);
    meta.column = column;
    meta.fieldId = fieldId;
    meta.ngram = grams.size();
    meta.grams = grams.id();
    // The same resolution readFile applied when building these blooms; recorded so the artifact
    // says what it was built under. Informational — a serialized bloom carries its own geometry,
    // so a reader never needs this to probe correctly, and after a compaction folds leaves built
    // under an older setting the value describes the newest write.
    meta.fpp = fpp;
    meta.snapshotId = snapshotId;
    meta.timestampMs = System.currentTimeMillis();
    if (prior != null && !compacted) {
      meta.leafFiles.addAll(IndexStore.leaves(prior));
      meta.filesCovered = prior.filesCovered;
      meta.indexBytes = prior.indexBytes;
    }
    meta.leafFiles.add(leafPath);
    if (compacted) {
      // Recomputed, not accumulated: the single leaf now holds every file, so carrying the prior
      // running totals forward would double-count what it just absorbed.
      meta.filesCovered = blooms.size();
      meta.indexBytes = bloomBytes;
    } else {
      meta.filesCovered += blooms.size();
      meta.indexBytes += bloomBytes;
    }
    writeMeta(io, IndexMeta.metaPath(indexRoot, fieldId), meta);
    return bloomBytes;
  }
}
