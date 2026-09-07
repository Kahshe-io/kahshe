package io.kahshe.format.type.gram;

import java.io.IOException;
import java.nio.ByteBuffer;
import java.nio.charset.StandardCharsets;
import java.util.Arrays;
import java.util.Map;
import java.util.TreeMap;
import org.apache.iceberg.Schema;
import org.apache.iceberg.data.GenericRecord;
import org.apache.iceberg.data.Record;
import org.apache.iceberg.data.parquet.GenericParquetReaders;
import org.apache.iceberg.data.parquet.GenericParquetWriter;
import org.apache.iceberg.io.CloseableIterable;
import org.apache.iceberg.io.FileAppender;
import org.apache.iceberg.io.FileIO;
import org.apache.iceberg.parquet.Parquet;
import org.apache.iceberg.types.Types;
import org.roaringbitmap.RoaringBitmap;
import io.kahshe.format.type.term.TermIndexWriter;

/**
 * Writes and reads the term index's exact gram layer: one Parquet leaf per snapshot mapping every
 * lowercase character 3-gram seen in covered files to a roaring bitmap over the term index's file
 * ordinals. Rows are sorted by unsigned-lexicographic gram bytes; small row groups and pages keep
 * a future selective read range-friendly. Unlike the blooms this layer is exact over its covered
 * ordinals — an absent gram proves no covered file contains it.
 */
public final class GramIndexWriter {
  static final Schema GRAMS_SCHEMA =
      new Schema(
          Types.NestedField.required(1, "gram", Types.BinaryType.get()),
          Types.NestedField.required(2, "file_ordinals", Types.BinaryType.get()));

  /**
   * UTF-8 gram bytes as a value-equal, unsigned-ordered map key. A record with a byte[] component
   * would inherit identity equality — hence the hand-written wrapper.
   */
  public static final class ByteKey implements Comparable<ByteKey> {
    public final byte[] bytes;

    public ByteKey(byte[] bytes) {
      this.bytes = bytes;
    }

    public static ByteKey of(String gram) {
      return new ByteKey(gram.getBytes(StandardCharsets.UTF_8));
    }

    @Override
    public boolean equals(Object other) {
      return other instanceof ByteKey key && Arrays.equals(bytes, key.bytes);
    }

    @Override
    public int hashCode() {
      return Arrays.hashCode(bytes);
    }

    @Override
    public int compareTo(ByteKey other) {
      return Arrays.compareUnsigned(bytes, other.bytes);
    }
  }

  /** What the metadata records about a written grams leaf ({@code gram-coverage}). */
  public record Coverage(String leafPath, int fromOrdinal, long grams, long bytes) {}

  private GramIndexWriter() {}

  /** Named with a per-write nonce, like the bloom and aggregate leaves: a rewrite never reuses a path. */
  static String leafPath(String indexRoot, int fieldId, long snapshotId, String nonce) {
    return TermIndexWriter.dir(indexRoot, fieldId) + "/grams-" + snapshotId + "-" + nonce + ".parquet";
  }

  public static Coverage writeLeaf(
      FileIO io,
      String indexRoot,
      int fieldId,
      long snapshotId,
      int fromOrdinal,
      Map<ByteKey, RoaringBitmap> grams)
      throws IOException {
    String path =
        leafPath(
            indexRoot, fieldId, snapshotId,
            Long.toHexString(java.util.concurrent.ThreadLocalRandom.current().nextLong() >>> 32));
    try (FileAppender<Record> out =
        Parquet.write(io.newOutputFile(path))
            .set("write.parquet.compression-codec", "zstd")
            .schema(GRAMS_SCHEMA)
            .createWriterFunc(type -> GenericParquetWriter.create(GRAMS_SCHEMA, type))
            .set("write.parquet.row-group-size-bytes", "1048576")
            .set("write.parquet.page-size-bytes", "65536")
            .overwrite()
            .build()) {
      for (Map.Entry<ByteKey, RoaringBitmap> entry : new TreeMap<>(grams).entrySet()) {
        GenericRecord record = GenericRecord.create(GRAMS_SCHEMA);
        record.setField("gram", ByteBuffer.wrap(entry.getKey().bytes));
        record.setField("file_ordinals", ByteBuffer.wrap(TermIndexWriter.serialize(entry.getValue())));
        out.add(record);
      }
    }
    return new Coverage(path, fromOrdinal, grams.size(), io.newInputFile(path).getLength());
  }

  /** Streams a grams leaf into {@code into}; equal keys merge by bitmap union (widening is safe). */
  public static void readLeaf(FileIO io, String path, Map<ByteKey, RoaringBitmap> into) throws IOException {
    try (CloseableIterable<Record> records =
        Parquet.read(io.newInputFile(path))
            .project(GRAMS_SCHEMA)
            .createReaderFunc(fs -> GenericParquetReaders.buildReader(GRAMS_SCHEMA, fs))
            .build()) {
      for (Record record : records) {
        ByteBuffer gramBuffer = (ByteBuffer) record.getField("gram");
        byte[] gram = new byte[gramBuffer.remaining()];
        gramBuffer.duplicate().get(gram);
        ByteBuffer ordinalsBuffer = (ByteBuffer) record.getField("file_ordinals");
        RoaringBitmap ordinals = new RoaringBitmap();
        ordinals.deserialize(ordinalsBuffer.duplicate());
        into.merge(
            new ByteKey(gram),
            ordinals,
            (existing, loaded) -> {
              existing.or(loaded);
              return existing;
            });
      }
    }
  }
}
