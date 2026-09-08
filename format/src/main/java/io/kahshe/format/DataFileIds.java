package io.kahshe.format;

import java.io.EOFException;
import java.io.IOException;
import java.nio.ByteBuffer;
import org.apache.iceberg.Table;
import org.apache.iceberg.mapping.NameMapping;
import org.apache.iceberg.mapping.NameMappingParser;
import org.apache.iceberg.parquet.Parquet;
import org.apache.iceberg.parquet.ParquetSchemaUtil;
import org.apache.parquet.hadoop.ParquetFileReader;

/**
 * The name mapping a data file without field ids is read under, and why a file with neither is
 * refused.
 *
 * <p>Iceberg resolves a Parquet file's columns by the field ids in the file; failing that by the
 * table's {@code schema.name-mapping.default}; failing that by column POSITION. A table built by
 * {@code add_files} or a Hive {@code migrate} keeps its original Parquet, which carries no ids, so
 * it relies on the mapping — and a reader that skips it falls through to positions, which equal
 * field ids only for a schema that has never dropped, reordered, or added a column out of order.
 * Where they differ the build publishes one column's tokens under another's id, the index covers
 * every file, and a probe prunes exactly the files that match. A wrong answer with no error is the
 * outcome the format exists to prevent, so the caller refuses a file with neither ids nor a
 * mapping rather than let the fallback run.
 */
public final class DataFileIds {

  /** Iceberg's own property; the name is the spec's, not kahshe's. */
  public static final String NAME_MAPPING = "schema.name-mapping.default";

  private DataFileIds() {}

  /** The table's name mapping, or null when it declares none. */
  public static NameMapping mappingOf(Table table) {
    String json = table.properties() == null ? null : table.properties().get(NAME_MAPPING);
    if (json == null || json.isBlank()) {
      return null;
    }
    try {
      return NameMappingParser.fromJson(json);
    } catch (RuntimeException e) {
      throw new IllegalStateException(
          "the table's " + NAME_MAPPING + " could not be parsed, so a data file without field ids "
              + "cannot be resolved by name and would be read by COLUMN POSITION instead — which "
              + "silently indexes the wrong column when a schema has ever dropped or reordered "
              + "one. Fix the property, or rewrite the files with field ids", e);
    }
  }

  /**
   * Applies the mapping to a read that needs it.
   *
   * <p>Passing it is harmless when the file already carries ids: Iceberg prefers the file's own.
   */
  public static <T> Parquet.ReadBuilder withMapping(Parquet.ReadBuilder builder, NameMapping mapping) {
    return mapping == null ? builder : builder.withNameMapping(mapping);
  }

  /**
   * Refuses a data file that carries no field ids when the table declares no mapping to resolve it
   * by name, because Iceberg would otherwise resolve it by column POSITION.
   *
   * <p>Reads the file's footer only when {@code mapping} is null, so a table that declares a
   * mapping pays nothing: the mapping resolves the file whether or not it carries ids. A table
   * without one pays one footer read per file, which is noise beside the column scan that follows.
   *
   * <p>The condition tested is every primitive LEAF carrying a field id, which is stricter than
   * Iceberg's {@code ParquetSchemaUtil.hasIds} and deliberately so: that is an ANY test, returning
   * true as soon as one id appears anywhere, so a file whose outer group is identified and whose
   * leaves are not passes it. Iceberg does not fail on such a file either — {@code convert}
   * invents an id for the id-less leaf and {@code convertAndPrune}, which a read schema is built
   * from, drops the column outright. A dropped column reads as null, a null contributes no terms,
   * and the file is still covered, so a probe for a term it holds finds it absent and prunes it.
   *
   * <p>Only leaves are required to carry ids. Parquet's three-level list and map encodings put a
   * synthetic REPEATED group between the field and its element, and Iceberg does not identify
   * those — measured against an Iceberg-written {@code list<string>} and
   * {@code map<string,string>}, where {@code tags.list} and {@code props.key_value} carry no id
   * while every leaf under them does. Requiring ids on groups would refuse correct files.
   *
   * <p>It throws rather than skipping the file. Skipping is the better behaviour and is not built:
   * a skipped file must land OUTSIDE coverage so the pruner keeps it, and ordinals are assigned by
   * position in the build's wave before the read, so leaving one out is a change to coverage
   * allocation rather than to this method. Failing the build leaves the previous generation
   * serving and the table correct, which is the safe direction until that exists.
   */
  public static void requireResolvable(org.apache.iceberg.io.InputFile file, NameMapping mapping) {
    if (mapping != null) {
      return;
    }
    String unidentified;
    boolean anyId;
    try (ParquetFileReader reader = ParquetFileReader.open(new ParquetInput(file))) {
      org.apache.parquet.schema.MessageType schema = reader.getFileMetaData().getSchema();
      anyId = ParquetSchemaUtil.hasIds(schema);
      unidentified = firstLeafWithoutId(schema, "");
    } catch (IOException e) {
      throw new java.io.UncheckedIOException(
          "could not read the Parquet footer of " + file.location()
              + " to check whether it carries field ids", e);
    }
    if (unidentified == null) {
      return;
    }
    // The two populations fail for different reasons and an operator fixes them differently, so
    // they are named apart: a file with no ids at all is what add_files and a Hive migrate leave,
    // while a file with some is a writer that identified its groups and not its leaves.
    throw new IllegalStateException(
        anyId
            ? file.location() + " carries field ids but not on every column: " + unidentified
                + " has none, and the table declares no " + NAME_MAPPING + ". Iceberg does not "
                + "fail on this -- it drops that column from the read schema, so it reads as null, "
                + "contributes nothing to the index, and leaves the file COVERED while holding "
                + "none of its values. A probe would then prune exactly the files that match. "
                + "Set " + NAME_MAPPING + ", or rewrite the files with ids on every column"
            : file.location() + " carries no field ids and the table declares no " + NAME_MAPPING
                + ", so its columns would be resolved by POSITION -- which reads one column's values "
                + "and publishes them under another column's id whenever the schema has ever dropped, "
                + "reordered, or added a column out of order. The index would then cover the file "
                + "while holding the wrong values, and a probe would prune exactly the files that "
                + "match. Set " + NAME_MAPPING + " on the table, or rewrite the files with field ids");
  }

  /**
   * The dotted path of the first primitive leaf carrying no field id, or null when every leaf has
   * one. Groups are not required to: see {@link #requireResolvable}.
   */
  private static String firstLeafWithoutId(org.apache.parquet.schema.GroupType group, String at) {
    for (org.apache.parquet.schema.Type field : group.getFields()) {
      String path = at.isEmpty() ? field.getName() : at + "." + field.getName();
      if (field.isPrimitive()) {
        if (field.getId() == null) {
          return path;
        }
      } else {
        String deeper = firstLeafWithoutId(field.asGroupType(), path);
        if (deeper != null) {
          return deeper;
        }
      }
    }
    return null;
  }

  /** Iceberg's InputFile as Parquet's; the same adapter Iceberg keeps package-private. */
  private record ParquetInput(org.apache.iceberg.io.InputFile file)
      implements org.apache.parquet.io.InputFile {
    @Override
    public long getLength() {
      return file.getLength();
    }

    @Override
    public org.apache.parquet.io.SeekableInputStream newStream() {
      return new ParquetStream(file.newStream());
    }
  }

  /** Iceberg's SeekableInputStream as Parquet's, which adds the readFully and ByteBuffer forms. */
  private static final class ParquetStream extends org.apache.parquet.io.SeekableInputStream {
    private final org.apache.iceberg.io.SeekableInputStream in;

    ParquetStream(org.apache.iceberg.io.SeekableInputStream in) {
      this.in = in;
    }

    @Override
    public long getPos() throws IOException {
      return in.getPos();
    }

    @Override
    public void seek(long pos) throws IOException {
      in.seek(pos);
    }

    @Override
    public int read() throws IOException {
      return in.read();
    }

    @Override
    public int read(byte[] b, int off, int len) throws IOException {
      return in.read(b, off, len);
    }

    @Override
    public void readFully(byte[] b) throws IOException {
      readFully(b, 0, b.length);
    }

    @Override
    public void readFully(byte[] b, int off, int len) throws IOException {
      int done = 0;
      while (done < len) {
        int n = in.read(b, off + done, len - done);
        if (n < 0) {
          throw new EOFException("wanted " + len + " bytes, got " + done);
        }
        done += n;
      }
    }

    @Override
    public int read(ByteBuffer buf) throws IOException {
      byte[] tmp = new byte[buf.remaining()];
      int n = in.read(tmp, 0, tmp.length);
      if (n > 0) {
        buf.put(tmp, 0, n);
      }
      return n;
    }

    @Override
    public void readFully(ByteBuffer buf) throws IOException {
      byte[] tmp = new byte[buf.remaining()];
      readFully(tmp, 0, tmp.length);
      buf.put(tmp);
    }

    @Override
    public void close() throws IOException {
      in.close();
    }
  }
}
