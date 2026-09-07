package io.kahshe.format.type.term;

import java.io.Closeable;
import java.io.EOFException;
import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.StandardOpenOption;

/**
 * The on-disk sorted run: {@code (term, ordinal, count)} per record, ascending by term as unsigned
 * UTF-8 bytes then by ordinal, terminated explicitly.
 *
 * <p>Ordinals stay plain integers the whole way to the final merge, where a term's complete ordinal
 * set is known and one {@link org.roaringbitmap.RoaringBitmap} is built; no bitmap is ever
 * serialized into a run. Unsigned byte order, rather than {@code String.compareTo}, is the order
 * Parquet's {@code STRING} statistics comparator and Iceberg's {@code Comparators.charSequences()}
 * use, and {@link TermRanges} is monotonic in it too.
 */
final class TermRun {

  private static final byte ROW_PRESENT = 1;
  private static final byte ROW_END = 0;

  private TermRun() {}

  /**
   * Compares two terms as unsigned UTF-8 bytes. Java's {@code byte} is signed, so a naive
   * {@code a[i] - b[i]} would order every byte at or above 0x80 below every ASCII byte — putting
   * "é" before "a" the first time the term space widens past what the analyzer emits today.
   */
  static int compare(byte[] a, int aFrom, int aLen, byte[] b, int bFrom, int bLen) {
    int n = Math.min(aLen, bLen);
    for (int i = 0; i < n; i++) {
      int x = a[aFrom + i] & 0xFF;
      int y = b[bFrom + i] & 0xFF;
      if (x != y) {
        return x - y;
      }
    }
    return aLen - bLen;
  }

  static int compare(byte[] a, byte[] b) {
    return compare(a, 0, a.length, b, 0, b.length);
  }

  /**
   * Writes one run, asserting as it goes that what it is handed ascends. The merge trusts every
   * cursor to be ascending and has no check of its own, and that order now comes from a
   * hand-written byte comparator over a packed arena: feed the merge an unsorted stream and it
   * drops rows, which is a file wrongly pruned. One comparison per row makes that a build failure.
   */
  static final class Writer implements Closeable {
    private final OutputStream out;
    /** Reused across rows; {@link #lastTermLength} says how much of it is live. */
    private byte[] lastTerm = null;
    private int lastTermLength = -1;
    private int lastOrdinal = -1;
    private long rows;
    /** Bytes handed to the stream so far; with {@link #position} this is the current offset. */
    private long flushed;
    /** Offset of each range's first record, -1 when the run holds none of that range. */
    private final long[] rangeStart = new long[TermRanges.COUNT];
    private int currentRange = -1;
    private long endOfRows = -1;

    {
      java.util.Arrays.fill(rangeStart, -1L);
    }

    /** One filesystem write per 64 KiB rather than a synchronized call per byte. */
    private static final int BUFFER_BYTES = 1 << 16;

    /** Longest varint this format can emit: 64 bits at 7 bits a byte. */
    private static final int MAX_VARINT_BYTES = 10;

    private final byte[] buffer = new byte[BUFFER_BYTES];
    private int position;

    Writer(Path file) throws IOException {
      // CREATE_NEW, not the default CREATE + TRUNCATE_EXISTING: a name collision between two
      // readers must fail the build, not silently destroy a finished run.
      // No BufferedOutputStream: this class buffers into its own array, and stacking the two
      // would put back the per-byte synchronized call that is the whole cost being removed.
      this.out =
          Files.newOutputStream(file, StandardOpenOption.CREATE_NEW, StandardOpenOption.WRITE);
    }

    void append(byte[] term, int from, int len, int ordinal, long count) throws IOException {
      if (lastTermLength >= 0) {
        int cmp = compare(lastTerm, 0, lastTermLength, term, from, len);
        if (cmp > 0 || (cmp == 0 && ordinal < lastOrdinal)) {
          throw new IOException(
              "term run written out of order: ["
                  + new String(lastTerm, 0, lastTermLength, StandardCharsets.UTF_8)
                  + "/"
                  + lastOrdinal
                  + "] then ["
                  + new String(term, from, len, StandardCharsets.UTF_8)
                  + "/"
                  + ordinal
                  + "]");
        }
      }
      // Front coded against the previous term, which is already held for the assertion above.
      // This format needs no version negotiation because a run never outlives the build that wrote
      // it: TermRunStore sweeps the scratch directory, and no resume path can hand a run from one
      // build to a reader from another.
      //
      // At a range boundary the first record is written in full and its offset recorded, so a
      // cursor opened there has no previous term to code against. Terms in different ranges differ
      // in their first character, so the shared prefix across a boundary is zero anyway; the
      // explicit `!boundary` below says so rather than relying on it. Ranges arrive in order
      // because the rows are sorted and TermRanges.of is monotonic.
      int range = TermRanges.of(term, from, len);
      boolean boundary = range != currentRange;
      if (boundary) {
        rangeStart[range] = flushed + position;
        currentRange = range;
      }
      int shared = 0;
      if (!boundary && lastTermLength >= 0) {
        int max = Math.min(lastTermLength, len);
        while (shared < max && lastTerm[shared] == term[from + shared]) {
          shared++;
        }
      }
      // one bounds check for the whole fixed-width part, then straight into the array
      if (position + 1 + 4 * MAX_VARINT_BYTES > buffer.length) {
        drain();
      }
      buffer[position++] = ROW_PRESENT;
      putVarint(shared);
      putVarint(len - shared);
      writeBytes(term, from + shared, len - shared);
      putVarint(Integer.toUnsignedLong(ordinal));
      putVarint(count);

      // Reused, not reallocated: a copy per row would be billions of small allocations across the
      // flush, the cascade and the final merge, to remember a value the next call overwrites.
      if (lastTerm == null || lastTerm.length < len) {
        lastTerm = new byte[Math.max(len, 64)];
      }
      System.arraycopy(term, from, lastTerm, 0, len);
      lastTermLength = len;
      lastOrdinal = ordinal;
      rows++;
    }

    long rows() {
      return rows;
    }

    /**
     * After {@link #close}: the byte bounds of each range, {@code [bounds[r], bounds[r + 1])}, over
     * the run's rows; an empty slice means the run holds no row of that range. The final bound is
     * the offset of the end marker, so no slice ever includes it.
     */
    long[] rangeOffsets() {
      if (endOfRows < 0) {
        throw new IllegalStateException("range offsets are known only after close()");
      }
      long[] bounds = new long[TermRanges.COUNT + 1];
      bounds[TermRanges.COUNT] = endOfRows;
      for (int r = TermRanges.COUNT - 1; r >= 0; r--) {
        bounds[r] = rangeStart[r] >= 0 ? rangeStart[r] : bounds[r + 1];
      }
      return bounds;
    }

    /** Appends a varint. The caller guarantees room, or calls {@link #drain} first. */
    private void putVarint(long value) throws IOException {
      if (position + MAX_VARINT_BYTES > buffer.length) {
        drain();
      }
      long remaining = value;
      while ((remaining & ~0x7FL) != 0) {
        buffer[position++] = (byte) ((remaining & 0x7F) | 0x80);
        remaining >>>= 7;
      }
      buffer[position++] = (byte) remaining;
    }

    /** Appends raw bytes, going straight to the stream for anything the buffer cannot hold. */
    private void writeBytes(byte[] source, int from, int len) throws IOException {
      if (len >= buffer.length) {
        drain();
        out.write(source, from, len);
        flushed += len;
        return;
      }
      if (position + len > buffer.length) {
        drain();
      }
      System.arraycopy(source, from, buffer, position, len);
      position += len;
    }

    private void drain() throws IOException {
      if (position > 0) {
        out.write(buffer, 0, position);
        flushed += position;
        position = 0;
      }
    }

    @Override
    public void close() throws IOException {
      // the terminator has to reach the file before the stream shuts: a run without it reads as
      // truncated, which is now an EOFException rather than a silent short read
      if (position + 1 > buffer.length) {
        drain();
      }
      endOfRows = flushed + position;
      buffer[position++] = ROW_END;
      drain();
      out.close();
    }
  }

  /**
   * Reads a run back in the order it was written. {@link Cursor#term()} is null once exhausted.
   *
   * <p>Decodes from a refilled byte block rather than from a stream: a run is almost entirely
   * varints, and a buffered stream charges a synchronized call per byte. The blocking is invisible
   * to the format — this reads exactly the bytes the writer wrote, in the same order.
   */
  static final class Cursor implements Closeable {
    /** One filesystem read per 64 KiB rather than per byte. */
    private static final int BUFFER_BYTES = 1 << 16;

    private final InputStream stream;
    private final byte[] buffer = new byte[BUFFER_BYTES];
    private int position;
    private int limit;
    /** A slice ends at a byte bound rather than at the end marker; see the 3-argument constructor. */
    private final boolean bounded;
    private long remaining;
    // The current row, held as fields rather than in a record: every consumer unwrapped the record
    // immediately, and one allocation per row -- across ~9e8 terms per cascade level -- bought
    // nothing but the unwrapping.
    private byte[] term;
    private int ordinal;
    private long count;

    Cursor(Path file) throws IOException {
      this(file, 0, -1);
    }

    /**
     * Reads only {@code [from, to)} of the run -- one range's slice, per {@code Writer.rangeOffsets}
     * -- and reports exhaustion at {@code to} rather than at the end marker. {@code to < 0} means
     * the whole run. The writer restarts front coding at every range boundary, so a slice's first
     * row carries its full term.
     */
    Cursor(Path file, long from, long to) throws IOException {
      // no BufferedInputStream: this class does its own buffering, and stacking the two would
      // reintroduce exactly the per-byte synchronized call it exists to avoid
      InputStream opened = Files.newInputStream(file);
      this.stream = opened;
      this.bounded = to >= 0;
      this.remaining = bounded ? to - from : Long.MAX_VALUE;
      try {
        if (from > 0) {
          opened.skipNBytes(from);
        }
        next();
      } catch (IOException | RuntimeException e) {
        opened.close();
        throw e;
      }
    }

    /**
     * The current row's term, or null once the run is exhausted. The array belongs to the row and a
     * later {@link #next()} does not touch it: the merge holds the smallest term while draining
     * every source carrying it, so {@link #next()} must allocate a fresh array per row rather than
     * refill one. See {@code RunMerger.Source}.
     */
    byte[] term() {
      return term;
    }

    int ordinal() {
      return ordinal;
    }

    long count() {
      return count;
    }

    void next() throws IOException {
      if (bounded && position == limit && !fill()) {
        term = null; // the slice ends at a record boundary, never at the marker
        return;
      }
      if (nextByte() == ROW_END) {
        term = null;
        return;
      }
      // Decoded into locals and published together, so a truncated run never leaves a new term
      // beside a stale ordinal and count.
      //
      // Front coded: the leading `shared` bytes come from the term this cursor last handed out.
      // Reading from it is safe; writing to it would not be, since the array belongs to the row and
      // RunMerger holds it while draining. Front coding saves IO, never allocation.
      int shared = (int) readVLong();
      int suffixLen = (int) readVLong();
      if (shared > (term == null ? 0 : term.length) || shared < 0 || suffixLen < 0) {
        // A run claiming more shared prefix than exists is corrupt. Say so rather than reading
        // whatever happens to be adjacent, which would invent a term that was never written.
        throw new IOException(
            "malformed term run: shared prefix " + shared + " exceeds the previous term");
      }
      byte[] nextTerm = new byte[shared + suffixLen];
      if (shared > 0) {
        System.arraycopy(term, 0, nextTerm, 0, shared);
      }
      readFully(nextTerm, shared, suffixLen);
      int nextOrdinal = (int) readVLong();
      long nextCount = readVLong();
      term = nextTerm;
      ordinal = nextOrdinal;
      count = nextCount;
    }

    /** True while the buffer holds at least one unread byte. */
    private boolean fill() throws IOException {
      position = 0;
      int want = (int) Math.min(buffer.length, remaining);
      if (want <= 0) {
        limit = 0;
        return false;
      }
      limit = stream.read(buffer, 0, want);
      if (limit <= 0) {
        limit = 0;
        return false;
      }
      remaining -= limit;
      return true;
    }

    private int nextByte() throws IOException {
      if (position == limit && !fill()) {
        throw new EOFException("truncated term run");
      }
      return buffer[position++] & 0xFF;
    }

    /** Copies {@code len} bytes into {@code dest} at {@code off}, crossing buffer refills. */
    private void readFully(byte[] dest, int off, int len) throws IOException {
      int copied = 0;
      while (copied < len) {
        if (position == limit && !fill()) {
          throw new EOFException("truncated term run");
        }
        int take = Math.min(len - copied, limit - position);
        System.arraycopy(buffer, position, dest, off + copied, take);
        position += take;
        copied += take;
      }
    }

    /** The same encoding {@link #writeVLong} produces, decoded out of the block buffer. */
    private long readVLong() throws IOException {
      long value = 0;
      for (int shift = 0; shift < 64; shift += 7) {
        int b = nextByte();
        value |= (long) (b & 0x7F) << shift;
        if ((b & 0x80) == 0) {
          return value;
        }
      }
      throw new IOException("malformed varint in term run");
    }

    @Override
    public void close() throws IOException {
      stream.close();
    }
  }

  // ------------------------------------------------------- varint encoding
  // Hand-rolled rather than borrowed: a run is mostly small counts and small ordinals, so
  // fixed-width fields would roughly double it.

  /** Encodes a varint into {@code dest} at {@code at}; returns the number of bytes written. */
  static int putVInt(byte[] dest, int at, long value) {
    int written = 0;
    long remaining = value;
    while ((remaining & ~0x7FL) != 0) {
      dest[at + written++] = (byte) ((remaining & 0x7F) | 0x80);
      remaining >>>= 7;
    }
    dest[at + written++] = (byte) remaining;
    return written;
  }

  /** How many bytes {@link #putVInt} would write for {@code value}. */
  static int vIntLength(long value) {
    int bytes = 1;
    long remaining = value;
    while ((remaining & ~0x7FL) != 0) {
      remaining >>>= 7;
      bytes++;
    }
    return bytes;
  }

  /** Decodes a varint from {@code src} at {@code at}; the value is in the low 56 bits of the
   * result and the byte length in the top 8. */
  static long getVInt(byte[] src, int at) {
    long value = 0;
    int len = 0;
    for (int shift = 0; shift < 64; shift += 7) {
      int b = src[at + len++] & 0xFF;
      value |= (long) (b & 0x7F) << shift;
      if ((b & 0x80) == 0) {
        break;
      }
    }
    return ((long) len << 56) | (value & 0x00FF_FFFF_FFFF_FFFFL);
  }

  static int vIntValue(long packed) {
    return (int) (packed & 0x00FF_FFFF_FFFF_FFFFL);
  }

  static int vIntBytes(long packed) {
    return (int) (packed >>> 56);
  }
}
