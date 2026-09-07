package io.kahshe.format.type.term;

import java.io.Closeable;
import java.io.IOException;
import java.nio.file.Path;
import java.util.ArrayList;
import java.util.List;
import io.kahshe.analysis.analyzer.Analyzer;

/**
 * One reader's fixed byte arena: terms are appended raw and uncoalesced, then the arena is sorted in
 * place and written out as a sorted run, duplicates collapsing at that flush — so a file of prose
 * and a file of unique trace ids cost the same here.
 *
 * <p>Full is whichever of two allocated capacities is reached first — the arena
 * {@code byte[arenaBytes]}, or the offset index {@code int[arenaBytes / BYTES_PER_RECORD]}, which
 * binds when records are short — so peak cost is
 * {@code arenaBytes + 4 * (arenaBytes / BYTES_PER_RECORD)}, a number that can be written down
 * before the first file is opened. Nothing else in the term build path grows with the corpus.
 */
public final class RunBuffer implements Closeable {

  /**
   * Assumed bytes per record when sizing the offset index. Records shorter than this make the
   * buffer flush on record count rather than on bytes, which leaves the arena size governing
   * nothing. Eight is the smallest value at which bytes bind on typical log text, at a peak reader
   * heap of {@code arena * 1.5} rather than {@code arena * 1.25}.
   */
  static final int BYTES_PER_RECORD = 8;

  /** Names runs for one build. Shared, because run identity is the one thing readers cannot each
   * decide for themselves: a per-reader sequence collides across readers, and {@code
   * Thread.getId()} is recycled between builds. */
  interface Names {
    /** Each finished run's range byte bounds (TermRun.Writer.rangeOffsets), kept for the merge. */
    default void offsets(Path run, long[] bounds) {}

    default long[] offsets(Path run) {
      return null;
    }

    Path next(int readerSlot, int sequence) throws IOException;

    /** Called once a run is closed and its size is a fact. The build's local-disk budget is
     * enforced here rather than inside the buffer, because the volume is shared across readers. */
    default void finished(Path run) throws IOException {}
  }

  private final byte[] arena;
  private final int[] offsets;
  private final Names names;
  private final int readerSlot;
  private final List<Path> runs = new ArrayList<>();

  private int used;
  private int count;
  private int sequence;
  private long recordsWritten;
  private long flushes;

  /** Below this an arena holds no record; above {@link #MAX_ARENA_BYTES} no JVM allocates the array. */
  private static final int MIN_ARENA_BYTES = 64;

  private static final int MAX_ARENA_BYTES = Integer.MAX_VALUE - 8;

  /** One record decoded in place, reused so the flush loop allocates nothing per record. */
  private final Record record = new Record();

  private static final class Record {
    int termLen;
    int termAt;
    int ordinal;
    long total;
  }

  public RunBuffer(long arenaBytes, Names names, int readerSlot) {
    int bytes = (int) Math.max(MIN_ARENA_BYTES, Math.min(MAX_ARENA_BYTES, arenaBytes));
    this.arena = new byte[bytes];
    this.offsets = new int[Math.max(1, bytes / BYTES_PER_RECORD)];
    this.names = names;
    this.readerSlot = readerSlot;
  }

  /** The runs this buffer has written, in the order it wrote them. */
  public List<Path> runs() {
    return List.copyOf(runs);
  }

  long recordsWritten() {
    return recordsWritten;
  }

  long flushes() {
    return flushes;
  }

  int records() {
    return count;
  }

  /** Bytes this buffer can ever hold, arena plus offset index. Peak heap for a build's readers is
   * this times the reader count, and nothing else in the term path. */
  long capacityBytes() {
    return (long) arena.length + 4L * offsets.length;
  }

  /**
   * Appends one occurrence group. The term arrives as a {@code String}, not a {@code byte[]}: a
   * {@code byte[]} parameter would force a conversion once per token occurrence, and would
   * foreclose handing the arena {@code (offset, length)} pairs straight from the tokenizer.
   */
  public void append(String term, long count, int ordinal) throws IOException {
    int termBytes = utf8Length(term);
    int need = TermRun.vIntLength(termBytes) + termBytes
        + TermRun.vIntLength(Integer.toUnsignedLong(ordinal)) + TermRun.vIntLength(count);

    if (need > arena.length) {
      // A single term larger than the whole arena. "Flush and retry" cannot make progress on it,
      // and dropping it is a false negative: Analyzer caps no token length and a long alphanumeric
      // token is indexable. It gets a run of its own.
      flush();
      writeOversized(term, termBytes, count, ordinal);
      return;
    }
    if (used + need > arena.length || this.count == offsets.length) {
      flush();
    }

    offsets[this.count++] = used;
    used += TermRun.putVInt(arena, used, termBytes);
    used += encodeUtf8(term, arena, used);
    used += TermRun.putVInt(arena, used, Integer.toUnsignedLong(ordinal));
    used += TermRun.putVInt(arena, used, count);
  }

  /** Decodes the record at {@code at} into {@link #record}: term length and offset, ordinal, count. */
  private void decodeRecord(int at) {
    long lenPacked = TermRun.getVInt(arena, at);
    record.termLen = TermRun.vIntValue(lenPacked);
    record.termAt = at + TermRun.vIntBytes(lenPacked);
    long ordPacked = TermRun.getVInt(arena, record.termAt + record.termLen);
    record.ordinal = TermRun.vIntValue(ordPacked);
    record.total =
        TermRun.vIntValue(
            TermRun.getVInt(arena, record.termAt + record.termLen + TermRun.vIntBytes(ordPacked)));
  }

  /**
   * Sorts what the arena holds and writes it out as one run, coalescing adjacent equal
   * {@code (term, ordinal)}.
   *
   * <p>One file per flush. The run format is explicitly terminated, so appending a second sorted
   * block to one file would be indistinguishable from end-of-stream to a cursor — it would silently
   * discard every later block — or, with the terminator dropped, would hand the merge a saw-tooth
   * stream. Both are files wrongly pruned.
   */
  void flush() throws IOException {
    if (count == 0) {
      return;
    }
    sort(0, count - 1);

    Path run = names.next(readerSlot, sequence++);
    TermRun.Writer writer = new TermRun.Writer(run);
    try (writer) {
      int i = 0;
      while (i < count) {
        decodeRecord(offsets[i]);
        int termLen = record.termLen;
        int termAt = record.termAt;
        int ordinal = record.ordinal;
        long total = record.total;

        // Adjacent records with the same (term, ordinal) are one row. Without this the same pair
        // reaches the final merge as several rows, and every count is summed once per record.
        int j = i + 1;
        while (j < count && sameTermAndOrdinal(offsets[i], offsets[j])) {
          decodeRecord(offsets[j]);
          total += record.total;
          j++;
        }
        writer.append(arena, termAt, termLen, ordinal, total);
        recordsWritten++;
        i = j;
      }
    }
    names.offsets(run, writer.rangeOffsets());
    names.finished(run);
    runs.add(run);
    flushes++;
    used = 0;
    count = 0;
  }

  /** Flushes whatever is still held. The residue is not optional: without it the last file a
   * reader touched contributes nothing to the index. */
  @Override
  public void close() throws IOException {
    flush();
  }

  private void writeOversized(String term, int termBytes, long count, int ordinal)
      throws IOException {
    byte[] buffer = new byte[termBytes];
    encodeUtf8(term, buffer, 0);
    Path run = names.next(readerSlot, sequence++);
    TermRun.Writer writer = new TermRun.Writer(run);
    try (writer) {
      writer.append(buffer, 0, termBytes, ordinal, count);
    }
    names.offsets(run, writer.rangeOffsets());
    names.finished(run);
    runs.add(run);
    flushes++;
    recordsWritten++;
  }

  // ------------------------------------------------------------- the sort

  /**
   * Three-way quicksort over the offset array, comparing the arena bytes each offset points at.
   * Hand-rolled because the JDK has no {@code Arrays.sort(int[], Comparator)} and boxing to
   * {@code Integer[]} would cost more heap than the arena itself.
   *
   * <p>Three-way, not two-way: log text is mostly a few terms repeated hundreds of thousands of
   * times, and a two-way partition walks each equal run {@code log n} times over where a three-way
   * collapses it into the middle in one pass.
   */
  private void sort(int low, int high) {
    while (low < high) {
      if (high - low < 12) {
        for (int i = low + 1; i <= high; i++) {
          int key = offsets[i];
          int j = i - 1;
          while (j >= low && compareRecords(offsets[j], key) > 0) {
            offsets[j + 1] = offsets[j];
            j--;
          }
          offsets[j + 1] = key;
        }
        return;
      }
      // median of three, so an already-ordered arena does not degrade to O(n^2)
      int mid = low + ((high - low) >>> 1);
      if (compareRecords(offsets[mid], offsets[low]) < 0) {
        swap(mid, low);
      }
      if (compareRecords(offsets[high], offsets[low]) < 0) {
        swap(high, low);
      }
      if (compareRecords(offsets[high], offsets[mid]) < 0) {
        swap(high, mid);
      }
      // The pivot is captured by value. Only the offsets array is permuted; the records it points
      // at never move, so the captured offset keeps naming the same record however the array is
      // rearranged around it.
      int pivot = offsets[mid];

      int lt = low;
      int i = low;
      int gt = high;
      while (i <= gt) {
        int cmp = compareRecords(offsets[i], pivot);
        if (cmp < 0) {
          swap(lt++, i++);
        } else if (cmp > 0) {
          swap(i, gt--);
        } else {
          i++;
        }
      }
      // [low, lt) less, [lt, gt] equal and done, (gt, high] greater. Recurse into the smaller side
      // and loop on the larger, which bounds stack depth at log n.
      if (lt - low < high - gt) {
        sort(low, lt - 1);
        low = gt + 1;
      } else {
        sort(gt + 1, high);
        high = lt - 1;
      }
    }
  }

  private void swap(int a, int b) {
    int t = offsets[a];
    offsets[a] = offsets[b];
    offsets[b] = t;
  }

  /**
   * Orders two records by term, then by ordinal. The offset points at the record's <b>length
   * prefix</b>, not at its term, so comparing from the offset would produce a length-major order
   * and hand the merge a stream it wrongly believes is sorted.
   */
  private int compareRecords(int a, int b) {
    // Inlined single-byte varint fast path: every term shorter than 128 bytes has a one-byte
    // length prefix, and this runs tens of millions of times per flush.
    int aLen = arena[a] & 0xFF;
    int aAt;
    if (aLen < 0x80) {
      aAt = a + 1;
    } else {
      long ap = TermRun.getVInt(arena, a);
      aLen = TermRun.vIntValue(ap);
      aAt = a + TermRun.vIntBytes(ap);
    }
    int bLen = arena[b] & 0xFF;
    int bAt;
    if (bLen < 0x80) {
      bAt = b + 1;
    } else {
      long bp = TermRun.getVInt(arena, b);
      bLen = TermRun.vIntValue(bp);
      bAt = b + TermRun.vIntBytes(bp);
    }
    int cmp = TermRun.compare(arena, aAt, aLen, arena, bAt, bLen);
    if (cmp != 0) {
      return cmp;
    }
    // Tie-break on ordinal, so the flush's adjacent-merge sees every (term, ordinal) pair
    // together. Relying on append order to do this instead would be relying on a reader visiting
    // files in ascending order, which is true today and is not a property anything states.
    int aOrd = TermRun.vIntValue(TermRun.getVInt(arena, aAt + aLen));
    int bOrd = TermRun.vIntValue(TermRun.getVInt(arena, bAt + bLen));
    return Integer.compare(aOrd, bOrd);
  }

  private boolean sameTermAndOrdinal(int a, int b) {
    return compareRecords(a, b) == 0;
  }

  // ------------------------------------------------------------- encoding

  private static int utf8Length(String term) {
    int bytes = 0;
    for (int i = 0; i < term.length(); i++) {
      char c = term.charAt(i);
      if (c < 0x80) {
        bytes += 1;
      } else if (c < 0x800) {
        bytes += 2;
      } else if (Character.isHighSurrogate(c) && i + 1 < term.length()
          && Character.isLowSurrogate(term.charAt(i + 1))) {
        bytes += 4;
        i++;
      } else {
        bytes += 3;
      }
    }
    return bytes;
  }

  /**
   * Writes {@code term} as UTF-8 into {@code dest} at {@code at}; returns the bytes written.
   * Encoded here rather than through {@code String.getBytes} because this runs once per token
   * occurrence, and the common ASCII path is a byte-per-char copy with no intermediate array. The
   * wider cases are handled anyway: {@link TermRanges} is explicit that the term space must not
   * depend on the analyzer staying ASCII.
   */
  private static int encodeUtf8(String term, byte[] dest, int at) {
    int out = at;
    for (int i = 0; i < term.length(); i++) {
      char c = term.charAt(i);
      if (c < 0x80) {
        dest[out++] = (byte) c;
      } else if (c < 0x800) {
        dest[out++] = (byte) (0xC0 | (c >> 6));
        dest[out++] = (byte) (0x80 | (c & 0x3F));
      } else if (Character.isHighSurrogate(c) && i + 1 < term.length()
          && Character.isLowSurrogate(term.charAt(i + 1))) {
        int cp = Character.toCodePoint(c, term.charAt(++i));
        dest[out++] = (byte) (0xF0 | (cp >> 18));
        dest[out++] = (byte) (0x80 | ((cp >> 12) & 0x3F));
        dest[out++] = (byte) (0x80 | ((cp >> 6) & 0x3F));
        dest[out++] = (byte) (0x80 | (cp & 0x3F));
      } else {
        dest[out++] = (byte) (0xE0 | (c >> 12));
        dest[out++] = (byte) (0x80 | ((c >> 6) & 0x3F));
        dest[out++] = (byte) (0x80 | (c & 0x3F));
      }
    }
    return out - at;
  }
}
