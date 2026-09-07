package io.kahshe.format.type.term;

import static org.junit.jupiter.api.Assertions.assertArrayEquals;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNotSame;
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

import java.io.EOFException;
import java.io.IOException;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.ArrayList;
import java.util.List;
import java.util.Random;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

/**
 * The run reader decodes from a block buffer instead of a stream; it must still read exactly the
 * bytes the writer wrote.
 *
 * <p>A {@code DataInputStream} over a {@code BufferedInputStream} costs a synchronized virtual call
 * for every byte of every varint, which is a large share of a big build's wall clock. Refilling a
 * 64 KiB array and indexing it is a change to the hottest decode in the build and touches no format
 * at all. So what is tested is EQUALITY WITH THE WRITER rather than any property of the reader:
 * whatever went in comes back, byte for byte.
 *
 * <p>The cases are chosen where a hand-rolled buffer breaks: values that sit on varint width
 * boundaries, and records that straddle a buffer refill. A run that fits in one buffer would
 * exercise none of the interesting code.
 */
class TermRunRoundTripTest {
  @TempDir Path tmp;

  private record Expected(byte[] term, int ordinal, long count) {}

  /**
   * The prefix boundaries front coding introduces, round-tripped.
   *
   * <p>Rows are sorted by {@code (term, ordinal)} and each row stores only the bytes it does not
   * share with the row before it. The cases below are where an off-by-one in the shared count
   * turns into a term that was never written: consecutive rows carrying the SAME term under
   * different ordinals (shared is the whole term, suffix is empty), a term that is a strict prefix
   * of the next, a term SHORTER than the one before it, and two terms sharing nothing at all.
   *
   * <p>Equality with the writer is the assertion, as everywhere else in this file — whatever went
   * in comes back byte for byte, whatever the encoding did in between.
   */
  @Test
  void prefixBoundariesSurviveTheRoundTrip() throws IOException {
    List<Expected> in =
        List.of(
            row("alpha", 0, 1L),
            row("alpha", 1, 2L),      // identical term, later ordinal: shared is the whole term
            row("alphabet", 0, 3L),   // the previous term is a strict prefix of this one
            row("alphabetical", 0, 4L),
            row("alz", 0, 5L),        // shorter than the previous, sharing only "al"
            row("beta", 0, 6L));      // shares nothing
    assertSame(in, writeAndRead(tmp.resolve("prefixes.run"), in));
  }

  private static Expected row(String term, int ordinal, long count) {
    return new Expected(term.getBytes(StandardCharsets.UTF_8), ordinal, count);
  }

  /**
   * Front coding has to actually happen, or this file measures nothing.
   *
   * <p>The round-trip tests pass identically whether or not terms are compressed, which is what
   * makes them a good oracle and a useless guard. This asserts the size, on the shape the format
   * is sorted into: many rows sharing a long prefix.
   *
   * <p>Verified by breaking it: writing the full term every row — the format before front coding —
   * roughly triples the file and fails this.
   */
  @Test
  void aSharedPrefixIsNotWrittenTwice() throws IOException {
    List<Expected> rows = new ArrayList<>();
    for (int i = 0; i < 500; i++) {
      rows.add(row(String.format("shared-prefix-of-some-length-%04d", i), 0, 1L));
    }
    Path file = tmp.resolve("shared.run");
    try (TermRun.Writer writer = new TermRun.Writer(file)) {
      for (Expected r : rows) {
        writer.append(r.term(), 0, r.term().length, r.ordinal(), r.count());
      }
    }
    long size = Files.size(file);
    int termLen = rows.get(0).term().length;
    // Uncompressed this row costs 1 marker + 1 len + termLen + 1 ordinal + 1 count.
    long plain = (long) rows.size() * (4 + termLen);
    assertTrue(
        size < plain / 2,
        "front coding did not happen: " + size + " bytes for " + rows.size() + " rows sharing a "
            + (termLen - 4) + "-character prefix, against " + plain + " written in full");
    assertSame(rows, writeAndRead(tmp.resolve("shared-rt.run"), rows));
  }

  /**
   * A run claiming more shared prefix than exists is corrupt, and must say so.
   *
   * <p>Reading it anyway would copy whatever is adjacent in the buffer and hand back a term that
   * was never written — which the merge would then emit under that term. Absence prunes, so an
   * invented term is a false negative with no exception behind it. The first row of a run always
   * has a shared count of zero, so patching that byte is the smallest possible corruption.
   */
  @Test
  void aRunClaimingAnImpossiblePrefixIsRefused() throws IOException {
    Path file = tmp.resolve("corrupt.run");
    List<Expected> rows = List.of(row("alpha", 0, 1L), row("alphabet", 1, 2L));
    try (TermRun.Writer writer = new TermRun.Writer(file)) {
      for (Expected r : rows) {
        writer.append(r.term(), 0, r.term().length, r.ordinal(), r.count());
      }
    }
    byte[] bytes = Files.readAllBytes(file);
    bytes[1] = 9; // first row's shared count, which must be 0: claim nine bytes of nothing
    Files.write(file, bytes);

    assertThrows(
        IOException.class,
        () -> {
          try (TermRun.Cursor cursor = new TermRun.Cursor(file)) {
            while (cursor.term() != null) {
              cursor.next();
            }
          }
        },
        "a run claiming a prefix longer than the previous term was read as if it were valid");
  }

  /**
   * A term handed out stays valid after the cursor moves on. The merge depends on it.
   *
   * <p>{@code RunMerger.mergePairs} holds the smallest term while draining every cursor carrying
   * it, the cursor it came from included. If {@code Cursor.next()} refilled one array instead of
   * allocating per row — the obvious optimization, and the reason this test exists — the held term
   * would change mid-drain and the merge would emit rows under whatever term happened to be read
   * last. That is a false negative with no exception and
   * no metric, so the aliasing contract is pinned here rather than left to a comment.
   *
   * <p>Both terms are the SAME LENGTH on purpose: a reused buffer sized per row would reallocate
   * on a length change and the aliasing would not show. Equal lengths are the case that overwrites
   * in place.
   *
   * <p>Verified by breaking it: making {@code next()} reuse one array fails the third assertion.
   */
  @Test
  void aTermStaysValidAfterTheCursorMovesOn() throws IOException {
    Path file = tmp.resolve("aliasing.run");
    byte[] first = "alpha".getBytes(StandardCharsets.UTF_8);
    byte[] second = "bravo".getBytes(StandardCharsets.UTF_8);
    try (TermRun.Writer writer = new TermRun.Writer(file)) {
      writer.append(first, 0, first.length, 1, 10L);
      writer.append(second, 0, second.length, 2, 20L);
    }

    try (TermRun.Cursor cursor = new TermRun.Cursor(file)) {
      byte[] held = cursor.term();
      assertArrayEquals(first, held);
      cursor.next();
      assertArrayEquals(second, cursor.term(), "the cursor did not advance");
      assertArrayEquals(first, held, "next() overwrote a term it had already handed out");
      assertNotSame(held, cursor.term(), "both rows are one array; the merge holds the older one");
    }
  }

  private static List<Expected> writeAndRead(Path file, List<Expected> rows) throws IOException {
    try (TermRun.Writer writer = new TermRun.Writer(file)) {
      for (Expected row : rows) {
        writer.append(row.term(), 0, row.term().length, row.ordinal(), row.count());
      }
    }
    List<Expected> read = new ArrayList<>();
    try (TermRun.Cursor cursor = new TermRun.Cursor(file)) {
      while (cursor.term() != null) {
        read.add(new Expected(cursor.term(), cursor.ordinal(), cursor.count()));
        cursor.next();
      }
    }
    return read;
  }

  private static void assertSame(List<Expected> in, List<Expected> out) {
    assertEquals(in.size(), out.size(), "row count changed across the round trip");
    for (int i = 0; i < in.size(); i++) {
      assertArrayEquals(in.get(i).term(), out.get(i).term(), "term differs at row " + i);
      assertEquals(in.get(i).ordinal(), out.get(i).ordinal(), "ordinal differs at row " + i);
      assertEquals(in.get(i).count(), out.get(i).count(), "count differs at row " + i);
    }
  }

  /**
   * Values sitting exactly on varint width boundaries.
   *
   * <p>A varint changes length at every multiple of seven bits, and an off-by-one in the shift loop
   * shows up only there — everywhere else it silently agrees.
   */
  @Test
  void valuesOnVarintWidthBoundariesSurvive() throws Exception {
    List<Expected> rows = new ArrayList<>();
    long[] boundaries = {
      0, 1, 126, 127, 128, 129, 16_383, 16_384, 2_097_151, 2_097_152,
      268_435_455, 268_435_456, Integer.MAX_VALUE, Long.MAX_VALUE
    };
    // terms must be written in non-descending order, so pad an increasing prefix
    for (int i = 0; i < boundaries.length; i++) {
      byte[] term = String.format("t%04d", i).getBytes(StandardCharsets.UTF_8);
      rows.add(new Expected(term, i, boundaries[i]));
    }
    assertSame(rows, writeAndRead(tmp.resolve("boundaries.run"), rows));
  }

  /**
   * Records that straddle a buffer refill.
   *
   * <p>This is the case a hand-rolled block buffer can plausibly get wrong and a stream-based
   * reader cannot: a term or a varint split across two 64 KiB reads. The terms here are long enough
   * that many rows land on a boundary, and one is larger than the buffer itself, so {@code
   * readFully} has to refill mid-copy.
   */
  @Test
  void recordsThatStraddleABufferRefillSurvive() throws Exception {
    List<Expected> rows = new ArrayList<>();
    Random random = new Random(20260830L);
    for (int i = 0; i < 4000; i++) {
      byte[] term = new byte[200 + random.nextInt(120)];
      byte[] prefix = String.format("k%06d", i).getBytes(StandardCharsets.UTF_8);
      System.arraycopy(prefix, 0, term, 0, prefix.length);
      for (int b = prefix.length; b < term.length; b++) {
        term[b] = (byte) ('a' + random.nextInt(26));
      }
      rows.add(new Expected(term, random.nextInt(1 << 20), random.nextLong() >>> 1));
    }
    // one term larger than the whole 64 KiB buffer: readFully must refill mid-copy
    byte[] huge = new byte[200_000];
    java.util.Arrays.fill(huge, (byte) 'z');
    rows.add(new Expected(huge, 7, 42L));

    Path file = tmp.resolve("straddle.run");
    assertSame(rows, writeAndRead(file, rows));
    // the run really is many buffers long, or the test proved nothing
    assertEquals(true, Files.size(file) > 4L * (1 << 16), "run was too small to cross a refill");
  }

  /** An empty run reads back as immediately exhausted rather than as a failure. */
  @Test
  void anEmptyRunIsExhaustedImmediately() throws Exception {
    Path file = tmp.resolve("empty.run");
    try (TermRun.Writer writer = new TermRun.Writer(file)) {
      assertEquals(0, writer.rows());
    }
    try (TermRun.Cursor cursor = new TermRun.Cursor(file)) {
      assertNull(cursor.term());
    }
  }

  /**
   * A truncated run must fail loudly.
   *
   * <p>A block-buffered reader has to raise this itself rather than inheriting it from {@code
   * DataInputStream}, and a buffer that quietly returned zeros at EOF would turn a corrupt run into
   * plausible garbage — terms that never existed, mapped to ordinals that do.
   */
  @Test
  void aTruncatedRunThrowsRatherThanInventingRows() throws Exception {
    Path file = tmp.resolve("truncated.run");
    List<Expected> rows =
        List.of(
            new Expected("alpha".getBytes(StandardCharsets.UTF_8), 1, 10L),
            new Expected("bravo".getBytes(StandardCharsets.UTF_8), 2, 20L));
    try (TermRun.Writer writer = new TermRun.Writer(file)) {
      for (Expected row : rows) {
        writer.append(row.term(), 0, row.term().length, row.ordinal(), row.count());
      }
    }
    byte[] whole = Files.readAllBytes(file);
    Files.write(file, java.util.Arrays.copyOf(whole, whole.length - 3));

    assertThrows(
        EOFException.class,
        () -> {
          try (TermRun.Cursor cursor = new TermRun.Cursor(file)) {
            while (cursor.term() != null) {
              cursor.next();
            }
          }
        },
        "a truncated run was read to a clean end; a reader that invents rows at EOF turns a "
            + "corrupt run into terms that never existed");
  }
}
