package io.kahshe.format.type.term;

import java.io.Closeable;
import java.io.IOException;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.ArrayList;
import java.util.HashSet;
import java.util.List;
import java.util.Set;
import org.roaringbitmap.RoaringBitmap;

/**
 * The k-way merge that turns sorted runs into aggregate rows, over a heap and cascading through
 * intermediate runs past {@link #MAX_OPEN} sources.
 *
 * <p>Sources are mixed — a run row carries one ordinal, an incremental build's prior aggregate row a
 * whole bitmap, and an intermediate run is the same {@code (term, ordinal, count)} format as a run,
 * so nothing downstream knows a cascade happened — and a merged row's {@code file_count} is always
 * {@code union.getCardinality()}, never a sum, because one data file contributes to two runs as soon
 * as a reader's arena fills mid-file, i.e. always. The heap and the cascade are both forced by
 * {@code k} being the total run count, thousands of them on a large corpus: more descriptors than a
 * host will give, and far too many comparisons for a linear scan.
 */
final class RunMerger {

  /** How many sources one merge level may open at once. */
  static final int MAX_OPEN = 256;

  private RunMerger() {}

  /**
   * One sorted stream of rows, ascending by term as unsigned UTF-8 bytes.
   *
   * <p>Contract: the array {@link #term()} returns belongs to the row, and a later {@link #next()}
   * must not mutate it. The merge holds the smallest term while draining every source that carries
   * it — including the source it came from — so a cursor that reused one buffer across rows would
   * change the value being compared against, mid-comparison.
   */
  interface Source extends Closeable {
    /** The current row's term, or null when exhausted. */
    byte[] term();

    /** The current row's occurrence count. */
    long count();

    /** Adds the current row's ordinals to {@code target}. One ordinal for a run row, a whole
     * bitmap for a prior aggregate row. */
    void ordinalsInto(RoaringBitmap target);

    void next() throws IOException;
  }

  /** Receives merged rows in ascending term order. */
  interface RowSink {
    void accept(String term, long totalCount, RoaringBitmap ordinals) throws IOException;
  }

  /** Names the intermediate runs a cascading merge needs, and accounts for them once written. */
  interface Names {
    /** Each run's range byte bounds, recorded by the writer that produced it. */
    default void offsets(Path run, long[] bounds) {}

    default long[] offsets(Path run) {
      return null;
    }

    Path next(int level, int sequence, boolean intermediate) throws IOException;

    /** Called once an intermediate run is closed and its size is a fact; an intermediate is local
     * disk, charged against the same budget a reader's run is. */
    default void finished(Path run) throws IOException {}

    /** Called just before a consumed run is deleted, so the budget tracks what is on disk rather
     * than everything ever written. */
    default void released(Path run) throws IOException {}
  }

  /** A source over one run file. */
  static Source over(Path run) throws IOException {
    return over(new TermRun.Cursor(run));
  }

  static Source over(TermRun.Cursor cursor) throws IOException {
    return new Source() {
      @Override
      public byte[] term() {
        return cursor.term();
      }

      @Override
      public long count() {
        return cursor.count();
      }

      @Override
      public void ordinalsInto(RoaringBitmap target) {
        target.add(cursor.ordinal());
      }

      @Override
      public void next() throws IOException {
        cursor.next();
      }

      @Override
      public void close() throws IOException {
        cursor.close();
      }
    };
  }

  /**
   * Merges {@code runs} and {@code prior} into {@code sink}, ascending by term. Returns the number
   * of distinct terms emitted. Every source is closed, {@code prior} included.
   *
   * <p>{@code prior} is not optional plumbing: an incremental build's previous generation lives in
   * its aggregate leaves, and a merge that took only runs would publish an index covering every
   * file while holding terms from only the new ones — every already-covered file silently pruned
   * for every term it alone contains.
   */
  static long merge(List<Path> runs, List<Source> prior, Names names, RowSink sink)
      throws IOException {
    return merge(runs, prior, names, MAX_OPEN, sink);
  }

  /** As above, with the per-level fan-in. Parameterised so a test can force the cascade without
   * opening several hundred files to do it. */
  static long merge(List<Path> runs, List<Source> prior, Names names, int maxOpen, RowSink sink)
      throws IOException {
    return merge(runs, prior, names, maxOpen, sink, TermRun.Cursor::new);
  }

  /** How a run is opened: whole, or one range's slice of it. */
  interface Opener {
    TermRun.Cursor open(Path run) throws IOException;
  }

  /**
   * Merges one range: every run's slice for it (runs holding none of it are not opened) with that
   * range's prior source. Intermediate cascade runs belong to the range and are read whole.
   */
  static long mergeRange(
      List<Path> runs, Names names, int range, List<Source> prior, int maxOpen, RowSink sink)
      throws IOException {
    List<Path> sliced = new ArrayList<>();
    for (Path run : runs) {
      long[] b = names.offsets(run);
      if (b == null) {
        throw new IOException("no range offsets recorded for run " + run);
      }
      if (b[range + 1] > b[range]) {
        sliced.add(run);
      }
    }
    Set<Path> originals = new HashSet<>(sliced);
    // A range's cascade runs beside the other ranges' on the pool and reads the same original
    // runs through its own slice. So its intermediates are named by range as well as by level
    // and sequence, and an original is never released or deleted here: the scratch sweep at the
    // end of the build owns those.
    Names scoped =
        new Names() {
          @Override
          public Path next(int level, int sequence, boolean intermediate) throws IOException {
            return names.next(level, range * RANGE_SEQUENCE_STRIDE + sequence, intermediate);
          }

          @Override
          public void released(Path run) throws IOException {
            if (!originals.contains(run)) {
              names.released(run);
            }
          }

          @Override
          public void finished(Path run) throws IOException {
            names.finished(run);
          }

          @Override
          public void offsets(Path run, long[] b) {
            names.offsets(run, b);
          }

          @Override
          public long[] offsets(Path run) {
            return names.offsets(run);
          }
        };
    return merge(
        sliced, prior, scoped, maxOpen, sink, originals,
        run -> {
          if (!originals.contains(run)) {
            return new TermRun.Cursor(run);
          }
          long[] b = names.offsets(run);
          return new TermRun.Cursor(run, b[range], b[range + 1]);
        });
  }

  /** Sequence numbers per level stay far below this, so ranges cannot collide on a name. */
  static final int RANGE_SEQUENCE_STRIDE = 1 << 20;

  static long merge(
      List<Path> runs, List<Source> prior, Names names, int maxOpen, RowSink sink, Opener open)
      throws IOException {
    return merge(runs, prior, names, maxOpen, sink, Set.of(), open);
  }

  /** {@code keep}: runs the cascade may read but must not delete, because they are not its own. */
  static long merge(
      List<Path> runs, List<Source> prior, Names names, int maxOpen, RowSink sink,
      Set<Path> keep, Opener open)
      throws IOException {
    if (maxOpen < 2) {
      throw new IllegalArgumentException("merge fan-in must be at least 2, got " + maxOpen);
    }
    // `prior` is open on entry -- the caller built those cursors from Parquet leaves before
    // calling. The cascade below can throw, so they must be inside the try from the first
    // statement: otherwise a failed cascade leaks one open Parquet reader per prior range leaf.
    List<Source> sources = new ArrayList<>(prior);
    try {
      List<Path> level = runs;
      int depth = 0;
      while (level.size() + prior.size() > maxOpen) {
        level = mergeLevel(level, names, ++depth, maxOpen, keep, open);
        if (level.size() + prior.size() > maxOpen && level.size() <= 1) {
          // one run left and still over the limit means prior alone exceeds it; nothing more to
          // cascade, so open what remains rather than looping forever
          break;
        }
      }
      for (Path run : level) {
        sources.add(over(open.open(run)));
      }
      return mergeSources(sources, sink);
    } finally {
      closeAll(sources);
    }
  }

  /**
   * One level of a cascading merge: groups of at most {@link #MAX_OPEN} runs become one run.
   *
   * <p>This merges by {@code (term, ordinal)}, not by term: an intermediate run has exactly the
   * shape of its inputs, one row per pair carrying that pair's count. A term-level row here would
   * leave the per-ordinal counts nowhere to go — the whole term total on one ordinal and zero on
   * the rest — which the sum and the ordinal union both survive, so a run's {@code count} would
   * silently stop meaning "this term's occurrences in this file" with every test still passing.
   */
  private static List<Path> mergeLevel(
      List<Path> runs, Names names, int level, int maxOpen, Set<Path> keep, Opener open)
      throws IOException {
    List<Path> out = new ArrayList<>((runs.size() / maxOpen) + 1);
    for (int at = 0; at < runs.size(); at += maxOpen) {
      List<Path> group = runs.subList(at, Math.min(at + maxOpen, runs.size()));
      Path merged = names.next(level, out.size(), true);
      List<TermRun.Cursor> cursors = new ArrayList<>(group.size());
      try (TermRun.Writer writer = new TermRun.Writer(merged)) {
        for (Path run : group) {
          cursors.add(open.open(run));
        }
        mergePairs(cursors, writer);
      } finally {
        for (TermRun.Cursor cursor : cursors) {
          try {
            cursor.close();
          } catch (IOException ignored) {
            // nothing left to do with a cursor being discarded
          }
        }
      }
      // an intermediate is local disk like any other run, so the scratch budget is charged for it:
      // a cascade writes a second copy of every run it reads
      names.finished(merged);
      // and the inputs are deleted as soon as they are consumed rather than at the end-of-build
      // sweep, which is what keeps that second copy transient rather than peak
      for (Path run : group) {
        if (keep.contains(run)) {
          continue; // shared with other ranges; the build's scratch sweep deletes it
        }
        try {
          names.released(run);
          Files.deleteIfExists(run);
        } catch (IOException ignored) {
          // best effort; the build's scratch directory is swept whole on the way out
        }
      }
      out.add(merged);
    }
    return out;
  }

  /** Merges run cursors by {@code (term, ordinal)}, summing each pair's count, into one run. */
  private static void mergePairs(List<TermRun.Cursor> cursors, TermRun.Writer writer)
      throws IOException {
    IndexHeap heap =
        new IndexHeap(cursors.size()) {
          @Override
          boolean less(int a, int b) {
            TermRun.Cursor x = cursors.get(a);
            TermRun.Cursor y = cursors.get(b);
            int cmp = TermRun.compare(x.term(), y.term());
            return cmp != 0 ? cmp < 0 : x.ordinal() < y.ordinal();
          }
        };
    for (int i = 0; i < cursors.size(); i++) {
      if (cursors.get(i).term() != null) {
        heap.push(i);
      }
    }
    heap.heapify();
    while (heap.size > 0) {
      TermRun.Cursor head = cursors.get(heap.top());
      byte[] term = head.term();
      int ordinal = head.ordinal();
      long total = 0;
      while (heap.size > 0) {
        TermRun.Cursor cursor = cursors.get(heap.top());
        byte[] rowTerm = cursor.term();
        if (rowTerm == null || cursor.ordinal() != ordinal || TermRun.compare(rowTerm, term) != 0) {
          break;
        }
        total += cursor.count();
        cursor.next();
        heap.advanced(cursor.term() == null);
      }
      writer.append(term, 0, term.length, ordinal, total);
    }
  }

  /**
   * The merge proper: a binary min-heap over the sources, keyed on the current term of each.
   *
   * <p>Every row of the smallest outstanding term is drained from every source holding it, counts
   * summed and ordinals unioned, and exactly one row is emitted. Only that term's bitmap is ever
   * in memory.
   */
  private static long mergeSources(List<Source> sources, RowSink sink) throws IOException {
    IndexHeap heap =
        new IndexHeap(sources.size()) {
          @Override
          boolean less(int a, int b) {
            return TermRun.compare(sources.get(a).term(), sources.get(b).term()) < 0;
          }
        };
    for (int i = 0; i < sources.size(); i++) {
      if (sources.get(i).term() != null) {
        heap.push(i);
      }
    }
    heap.heapify();
    long terms = 0;
    byte[] emitted = null;
    while (heap.size > 0) {
      byte[] min = sources.get(heap.top()).term();
      if (emitted != null && TermRun.compare(min, emitted) <= 0) {
        throw new IOException(
            "term merge received a non-ascending source: ["
                + new String(emitted, StandardCharsets.UTF_8)
                + "] already emitted, then ["
                + new String(min, StandardCharsets.UTF_8)
                + "]");
      }
      long total = 0;
      RoaringBitmap union = new RoaringBitmap();
      while (heap.size > 0) {
        Source source = sources.get(heap.top());
        byte[] term = source.term();
        if (term == null || TermRun.compare(term, min) != 0) {
          break;
        }
        total += source.count();
        source.ordinalsInto(union);
        source.next();
        heap.advanced(source.term() == null);
      }
      sink.accept(new String(min, StandardCharsets.UTF_8), total, union);
      emitted = min;
      terms++;
    }
    return terms;
  }

  /**
   * A binary min-heap of source indexes. The two merges above differ only in {@link #less}: the
   * pair merge orders by (term, ordinal), the final merge by term alone. The comparison is a
   * bimorphic call the JIT inlines.
   */
  private abstract static class IndexHeap {
    final int[] heap;
    int size;

    IndexHeap(int capacity) {
      heap = new int[capacity];
    }

    abstract boolean less(int a, int b);

    void push(int index) {
      heap[size++] = index;
    }

    void heapify() {
      for (int i = (size >>> 1) - 1; i >= 0; i--) {
        siftDown(i);
      }
    }

    int top() {
      return heap[0];
    }

    /** The top's source moved on: drop it when exhausted, then restore the heap order. */
    void advanced(boolean exhausted) {
      if (exhausted) {
        heap[0] = heap[--size];
      }
      if (size > 0) {
        siftDown(0);
      }
    }

    private void siftDown(int at) {
      int node = at;
      while (true) {
        int left = (node << 1) + 1;
        if (left >= size) {
          return;
        }
        int smallest = left;
        int right = left + 1;
        if (right < size && less(heap[right], heap[left])) {
          smallest = right;
        }
        if (!less(heap[smallest], heap[node])) {
          return;
        }
        int tmp = heap[node];
        heap[node] = heap[smallest];
        heap[smallest] = tmp;
        node = smallest;
      }
    }
  }

  private static void closeAll(List<Source> sources) {
    for (Source source : sources) {
      try {
        source.close();
      } catch (IOException ignored) {
        // nothing left to do with a source being discarded
      }
    }
  }
}
