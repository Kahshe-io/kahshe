package io.kahshe.format.type.term;

/**
 * One data file's term counts: an open-addressed map from term to occurrence count, keys and counts
 * in parallel arrays.
 *
 * <p>An occurrence of a term already present costs one hash, one array probe and one increment, and
 * allocates nothing — which is the point, since the count is incremented per occurrence rather than
 * per distinct term and the build holds several files' maps in flight. The terms stay Strings: the
 * aggregate leaf stores them and the tokens arrive as Strings from
 * {@link io.kahshe.analysis.analyzer.Analyzer}.
 */
final class TermCounts {

  private String[] keys;
  private long[] counts;
  private int mask;
  private int size;
  private int growAt;

  /**
   * Distinct terms this map will hold before it stops accepting new ones.
   *
   * <p>The term build itself does not hold this map — readers stream into a fixed arena instead —
   * but a watch rule matching on tokens needs one file's counts as something it can probe by token,
   * so the map is capped rather than removed. The cap is on the one quantity that drives its size:
   * distinct terms, of which 16 million is roughly 400 MiB of keys, counts and String headers on a
   * corpus of identifiers. A file past the cap stops accumulating and says so
   * ({@link #truncated()}).
   */
  private static final int DEFAULT_MAX_DISTINCT =
      Integer.getInteger("kahshe.watch.max.distinct.terms", 16_000_000);

  /**
   * Held per instance rather than read from the static at each use: a class-initialization read
   * would let the first build in a JVM pin the cap for every later one.
   */
  private final int maxDistinct;

  private boolean truncated;

  TermCounts() {
    this(1 << 14);
  }

  TermCounts(int initialCapacity) {
    this(initialCapacity, DEFAULT_MAX_DISTINCT);
  }

  TermCounts(int initialCapacity, int maxDistinct) {
    this.maxDistinct = maxDistinct;
    int capacity = Integer.highestOneBit(Math.max(16, initialCapacity - 1)) << 1;
    this.keys = new String[capacity];
    this.counts = new long[capacity];
    this.mask = capacity - 1;
    this.growAt = capacity >>> 1; // load factor 0.5 keeps probe chains short
  }

  int size() {
    return size;
  }

  /** Adds one occurrence of {@code term}. */
  void add(String term) {
    add(term, 1L);
  }

  /**
   * Whether this file's terms were capped, so the counts are a lower bound rather than exact.
   *
   * <p>A watch rule reading truncated counts could under-report and silently fail to fire, so the
   * consumer must check this and degrade loudly rather than trusting the number.
   */
  boolean truncated() {
    return truncated;
  }

  /**
   * Adds {@code by} occurrences of {@code term}. At the distinct-term cap a term first seen here is
   * dropped and {@link #truncated()} flips; occurrences of a term already present keep accumulating
   * exactly, so a capped map is a lower bound rather than nonsense.
   */
  void add(String term, long by) {
    int at = slot(keys, mask, term);
    if (keys[at] == null) {
      if (size >= maxDistinct) {
        // stop growing, and remember that we did: occurrences of terms already present still
        // accumulate correctly, and what is lost is terms first seen past the cap
        truncated = true;
        return;
      }
      keys[at] = term;
      counts[at] = by;
      if (++size >= growAt) {
        grow();
      }
    } else {
      counts[at] += by;
    }
  }

  private static int slot(String[] table, int mask, String term) {
    // String.hashCode is weak in its high bits for short similar strings, which is exactly what a
    // corpus of hex identifiers is; mixing spreads them across the table.
    int h = term.hashCode();
    h ^= (h >>> 16);
    int at = (int) ((h * 0x9E3779B1L) >>> 8) & mask;
    while (table[at] != null && !table[at].equals(term)) {
      at = (at + 1) & mask;
    }
    return at;
  }

  private void grow() {
    String[] oldKeys = keys;
    long[] oldCounts = counts;
    int capacity = oldKeys.length << 1;
    String[] newKeys = new String[capacity];
    long[] newCounts = new long[capacity];
    int newMask = capacity - 1;
    for (int i = 0; i < oldKeys.length; i++) {
      if (oldKeys[i] != null) {
        int at = slot(newKeys, newMask, oldKeys[i]);
        newKeys[at] = oldKeys[i];
        newCounts[at] = oldCounts[i];
      }
    }
    keys = newKeys;
    counts = newCounts;
    mask = newMask;
    growAt = capacity >>> 1;
  }

  /** Empties the map, keeping the table so a reused buffer does not re-grow from scratch. */
  void clear() {
    java.util.Arrays.fill(keys, null);
    java.util.Arrays.fill(counts, 0L);
    size = 0;
  }

  /** The count for one term, or 0 if absent. */
  long countOf(String term) {
    int at = slot(keys, mask, term);
    return keys[at] == null ? 0L : counts[at];
  }

  /** Visits every (term, count) once, in no particular order. */
  void forEach(Visitor visitor) throws java.io.IOException {
    for (int i = 0; i < keys.length; i++) {
      if (keys[i] != null) {
        visitor.accept(keys[i], counts[i]);
      }
    }
  }

  interface Visitor {
    void accept(String term, long count) throws java.io.IOException;
  }
}
