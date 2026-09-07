package io.kahshe.format.type.term;

/**
 * The term space, split into {@link #COUNT} ranges by first character, which is what lets a reader
 * open one aggregate leaf for a needle and a build merge each range independently — necessary
 * because on identifier-heavy text the dictionary approaches one entry per source row.
 *
 * <p>The ranges tile the entire String space, not just the analyzer's alphabet — range 0 runs from
 * negative infinity and the last to positive infinity — so every possible term lands in exactly one
 * range even if {@link io.kahshe.analysis.analyzer.Analyzer} is changed to emit characters outside
 * {@code [0-9a-z]}. A term that fell in a gap would be a term the index does not know about, and
 * that is a file wrongly pruned. The count is fixed for every build, three-row tables included, so
 * there is one partitioning path rather than a rarely-run second one.
 */
public final class TermRanges {

  /**
   * Ascending, and that order matters: {@link io.kahshe.analysis.analyzer.Analyzer} emits maximal runs of ASCII {@code [a-z0-9]}
   * and the accumulator orders terms by {@code String.compareTo}, which over that alphabet is
   * unsigned byte order — digits (0x30-0x39) sort before letters (0x61-0x7a). So a term's range
   * index is monotonic in the term, and a sorted drain visits the ranges in order and never
   * revisits one.
   */
  static final String ALPHABET = "0123456789abcdefghijklmnopqrstuvwxyz";

  public static final int COUNT = ALPHABET.length();

  private TermRanges() {}

  /** Which range {@code term} belongs to. Total: every string maps to exactly one index. */
  static int of(String term) {
    if (term == null || term.isEmpty()) {
      return 0; // the empty term sorts before everything, and range 0 starts at -infinity
    }
    return ofFirst(term.charAt(0));
  }

  /**
   * The range of a term given as UTF-8 bytes: its first byte. For the ASCII alphabet that is its
   * first char; a non-ASCII lead byte (>= 0x80) sorts above 'z' exactly as the char would.
   */
  static int of(byte[] term, int from, int len) {
    if (len == 0) {
      return 0;
    }
    return ofFirst((char) (term[from] & 0xFF));
  }

  private static int ofFirst(char first) {
    int at = ALPHABET.indexOf(first);
    if (at >= 0) {
      return at;
    }
    // Outside the alphabet. Place it by where it sorts, so the ranges stay contiguous in term
    // order: anything below '0' joins the first range, anything above 'z' joins the last, and
    // anything between two alphabet characters joins the range of the one below it.
    if (first < ALPHABET.charAt(0)) {
      return 0;
    }
    if (first > ALPHABET.charAt(COUNT - 1)) {
      return COUNT - 1;
    }
    int below = 0;
    for (int i = 0; i < COUNT; i++) {
      if (ALPHABET.charAt(i) < first) {
        below = i;
      } else {
        break;
      }
    }
    return below;
  }

  /** The leaf file name for one range: zero-padded so a directory listing sorts the way terms do. */
  static String leafName(long snapshotId, int range, String nonce) {
    return String.format("aggregate-%d-r%02d-%s.parquet", snapshotId, range, nonce);
  }
}
