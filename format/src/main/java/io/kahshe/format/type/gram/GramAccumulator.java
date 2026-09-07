package io.kahshe.format.type.gram;

import java.util.LinkedHashSet;
import java.util.Set;
import io.kahshe.format.type.bloom.NgramBloom;

/**
 * The distinct n-grams of one data file, accumulated as primitive longs.
 *
 * <p>A few characters cannot be very diverse, so one data file yields orders of magnitude more gram
 * positions than distinct grams and the work is nearly all "have I seen this before" — which needs
 * an identity, not a String. Three units pack into a long at 21 bits each, with an absent marker
 * distinguishing a short whole value (one shorter than the gram size is indexed whole) from a
 * full-width gram. The encoding is exact and reversible, so {@link #toStrings} yields exactly the
 * strings {@link NgramBloom#gramsOf} produces.
 *
 * <p>Open-addressed with linear probing over a power-of-two table, allocating nothing per add.
 *
 * <p>It does not decide what a window IS — {@link GramRule#forEachWindowUnits} does, so the build
 * and the probe share one definition of the cut.
 */
public final class GramAccumulator {
  private static final int UNIT_BITS = 21; // a code point fits; so does one UTF-16 unit
  private static final long UNIT_MASK = (1L << UNIT_BITS) - 1;
  private static final long ABSENT = UNIT_MASK; // 0x1FFFFF is not a code point: an empty slot
  private static final int UNITS_PER_WORD = 3; // 63 bits; bit 63 of word 0 marks a used slot
  private static final long USED = Long.MIN_VALUE;

  private final Grams.Contract contract;
  private final int words;
  private long[] table;
  private int mask;
  private int size;
  private int growAt;
  private final long[] key;
  /** Bound once: the per-window callback must not allocate. */
  private final GramRule.Units sink = this::encodeWindow;

  public GramAccumulator(Grams.Contract contract) {
    this(contract, 1 << 12);
  }

  GramAccumulator(Grams.Contract contract, int initialCapacity) {
    this.contract = contract;
    this.words = (contract.size() + UNITS_PER_WORD - 1) / UNITS_PER_WORD;
    this.key = new long[words];
    int capacity = Integer.highestOneBit(Math.max(16, initialCapacity - 1)) << 1;
    this.table = new long[capacity * words];
    this.mask = capacity - 1;
    this.growAt = capacity >>> 1; // load factor 0.5: probes stay short, memory is trivial here
  }

  int size() {
    return size;
  }

  /** Adds every gram of {@code value} under this accumulator's contract, deduplicated. */
  public void addAll(String value) {
    String v = value.toLowerCase(java.util.Locale.ROOT);
    if (contract.shorterThanWindow(v)) {
      encodeWhole(v);
      add();
      return;
    }
    // The rule cuts and this only encodes: one definition of the window,
    // GramRule.forEachWindowUnits.
    contract.family().forEachWindowUnits(contract, v, sink);
  }

  /** Encodes one window the rule produced and adds it, deduplicated. */
  private void encodeWindow(int[] units, int count) {
    java.util.Arrays.fill(key, 0L);
    for (int j = 0; j < count; j++) {
      put(j, units[j]);
    }
    for (int j = count; j < contract.size(); j++) {
      put(j, (int) ABSENT);
    }
    add();
  }

  /** The whole (short) value as one gram: its units, then ABSENT in the unused slots. */
  private void encodeWhole(String v) {
    java.util.Arrays.fill(key, 0L);
    int j = 0;
    if (contract.rule() == Grams.Rule.UTF16_V1) {
      for (int i = 0; i < v.length(); i++) {
        put(j++, v.charAt(i));
      }
    } else {
      for (int i = 0; i < v.length(); ) {
        int cp = v.codePointAt(i);
        put(j++, cp);
        i += Character.charCount(cp);
      }
    }
    for (; j < contract.size(); j++) {
      put(j, (int) ABSENT);
    }
  }

  private void put(int slot, int unit) {
    int word = slot / UNITS_PER_WORD;
    int shift = UNIT_BITS * (UNITS_PER_WORD - 1 - slot % UNITS_PER_WORD);
    key[word] |= ((long) unit & UNIT_MASK) << shift;
    key[0] |= USED;
  }

  private void add() {
    int at = index();
    if (table[at * words] != 0L) {
      return; // already present
    }
    System.arraycopy(key, 0, table, at * words, words);
    if (++size >= growAt) {
      grow();
    }
  }

  private int index() {
    long h = 0;
    for (long word : key) {
      h = (h ^ word) * 0x9E3779B97F4A7C15L;
    }
    int at = (int) (h >>> 40) & mask;
    while (table[at * words] != 0L && !sameAt(at)) {
      at = (at + 1) & mask;
    }
    return at;
  }

  private boolean sameAt(int at) {
    for (int j = 0; j < words; j++) {
      if (table[at * words + j] != key[j]) {
        return false;
      }
    }
    return true;
  }

  private void grow() {
    long[] old = table;
    int capacity = (old.length / words) << 1;
    table = new long[capacity * words];
    mask = capacity - 1;
    growAt = capacity >>> 1;
    for (int slot = 0; slot < old.length / words; slot++) {
      if (old[slot * words] != 0L) {
        System.arraycopy(old, slot * words, key, 0, words);
        System.arraycopy(key, 0, table, index() * words, words);
      }
    }
  }

  /** The distinct grams, decoded. */
  public Set<String> toStrings() {
    Set<String> out = new LinkedHashSet<>(Math.max(16, size * 2));
    for (int slot = 0; slot < table.length / words; slot++) {
      if (table[slot * words] != 0L) {
        out.add(decode(slot));
      }
    }
    return out;
  }

  private String decode(int slot) {
    StringBuilder sb = new StringBuilder(contract.size() * 2);
    for (int j = 0; j < contract.size(); j++) {
      int word = j / UNITS_PER_WORD;
      int shift = UNIT_BITS * (UNITS_PER_WORD - 1 - j % UNITS_PER_WORD);
      int unit = (int) ((table[slot * words + word] >>> shift) & UNIT_MASK);
      if (unit == (int) ABSENT) {
        break;
      }
      if (contract.rule() == Grams.Rule.UTF16_V1) {
        sb.append((char) unit);
      } else {
        sb.appendCodePoint(unit);
      }
    }
    return sb.toString();
  }
}
