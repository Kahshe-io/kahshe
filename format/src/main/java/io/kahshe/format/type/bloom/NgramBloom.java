package io.kahshe.format.type.bloom;

import java.nio.ByteBuffer;
import java.nio.charset.StandardCharsets;
import java.util.Set;
import io.kahshe.format.type.gram.Grams;

/**
 * A bloom filter over lowercase character n-grams of a string column's values, one per data file.
 *
 * <p>Probe semantics are strictly "definitely not present": {@link #mightContain(String, Mode)}
 * returning false proves no value in the file can satisfy the predicate, so the file may be
 * pruned. True means nothing. Literals shorter than the gram size can only be probed for EQ
 * (whole values shorter than the gram size are indexed as a single gram); CONTAINS/STARTS_WITH
 * probes of short literals return true (no pruning).
 */
public final class NgramBloom {
  private static final int MAGIC = 0x504F4E42; // "PONB"

  public enum Mode {
    EQ,
    CONTAINS,
    STARTS_WITH
  }

  private final Grams.Contract contract;
  private final int numHashes;
  private final long[] bits;

  private NgramBloom(Grams.Contract contract, int numHashes, long[] bits) {
    this.contract = contract;
    this.numHashes = numHashes;
    this.bits = bits;
  }

  public static NgramBloom build(Set<String> grams, Grams.Contract contract, double fpp) {
    int n = Math.max(grams.size(), 1);
    long m = (long) Math.ceil(-n * Math.log(fpp) / (Math.log(2) * Math.log(2)));
    m = ((m + 63) / 64) * 64;
    int k = Math.max(1, Math.min(8, (int) Math.round((double) m / n * Math.log(2))));
    NgramBloom bloom = new NgramBloom(contract, k, new long[(int) (m / 64)]);
    grams.forEach(bloom::insert);
    return bloom;
  }

  /** The grams of a value under {@code contract}; see {@link Grams.Contract#gramsOf}. */
  public static Set<String> gramsOf(String value, Grams.Contract contract) {
    return contract.gramsOf(value);
  }

  public boolean mightContain(String literal, Mode mode) {
    // any lowercase form present keeps the file: kept files stay a superset of true matches
    for (String form : variants(literal)) {
      if (mightContainForm(form, mode)) {
        return true;
      }
    }
    return false;
  }

  /**
   * Lowercase probe forms of a literal, shared by the bloom and gram probe paths. Grams are built
   * from values lowercased whole, and lowercasing is context-sensitive (Greek capital sigma lowers
   * to 'ς' word-finally but 'σ' medially), so a literal cut from a value's interior can lowercase
   * differently standalone than it did in context; probing every form keeps a probe from pruning a
   * truly-matching file.
   *
   * <p>Context is recovered on both sides because Java's Final_Sigma rule reads what precedes the
   * sigma as well as what follows it.
   */
  public static java.util.List<String> variants(String literal) {
    // A sentinel is a cased letter, so it makes the adjacent position non-word-boundary; it
    // lowercases to itself and to exactly one char, so stripping by index is sound.
    String lead = "α" + literal;
    String trail = literal + "α";
    String both = "α" + literal + "α";
    java.util.List<String> forms = new java.util.ArrayList<>(4);
    for (String form :
        new String[] {
          literal.toLowerCase(java.util.Locale.ROOT),
          strip(trail.toLowerCase(java.util.Locale.ROOT), 0, 1),
          strip(lead.toLowerCase(java.util.Locale.ROOT), 1, 0),
          strip(both.toLowerCase(java.util.Locale.ROOT), 1, 1)
        }) {
      if (!forms.contains(form)) {
        forms.add(form);
      }
    }
    return forms;
  }

  private static String strip(String padded, int fromStart, int fromEnd) {
    return padded.substring(fromStart, padded.length() - fromEnd);
  }

  private boolean mightContainForm(String v, Mode mode) {
    if (contract.shorterThanWindow(v)) {
      // Short literals: only EQ is probeable (whole short values were indexed as one gram).
      return mode != Mode.EQ || mightContainGram(v);
    }
    boolean[] all = {true};
    contract.forEachWindow(v, gram -> {
      if (all[0] && !mightContainGram(gram)) {
        all[0] = false;
      }
    });
    return all[0];
  }

  private void insert(String gram) {
    long h1 = fnv1a(gram);
    long h2 = mix(h1) | 1L;
    long numBits = bits.length * 64L;
    for (int i = 0; i < numHashes; i++) {
      long bit = Math.floorMod(h1 + i * h2, numBits);
      bits[(int) (bit >>> 6)] |= 1L << (bit & 63);
    }
  }

  private boolean mightContainGram(String gram) {
    long h1 = fnv1a(gram);
    long h2 = mix(h1) | 1L;
    long numBits = bits.length * 64L;
    for (int i = 0; i < numHashes; i++) {
      long bit = Math.floorMod(h1 + i * h2, numBits);
      if ((bits[(int) (bit >>> 6)] & (1L << (bit & 63))) == 0) {
        return false;
      }
    }
    return true;
  }

  public byte[] serialize() {
    ByteBuffer buffer = ByteBuffer.allocate(4 + 4 + 4 + 8 + bits.length * 8);
    buffer.putInt(MAGIC).putInt(contract.size()).putInt(numHashes).putLong(bits.length * 64L);
    for (long word : bits) {
      buffer.putLong(word);
    }
    return buffer.array();
  }

  /**
   * A blob carries its size, not its rule: the rule is the metadata document's (docs/FORMAT.md
   * §3.1), so the reader passes it in; a blob from before the rule had an id is v1.
   */
  public static NgramBloom deserialize(byte[] bytes, Grams.Rule rule) {
    ByteBuffer buffer = ByteBuffer.wrap(bytes);
    if (buffer.getInt() != MAGIC) {
      throw new IllegalArgumentException("not an ngram-bloom blob");
    }
    int ngram = buffer.getInt();
    int numHashes = buffer.getInt();
    long[] bits = new long[(int) (buffer.getLong() / 64)];
    for (int i = 0; i < bits.length; i++) {
      bits[i] = buffer.getLong();
    }
    return new NgramBloom(new Grams.Contract(rule, ngram), numHashes, bits);
  }

  public int sizeBytes() {
    return 20 + bits.length * 8;
  }

  public int ngram() {
    return contract.size();
  }

  private static long fnv1a(String s) {
    long hash = 0xcbf29ce484222325L;
    for (byte b : s.getBytes(StandardCharsets.UTF_8)) {
      hash ^= b & 0xff;
      hash *= 0x100000001b3L;
    }
    return hash;
  }

  // 64-bit finalizer (splitmix64) to derive an independent second hash
  private static long mix(long z) {
    z = (z ^ (z >>> 30)) * 0xbf58476d1ce4e5b9L;
    z = (z ^ (z >>> 27)) * 0x94d049bb133111ebL;
    return z ^ (z >>> 31);
  }
}
