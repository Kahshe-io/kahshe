package io.kahshe.analysis.analyzer;

import java.util.ArrayList;
import java.util.List;

/**
 * kahshe-ascii-v3-max&lt;cap&gt;: the pinned, versioned tokenization contract for term indexes.
 *
 * <p>Tokens are maximal runs of ASCII {@code [a-z0-9]} after locale-independent lowercasing;
 * non-ASCII characters terminate a token. v3 adds, additively, the identifiers text carries with
 * punctuation inside them: a maximal run of {@code [a-z0-9.:_-]}, stripped of leading and trailing
 * separators, shaped like an IPv4 or IPv6 address, a UUID or dashed/underscored hex is emitted
 * whole as well as in pieces ({@link #tokensV3}), so nothing that matched under v2 stops matching.
 *
 * <p>The id of the analyzer an index was built under is recorded in that index's metadata, and
 * probes tokenize through the same code, so changed behaviour requires a new id rather than an
 * edit here. Indexability is the per-version admission rule a {@link Contract} carries, and a
 * reader applies the rule of the analyzer the index names; a probe for a token that rule never
 * admitted must not prune. See "The analyzer contract" in the module README.
 */
public final class Analyzer {
  /**
   * The v2 tokens family. The suffix is the token-length cap, because indexability is part of the
   * pinned contract: an index built under one cap does not contain what another would probe for,
   * and absence prunes. Encoding the cap in the id means a reader can tell, rather than assume.
   */
  public static final String ID_PREFIX = "kahshe-ascii-v2-max";

  /** The current tokens family: v2's runs plus compound identifiers, whole. */
  public static final String V3_ID_PREFIX = "kahshe-ascii-v3-max";

  /** The separators a compound identifier may carry between its pieces. */
  public static final String COMPOUND_SEPARATORS = ".:_-";

  /**
   * Longest token that goes into a dictionary, unless a column overrides it. 256 follows
   * Elasticsearch's {@code ignore_above} convention; at that height it is a valve against
   * pathological tokens — a base64 blob, a stack frame run together — rather than a filter on
   * ordinary content.
   */
  public static final int DEFAULT_MAX_TOKEN_LEN = 256;

  /**
   * The first tokens analyzer id. Its admission rule is kept so an index built under it is read
   * under its own contract and keeps pruning, rather than being refused, until it is rebuilt.
   */
  public static final String V1_ID = "kahshe-ascii-v1";

  /** The whole-value analyzer's id prefix: the canonical value is one term, exact and case-sensitive. */
  public static final String VALUE_ID_PREFIX = "kahshe-value-v1-max";

  /** What a column's analyzer makes of a value: ASCII tokens (the default) or the whole value. */
  public enum Kind {
    TOKENS,
    VALUE
  }

  /**
   * What an index's analyzer id says a reader may probe for: the family that owns the id, the
   * kind, the version's admission rule and the token-length cap. Read off the index, never off the
   * reader's configuration, so a probe is never made for a term the build could not have written.
   *
   * <p>Every question below is answered by the {@link AnalyzerFamily}, so a family added through
   * the service loader tokenizes, admits and names its ids without a line changing here. The
   * three-argument constructor is the built-in one: it takes the family {@link Analyzers#builtin}
   * gives for the kind, which is what every caller inside kahshe means.
   */
  public record Contract(AnalyzerFamily family, Kind kind, int version, int maxTokenLen) {
    public Contract {
      if (family == null) {
        throw new IllegalArgumentException("a contract belongs to an analyzer family");
      }
      // A cap of 0 constructs an id ("...-max0") that contractOf cannot parse back, so every
      // reader refuses the index and the term tier prunes nothing -- silently, behind one WARN
      // per load. KAHSHE_MAX_TOKEN_LENGTH=0 is a plausible guess at "unlimited"; it fails here,
      // at startup, with the reason.
      if (maxTokenLen <= 0) {
        throw new IllegalArgumentException(
            "maxTokenLen must be positive (a cap of 0 is not 'unlimited'; the default is "
                + DEFAULT_MAX_TOKEN_LEN + "): " + maxTokenLen);
      }
      if (version <= 0) {
        throw new IllegalArgumentException("analyzer version must be positive: " + version);
      }
      // The family decides the kind; a contract claiming the other one constructs an id that
      // parses back as the family's, so the claim would be silently overwritten on the next read.
      if (kind != family.kind()) {
        throw new IllegalArgumentException(
            "contract kind " + kind + " is not its family's (" + family.kind() + ")");
      }
    }

    /** A contract of the built-in family for {@code kind}; what a kahshe build writes under. */
    public Contract(Kind kind, int version, int maxTokenLen) {
      this(Analyzers.builtin(kind), kind, version, maxTokenLen);
    }

    /** The terms of a value under this contract: its tokens, or the value itself, whole. */
    public List<String> tokens(String text) {
      return family.tokens(this, text);
    }

    /**
     * The terms a query probes for: under v3, each compound the text carries and the pieces not
     * inside one. A file holding a compound holds its pieces, so probing the pieces too would
     * only cost lookups; every other contract probes exactly what it indexes.
     */
    public List<String> queryTerms(String text) {
      return family.queryTerms(this, text);
    }

    /**
     * Whether {@code token} is admitted to the dictionary under this contract — the admission rule
     * of the index's own analyzer, never the reader's configuration. A probe for a token this rule
     * never admitted must not prune: the index's silence about it is not evidence of absence.
     */
    public boolean isIndexable(String token) {
      return family.isIndexable(this, token);
    }

    /**
     * Whether {@code prefix} (already lowercased for a tokens contract) can be the start of a term
     * this contract writes: any string for a whole-value column, only run characters for tokens.
     * A prefix that cannot start a term is not probed, so it prunes nothing.
     */
    public boolean prefixable(String prefix) {
      return family.prefixable(this, prefix);
    }

    /** The id an index built under this contract records; {@link #contractOf} is its inverse. */
    public String id() {
      return family.id(this);
    }
  }

  /** Whether every character is a digit; only v1's admission rule turns on this. */
  static boolean allDigits(String token) {
    for (int i = 0; i < token.length(); i++) {
      char c = token.charAt(i);
      if (c < '0' || c > '9') {
        return false;
      }
    }
    return true;
  }

  /**
   * The contract an analyzer id names, or null for a family this reader does not know.
   *
   * <p>{@link Analyzers} is consulted in registration order — the built-ins first, then whatever
   * the service loader found — and the first family that {@link AnalyzerFamily#owns owns} the id
   * parses it. Null is how a reader is told to keep every file and serve unpruned rather than
   * misread an index it does not understand.
   */
  public static Contract contractOf(String analyzerId) {
    AnalyzerFamily family = Analyzers.owner(analyzerId);
    return family == null ? null : family.parse(analyzerId);
  }

  /** The current contract of {@code kind} at {@code maxTokenLen}: what a build writes today. */
  public static Contract contract(Kind kind, int maxTokenLen) {
    return new Contract(kind, kind == Kind.VALUE ? 1 : 3, maxTokenLen);
  }

  private static final java.util.regex.Pattern IPV4 =
      java.util.regex.Pattern.compile("\\d{1,3}(?:\\.\\d{1,3}){3}");
  private static final java.util.regex.Pattern UUID_SHAPE =
      java.util.regex.Pattern.compile(
          "[0-9a-f]{8}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{12}");
  private static final java.util.regex.Pattern HEX_PIECES =
      java.util.regex.Pattern.compile("[0-9a-f]+(?:[-_][0-9a-f]+)+");

  /**
   * Whether a separator-stripped run has one of v3's compound shapes. Syntactic on purpose: a
   * time like {@code 12:30:45} or a date like {@code 2024-09-02} qualifies, and indexing it whole
   * is additive and harmless. IPv6 is checked by rule rather than regex: hex groups of at most
   * four, two to seven colons, at most one {@code ::}.
   */
  static boolean isCompound(String core) {
    if (IPV4.matcher(core).matches()
        || UUID_SHAPE.matcher(core).matches()
        || HEX_PIECES.matcher(core).matches()) {
      return true;
    }
    int colons = 0;
    int group = 0;
    int digits = 0;
    boolean doubleColon = false;
    for (int i = 0; i < core.length(); i++) {
      char c = core.charAt(i);
      if (c == ':') {
        colons++;
        if (i > 0 && core.charAt(i - 1) == ':') {
          if (doubleColon || (i > 1 && core.charAt(i - 2) == ':')) {
            return false;
          }
          doubleColon = true;
        }
        group = 0;
      } else if ((c >= '0' && c <= '9') || (c >= 'a' && c <= 'f')) {
        if (++group > 4) {
          return false;
        }
        digits++;
      } else {
        return false;
      }
    }
    return colons >= 2 && colons <= 7 && digits > 0;
  }

  /**
   * v3: v2's pieces plus every compound identifier, whole. A compound is a maximal run of
   * {@code [a-z0-9]} and {@link #COMPOUND_SEPARATORS} with leading and trailing separators
   * stripped, when the remainder {@link #isCompound is a shape}; the run's edges are the string
   * edges or any other character. {@link #matchPattern} states the same boundary on the SQL side.
   */
  public static List<String> tokensV3(String text) {
    List<String> out = new ArrayList<>();
    compounds(text, out, true);
    return out;
  }

  private static boolean isRunChar(char c) {
    return (c >= 'a' && c <= 'z') || (c >= '0' && c <= '9') || COMPOUND_SEPARATORS.indexOf(c) >= 0;
  }

  /** v3's query terms: every compound whole, and only the pieces no compound already covers. */
  static List<String> compoundQueryTerms(String text) {
    List<String> out = new ArrayList<>();
    compounds(text, out, false);
    return out;
  }

  private static void compounds(String text, List<String> out, boolean everyPiece) {
    StringBuilder run = new StringBuilder();
    List<String> pieces = new ArrayList<>();
    StringBuilder piece = new StringBuilder();
    for (int i = 0; i <= text.length(); i++) {
      char c = i < text.length() ? text.charAt(i) : ' ';
      if (c >= 'A' && c <= 'Z') {
        c = (char) (c + 32);
      }
      if (isRunChar(c)) {
        run.append(c);
        if (COMPOUND_SEPARATORS.indexOf(c) < 0) {
          piece.append(c);
        } else if (piece.length() > 0) {
          pieces.add(piece.toString());
          piece.setLength(0);
        }
        continue;
      }
      if (piece.length() > 0) {
        pieces.add(piece.toString());
        piece.setLength(0);
      }
      if (run.length() > 0) {
        String core = stripSeparators(run.toString());
        boolean compound = !core.isEmpty() && core.length() < run.length() + 1 && isCompound(core);
        if (everyPiece || !compound) {
          out.addAll(pieces);
        }
        if (compound) {
          out.add(core);
        }
        run.setLength(0);
        pieces.clear();
      }
    }
  }

  private static String stripSeparators(String run) {
    int from = 0;
    int to = run.length();
    while (from < to && COMPOUND_SEPARATORS.indexOf(run.charAt(from)) >= 0) {
      from++;
    }
    while (to > from && COMPOUND_SEPARATORS.indexOf(run.charAt(to - 1)) >= 0) {
      to--;
    }
    return run.substring(from, to);
  }

  /**
   * The canonical {@code regexp_like} pattern meaning "this term is present" — the one shape an
   * engine-side recogniser turns back into a token match. A plain token needs a non-token
   * character or an edge on both sides; a compound needs a character outside the run class on both
   * sides, with any separators between it and the needle. That is exactly the run-and-strip rule
   * of {@link #tokensV3}, so a row the pattern matches is a row the index holds the term for.
   *
   * @throws IllegalArgumentException if {@code needle} is not a term this analyzer could emit
   */
  public static String matchPattern(String needle) {
    if (needle.isEmpty()) {
      throw new IllegalArgumentException("empty needle");
    }
    boolean plain = true;
    for (int i = 0; i < needle.length(); i++) {
      char c = needle.charAt(i);
      if (!isRunChar(c) || (c >= 'A' && c <= 'Z')) {
        throw new IllegalArgumentException("not an analyzer term: " + needle);
      }
      if (COMPOUND_SEPARATORS.indexOf(c) >= 0) {
        plain = false;
      }
    }
    if (plain) {
      return "(^|[^a-z0-9])" + needle + "([^a-z0-9]|$)";
    }
    char first = needle.charAt(0);
    char last = needle.charAt(needle.length() - 1);
    if (COMPOUND_SEPARATORS.indexOf(first) >= 0 || COMPOUND_SEPARATORS.indexOf(last) >= 0) {
      throw new IllegalArgumentException("a compound starts and ends with a letter or digit: " + needle);
    }
    return "(^|[^a-z0-9.:_-])[.:_-]*" + needle.replace(".", "\\.") + "[.:_-]*([^a-z0-9.:_-]|$)";
  }

  /** The cap an id carries after {@code prefix}, or -1 when it carries no positive one. */
  static int capOf(String analyzerId, String prefix) {
    if (analyzerId == null || !analyzerId.startsWith(prefix)) {
      return -1;
    }
    try {
      int len = Integer.parseInt(analyzerId.substring(prefix.length()));
      return len > 0 ? len : -1;
    } catch (NumberFormatException e) {
      return -1;
    }
  }

  private Analyzer() {}

  /** v1 and v2's terms: maximal ASCII {@code [a-z0-9]} runs, lowercased. */
  public static List<String> tokenize(String text) {
    List<String> tokens = new ArrayList<>();
    StringBuilder current = new StringBuilder();
    for (int i = 0; i < text.length(); i++) {
      char c = text.charAt(i);
      if (c >= 'A' && c <= 'Z') {
        c = (char) (c + 32);
      }
      if ((c >= 'a' && c <= 'z') || (c >= '0' && c <= '9')) {
        current.append(c);
      } else if (current.length() > 0) {
        tokens.add(current.toString());
        current.setLength(0);
      }
    }
    if (current.length() > 0) {
      tokens.add(current.toString());
    }
    return tokens;
  }




}
