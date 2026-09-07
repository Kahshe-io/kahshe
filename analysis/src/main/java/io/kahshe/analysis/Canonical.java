package io.kahshe.analysis;

import java.nio.ByteBuffer;
import java.util.UUID;

/**
 * The one string form an indexed value has, on both sides of the index.
 *
 * <p>The index stores strings, so a non-string value is indexed by its canonical string form and a
 * predicate's literal must be canonicalised the same way; canonicalised any other way, a literal
 * finds its value absent and prunes the file that holds it. One method therefore serves both the
 * build (values) and the pruner (literals). The forms are part of the pinned analyzer contract —
 * changing one changes what every existing index of that type means — and are listed under
 * "Canonical forms" in the module README.
 */
public final class Canonical {
  private static final char[] HEX = "0123456789abcdef".toCharArray();

  private Canonical() {}

  /**
   * Whether a form is defined for this kind at all — what a build asks before it indexes a column,
   * and the exact complement of the kinds {@link #form} answers null for.
   */
  public static boolean indexable(ValueKind kind) {
    return switch (kind) {
      case STRING, INTEGRAL, DECIMAL, UUID, BINARY -> true;
      default -> false;
    };
  }

  /**
   * The canonical form of {@code value} for a column of {@code kind}, or null when the kind has no
   * form or the value will not parse into one. Callers read null as "not indexable": the build
   * refuses the column, the pruner keeps every file. A literal arriving as text for a non-string
   * column — a JSON plan filter writes UUIDs as strings — is parsed to the column's type first.
   */
  public static String form(ValueKind kind, Object value) {
    if (value == null) {
      return null;
    }
    return switch (kind) {
      case STRING -> value.toString();
      case INTEGRAL -> value instanceof Number n ? Long.toString(n.longValue())
          : parseLong(value.toString());
      // Trailing zeros stripped, so 12.50 (the column's scale) and 12.5 (a literal as typed) are
      // one form; an integer-valued id stays its digits.
      case DECIMAL -> parseDecimal(value.toString());
      case UUID -> {
        UUID uuid = value instanceof UUID u ? u : parseUuid(value.toString());
        yield uuid == null ? null : undashed(uuid);
      }
      case BINARY -> {
        if (value instanceof ByteBuffer buffer) {
          yield hex(buffer);
        }
        if (value instanceof byte[] bytes) {
          yield hex(ByteBuffer.wrap(bytes));
        }
        yield null;
      }
      default -> null;
    };
  }

  private static String parseLong(String text) {
    try {
      return Long.toString(Long.parseLong(text.trim()));
    } catch (NumberFormatException e) {
      return null;
    }
  }

  private static String parseDecimal(String text) {
    try {
      return new java.math.BigDecimal(text.trim()).stripTrailingZeros().toPlainString();
    } catch (NumberFormatException e) {
      return null;
    }
  }

  private static UUID parseUuid(String text) {
    try {
      return UUID.fromString(text.trim());
    } catch (IllegalArgumentException e) {
      return null;
    }
  }

  private static String undashed(UUID uuid) {
    return uuid.toString().replace("-", "");
  }

  private static String hex(ByteBuffer buffer) {
    ByteBuffer view = buffer.duplicate();
    StringBuilder out = new StringBuilder(view.remaining() * 2);
    while (view.hasRemaining()) {
      int b = view.get() & 0xff;
      out.append(HEX[b >>> 4]).append(HEX[b & 0xf]);
    }
    return out.toString();
  }
}
