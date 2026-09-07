package io.kahshe.common;

import java.nio.charset.StandardCharsets;
import java.security.MessageDigest;
import java.security.NoSuchAlgorithmException;
import java.util.Base64;

/**
 * The one digest kahshe uses: cache keys that must not hold the credential they were made from.
 */
public final class Hashing {
  private Hashing() {}

  /** Base64 of the SHA-256 of {@code value}'s UTF-8 bytes. */
  public static String sha256Base64(String value) {
    try {
      byte[] digest = MessageDigest.getInstance("SHA-256").digest(value.getBytes(StandardCharsets.UTF_8));
      return Base64.getEncoder().encodeToString(digest);
    } catch (NoSuchAlgorithmException e) {
      throw new IllegalStateException("SHA-256 is mandatory in every JVM", e);
    }
  }
}
