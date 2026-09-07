package io.kahshe.indexer.maintain;

import java.util.ArrayList;
import java.util.List;

/**
 * Which member of an indexer fleet this process is, and the claim order that follows from it.
 *
 * <p>The unit of scale is the column: N replicas each poll the watched tables and take a column's
 * {@link io.kahshe.format.BuildLease} when its index is behind, skipping the columns another
 * member already holds. Nothing coordinates them, so an identical claim order would have every
 * replica race the same column first and then skip its way down the same list — correct, and no
 * faster than one replica. Rotating each replica's order by its own ordinal starts them at
 * different columns.
 *
 * <p>The ordinal is configuration when set, otherwise derived from the hostname. It only has to be
 * stable for a pod's life: this is a spread, not an assignment, and two members landing on the
 * same ordinal costs a skip, never a wrong answer.
 */
public final class Fleet {
  private Fleet() {}

  /**
   * The ordinal for this process: {@code configured} when non-negative, else derived from
   * {@code hostname} — a trailing {@code -N} is read as the ordinal directly (a StatefulSet's pod
   * name), and any other name hashes to a stable value.
   *
   * @param configured the configured ordinal, or a negative number to derive one
   * @param hostname this process's hostname, which may be null
   * @return a non-negative ordinal
   */
  public static int ordinal(int configured, String hostname) {
    if (configured >= 0) {
      return configured;
    }
    if (hostname == null || hostname.isBlank()) {
      return 0;
    }
    int dash = hostname.lastIndexOf('-');
    if (dash >= 0 && dash < hostname.length() - 1) {
      String tail = hostname.substring(dash + 1);
      if (tail.chars().allMatch(Character::isDigit) && tail.length() <= 9) {
        return Integer.parseInt(tail);
      }
    }
    return Math.floorMod(hostname.hashCode(), 1024);
  }

  /** This process's ordinal, from the environment's {@code HOSTNAME}. */
  public static int ordinal(int configured) {
    return ordinal(configured, System.getenv("HOSTNAME"));
  }

  /**
   * {@code items} rotated left by {@code ordinal}, so each member starts at a different one.
   * Every item is still visited, in the same cycle — a member that finds every column held has
   * still checked them all.
   *
   * @param items the claim order shared by every member
   * @param ordinal this member's ordinal
   * @return the same items, rotated
   */
  public static <T> List<T> rotate(List<T> items, int ordinal) {
    int n = items.size();
    if (n <= 1 || ordinal <= 0) {
      return items;
    }
    int start = Math.floorMod(ordinal, n);
    if (start == 0) {
      return items;
    }
    List<T> rotated = new ArrayList<>(n);
    for (int i = 0; i < n; i++) {
      rotated.add(items.get((start + i) % n));
    }
    return List.copyOf(rotated);
  }
}
