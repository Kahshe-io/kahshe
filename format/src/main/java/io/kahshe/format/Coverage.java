package io.kahshe.format;

import com.fasterxml.jackson.databind.JsonNode;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;

/**
 * Which data files an index covers, and the ordinal each one is known by. The ordinal is the join
 * key between this list and every bitmap in the gram and term tiers, so it is the single identity
 * the whole index format rests on.
 *
 * <p>An ordinal is allocated, not positional. A departed file keeps its slot, marked dead, and new
 * files are numbered from one past the highest ordinal ever issued, so nothing an existing bitmap
 * says has to change. A path that returns is revived at its old ordinal rather than renumbered:
 * Iceberg data files are immutable, so the bitmaps naming it still describe it.
 */
public final class Coverage {

  /** One covered data file: its path, the ordinal it is known by, and whether it still exists. */
  public record Entry(String path, int ordinal, boolean live) {
    Entry withLive(boolean nowLive) {
      return live == nowLive ? this : new Entry(path, ordinal, nowLive);
    }
  }

  private Coverage() {}

  /**
   * Reads the metadata's {@code files} node, accepting both shapes: a legacy plain array of path
   * strings, whose order was the numbering and which reads back as "ordinal = position, everything
   * live", and the current array of objects carrying the ordinal explicitly. They are told apart
   * per element rather than by a version field.
   */
  public static List<Entry> parse(JsonNode filesNode) {
    List<Entry> entries = new ArrayList<>();
    if (filesNode == null || !filesNode.isArray()) {
      return entries;
    }
    int position = 0;
    for (JsonNode node : filesNode) {
      if (node.isTextual()) {
        entries.add(new Entry(node.asText(), position, true));
      } else if (node.isObject()) {
        String path = node.path("path").asText("");
        if (!path.isEmpty()) {
          // An entry with no explicit ordinal is malformed rather than legacy; fall back to its
          // position, which is what a legacy document would have meant by the same slot.
          entries.add(
              new Entry(path, node.path("ordinal").asInt(position), node.path("live").asBoolean(true)));
        }
      }
      position++;
    }
    return entries;
  }

  /** The JSON form written today: ordinal explicit, liveness explicit. */
  public static List<Object> toJson(List<Entry> entries) {
    List<Object> out = new ArrayList<>(entries.size());
    for (Entry entry : entries) {
      Map<String, Object> node = new LinkedHashMap<>();
      node.put("path", entry.path());
      node.put("ordinal", entry.ordinal());
      node.put("live", entry.live());
      out.add(node);
    }
    return out;
  }

  /**
   * Path to ordinal for live entries only. Omitting dead entries is what makes a tombstone safe: a
   * path absent from this map is kept by every pruner, so the worst a stale tombstone can do is
   * cost a scan.
   */
  public static Map<String, Integer> ordinalOfLive(List<Entry> entries) {
    Map<String, Integer> ordinalOf = new HashMap<>();
    for (Entry entry : entries) {
      if (entry.live()) {
        Integer twice = ordinalOf.put(entry.path(), entry.ordinal());
        if (twice != null) {
          // Two live ordinals for one path is a corrupt coverage: bitmaps were written for one of
          // them, and a probe resolving the other finds nothing and PRUNES the file. A build once
          // published exactly this, and which ordinal won depended on map order nobody stated.
          // Refusing keeps every file for the column: the loud, safe direction.
          throw new IllegalStateException("coverage lists " + entry.path() + " as live twice, at "
              + "ordinals " + twice + " and " + entry.ordinal() + "; rebuild the column in full");
        }
      }
    }
    return ordinalOf;
  }

  public static List<String> livePaths(List<Entry> entries) {
    List<String> paths = new ArrayList<>();
    for (Entry entry : entries) {
      if (entry.live()) {
        paths.add(entry.path());
      }
    }
    return paths;
  }

  public static boolean hasTombstones(List<Entry> entries) {
    return entries.stream().anyMatch(entry -> !entry.live());
  }

  /**
   * One past the highest ordinal ever issued, live or dead. Dead slots are counted: reusing a dead
   * ordinal would hand a new file the bitmaps of the file that used to hold it.
   */
  public static int nextOrdinal(List<Entry> entries) {
    int max = -1;
    for (Entry entry : entries) {
      max = Math.max(max, entry.ordinal());
    }
    return max + 1;
  }

  /**
   * The result of renumbering: the surviving entries, and how to translate an old ordinal.
   *
   * <p>{@code remap} maps an old ordinal to its new one, or to -1 for an ordinal that no live file
   * holds any more. Every bitmap in the gram and term tiers has to be rewritten through it in the
   * same build that adopts the new coverage, or the two disagree about what a number means.
   */
  public record Renumbered(List<Entry> entries, Map<Integer, Integer> remap) {}

  /**
   * Drops dead entries and renumbers the survivors contiguously from zero, in old-ordinal order —
   * so the numbering is a function of the surviving set rather than of the call order.
   *
   * <p>Only safe when every bitmap in the gram and term tiers is rewritten through
   * {@link Renumbered#remap} in the same build; otherwise the two disagree about what a number
   * means.
   */
  public static Renumbered renumberLive(List<Entry> entries) {
    List<Entry> live = new ArrayList<>();
    for (Entry entry : entries) {
      if (entry.live()) {
        live.add(entry);
      }
    }
    live.sort(java.util.Comparator.comparingInt(Entry::ordinal));
    Map<Integer, Integer> remap = new HashMap<>();
    List<Entry> out = new ArrayList<>(live.size());
    for (int i = 0; i < live.size(); i++) {
      remap.put(live.get(i).ordinal(), i);
      out.add(new Entry(live.get(i).path(), i, true));
    }
    return new Renumbered(out, remap);
  }

  /** The dead share of a coverage list, as a percentage. */
  public static int deadPercent(List<Entry> entries) {
    if (entries.isEmpty()) {
      return 0;
    }
    long dead = entries.stream().filter(e -> !e.live()).count();
    return (int) (100L * dead / entries.size());
  }

  /**
   * Re-marks liveness against the table's current file set: departed files are tombstoned,
   * returning ones revived. Order and ordinals are untouched.
   */
  public static List<Entry> reconcile(List<Entry> entries, java.util.Set<String> currentPaths) {
    List<Entry> out = new ArrayList<>(entries.size());
    for (Entry entry : entries) {
      out.add(entry.withLive(currentPaths.contains(entry.path())));
    }
    return out;
  }
}
