package io.kahshe.format.type;

import java.util.ArrayList;
import java.util.List;
import java.util.ServiceLoader;
import io.kahshe.format.type.bloom.BloomIndexType;
import io.kahshe.format.type.gram.GramIndexType;
import io.kahshe.format.type.term.TermIndexType;

/**
 * The registry every build and every plan iterates instead of naming the tiers.
 *
 * <p>Types are discovered with {@link ServiceLoader}; the built-ins are declared the same way, in
 * {@code META-INF/services/io.kahshe.format.type.IndexType}. {@link #COST_ORDER} fixes the order the
 * built-ins are consulted in — cheapest proof first — and anything discovered beyond them follows,
 * in discovery order.
 */
public final class IndexTypes {

  /** The built-ins, cheapest proof first. A discovered type outside this list runs after them. */
  public static final List<String> COST_ORDER =
      List.of(BloomIndexType.KEY, GramIndexType.KEY, TermIndexType.KEY);

  private static final List<IndexType> REGISTERED = discover();

  private IndexTypes() {}

  /** Every registered type, built-ins first in {@link #COST_ORDER}. */
  public static List<IndexType> inCostOrder() {
    return REGISTERED;
  }

  /**
   * The collectors that want to see this build's rows, in cost order. A type answering
   * {@link IndexType.Collector#NONE} is left out, so a build with only built-ins calls nothing per
   * row: the three built-ins collect inside the read itself, through the accumulators the read
   * pass already owns.
   */
  public static IndexType.Collector[] collectors(IndexType.BuildContext ctx) {
    List<IndexType.Collector> wanted = new ArrayList<>();
    for (IndexType type : REGISTERED) {
      IndexType.Collector collector = type.collector(ctx);
      if (collector != null && collector != IndexType.Collector.NONE) {
        wanted.add(collector);
      }
    }
    return wanted.toArray(new IndexType.Collector[0]);
  }

  private static List<IndexType> discover() {
    List<IndexType> found = new ArrayList<>();
    for (IndexType type : ServiceLoader.load(IndexType.class, IndexTypes.class.getClassLoader())) {
      found.add(type);
    }
    found.sort(java.util.Comparator.comparingInt(type -> rank(type.key())));
    return List.copyOf(found);
  }

  /** A built-in ranks by its place in the cost order; anything else sorts after all of them. */
  private static int rank(String key) {
    int at = COST_ORDER.indexOf(key);
    return at < 0 ? COST_ORDER.size() : at;
  }
}
