package io.kahshe.watch.scan;

import java.util.ArrayList;
import java.util.Comparator;
import java.util.List;
import java.util.ServiceLoader;

/**
 * The registry {@link ScanPass} iterates instead of naming the rule scanner.
 *
 * <p>Scanners are discovered with {@link ServiceLoader}; the built-in is declared the same way,
 * in {@code META-INF/services/io.kahshe.watch.scan.Scanner}. {@link #BUILT_INS} fixes the order
 * the built-ins run in and anything discovered beyond them follows, in discovery order — the same
 * rule {@code IndexTypes} uses, so a scanner kahshe does not ship joins by adding a jar.
 */
public final class Scanners {

  /** The built-ins, in the order the pass consults them. */
  public static final List<String> BUILT_INS = List.of(RuleScanner.NAME, WindowScanner.NAME);

  private Scanners() {}

  /** Every registered scanner, built-ins first, each already {@link Scanner#configure configured}. */
  public static List<Scanner> discover(ScanContext ctx) {
    List<Scanner> found = new ArrayList<>();
    for (Scanner scanner : ServiceLoader.load(Scanner.class, Scanners.class.getClassLoader())) {
      found.add(scanner);
    }
    found.sort(Comparator.comparingInt(scanner -> rank(scanner.name())));
    for (Scanner scanner : found) {
      scanner.configure(ctx);
    }
    return List.copyOf(found);
  }

  /** A built-in ranks by its place in the list; anything else sorts after all of them. */
  private static int rank(String name) {
    int at = BUILT_INS.indexOf(name);
    return at < 0 ? BUILT_INS.size() : at;
  }
}
