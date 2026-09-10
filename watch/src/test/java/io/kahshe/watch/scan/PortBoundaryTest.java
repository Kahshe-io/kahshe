package io.kahshe.watch.scan;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertTrue;

import java.io.IOException;
import java.net.URISyntaxException;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.ArrayList;
import java.util.List;
import java.util.Set;
import java.util.TreeSet;
import java.util.stream.Stream;

import org.junit.jupiter.api.Test;

/**
 * The port boundary, enforced rather than measured.
 *
 * <p>The claim this pins: the watcher's evaluation path — the rules, the conditions, the windows,
 * the row test, the scanner seam, the sinks — is written against {@link TableView}, {@link Row}
 * and {@link io.kahshe.analysis.ValueKind}, and names no table format at all. Iceberg lives in
 * four classes, and they are listed below by name. If that holds, serving Delta or any other
 * Log-Structured Table is a reader and a type binding, not a rewrite.
 *
 * <p>It is deny-by-default on purpose: every compiled class in the module is checked, so a new
 * scanner or a new rule form is covered the day it is written, without anyone remembering to add
 * it here. Making it pass by editing {@link #THE_ICEBERG_IMPLEMENTATION} is the deliberate act
 * the test exists to force — that list IS the port surface, and the work is to shorten it, not to
 * lengthen it.
 *
 * <p>Class files are read rather than sources because a constant pool catches a type used in a
 * signature, a field, a cast, a lambda or an annotation alike, and a grep for the import does not.
 * Verified red by declaring an {@code org.apache.iceberg.Schema} field on {@link RuleScanner}.
 */
class PortBoundaryTest {

  /**
   * The only classes allowed to name Iceberg: the reader and its row adapter, plus the discovery
   * and delivery pollers that talk to the catalog. A second format brings its own; nothing else in
   * the module may.
   *
   * <p>{@code FileScanContext} must never join it: it is the seam's ARGUMENT, handed to every
   * scanner anyone writes, so an Iceberg type reachable through it makes the seam Iceberg's. What
   * the pass's reader needs from the format it holds directly.
   */
  private static final Set<String> THE_ICEBERG_IMPLEMENTATION = Set.of(
      "io/kahshe/watch/TableDiscovery",
      "io/kahshe/watch/ReportPoller",
      "io/kahshe/watch/scan/ScanPass",
      "io/kahshe/watch/scan/HuntPass",
      "io/kahshe/watch/scan/ProjectedRow");

  @Test
  void onlyTheReaderAndTheCatalogPollersNameATableFormat() throws IOException, URISyntaxException {
    Path root = compiledClasses();
    Set<String> offenders = new TreeSet<>();
    int checked = 0;

    try (Stream<Path> tree = Files.walk(root.resolve("io/kahshe/watch"))) {
      for (Path file : tree.filter(p -> p.toString().endsWith(".class")).toList()) {
        String name = root.relativize(file).toString().replace('\\', '/').replaceFirst("\\.class$", "");
        String topLevel = name.contains("$") ? name.substring(0, name.indexOf('$')) : name;
        checked++;
        if (THE_ICEBERG_IMPLEMENTATION.contains(topLevel)) {
          continue;
        }
        if (new String(Files.readAllBytes(file), StandardCharsets.ISO_8859_1)
            .contains("org/apache/iceberg")) {
          offenders.add(topLevel);
        }
      }
    }

    assertTrue(checked > 25, "only " + checked + " classes walked from " + root
        + ": the test found nothing to check and would pass for the wrong reason");
    assertTrue(offenders.isEmpty(),
        "the evaluation path must name no table format, or serving a second Log-Structured Table "
            + "is a rewrite rather than a reader. Reaching Iceberg from " + offenders
            + " — take what is needed through TableView, Row and ValueKind instead. If this class "
            + "genuinely IS the format implementation, add it to THE_ICEBERG_IMPLEMENTATION and "
            + "say in the commit why the port surface grew.");
  }

  @Test
  void theAllowedListNamesClassesThatExistAndStillNeedIt() throws IOException, URISyntaxException {
    Path root = compiledClasses();
    List<String> stale = new ArrayList<>();
    for (String name : THE_ICEBERG_IMPLEMENTATION) {
      Path file = root.resolve(name + ".class");
      if (!Files.exists(file)) {
        stale.add(name + " (no such class)");
      } else if (!new String(Files.readAllBytes(file), StandardCharsets.ISO_8859_1)
          .contains("org/apache/iceberg")) {
        stale.add(name + " (no longer reaches Iceberg — the port surface shrank, shrink the list)");
      }
    }
    assertEquals(List.of(), stale,
        "the port surface is the thing this list measures, so it must not drift: " + stale);
  }

  /** The directory this module's classes were compiled to, found through a class in it. */
  private static Path compiledClasses() throws URISyntaxException {
    return Path.of(Scanner.class.getProtectionDomain().getCodeSource().getLocation().toURI());
  }
}
