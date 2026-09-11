package io.kahshe.format;

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
 * The query seam's boundary, enforced rather than intended.
 *
 * <p>The claim this pins: {@code format} states facts about FILES, and the vocabulary of the
 * engine that is asking is not part of that. The module graph already stops half of it — these
 * sources compile against {@code common} and {@code analysis} only, so no {@code watch},
 * {@code proxy} or {@code indexer} type can reach in. What the graph cannot stop is a SHARED
 * third-party planning type, and one was in the tier interface until the partition seam landed:
 * {@link io.kahshe.format.type.IndexType} took and returned {@code List<FileScanTask>}, which is
 * Iceberg's PLANNING vocabulary rather than its table format, so every tier named the concern of
 * the one caller that happened to be first.
 *
 * <p>What is NOT asserted, deliberately: that {@code format} names no Iceberg at all. It legitimately
 * does — the index is keyed by field ids from an Iceberg schema and its leaves live in
 * Iceberg-managed storage through {@code FileIO}. The line is between describing a table and
 * describing a SCAN OF one.
 *
 * <p>Its honest limit is that a constant pool catches a TYPE and the rule is really about NAMES:
 * nothing here would stop a parameter called {@code tasks}, a method called {@code alert} or a
 * record called {@code Evidence}. Half the boundary is mechanical; the other half is review.
 *
 * <p>Class files are read rather than sources for {@code watch}'s {@code PortBoundaryTest}'s
 * reason — that test is this one's model: a constant pool catches a type used in a signature, a
 * field, a cast, a lambda or an
 * annotation alike, and a grep for the import does not. Verified red by returning
 * {@code List<FileScanTask>} from a tier again.
 */
class QueryApiBoundaryTest {

  /**
   * Iceberg's scan-planning vocabulary. These describe a QUERY over a table, not the table, so an
   * index tier that names one has taken on its caller's concern.
   */
  private static final List<String> PLANNING_TYPES = List.of(
      "org/apache/iceberg/FileScanTask",
      "org/apache/iceberg/ContentScanTask",
      "org/apache/iceberg/CombinedScanTask",
      "org/apache/iceberg/ScanTask",
      "org/apache/iceberg/TableScan",
      "org/apache/iceberg/DeleteFile");

  /**
   * The only class allowed to name them: the adapter that turns one caller's scan tasks into plan
   * ordinals and back. It is one name on purpose — the seam has exactly one translation point, and
   * a second entry here means a tier has started speaking a caller's language again.
   */
  private static final Set<String> THE_PLANNING_ADAPTER = Set.of("io/kahshe/format/IndexPruner");

  @Test
  void onlyThePrunerNamesIcebergsScanPlanningVocabulary() throws IOException, URISyntaxException {
    Path root = compiledClasses();
    Set<String> offenders = new TreeSet<>();
    int checked = 0;

    try (Stream<Path> tree = Files.walk(root.resolve("io/kahshe/format"))) {
      for (Path file : tree.filter(p -> p.toString().endsWith(".class")).toList()) {
        String name =
            root.relativize(file).toString().replace('\\', '/').replaceFirst("\\.class$", "");
        String topLevel = name.contains("$") ? name.substring(0, name.indexOf('$')) : name;
        checked++;
        if (THE_PLANNING_ADAPTER.contains(topLevel)) {
          continue;
        }
        String pool = new String(Files.readAllBytes(file), StandardCharsets.ISO_8859_1);
        for (String planning : PLANNING_TYPES) {
          if (pool.contains(planning)) {
            offenders.add(topLevel + " names " + planning);
          }
        }
      }
    }

    assertTrue(checked > 40, "only " + checked + " classes walked from " + root
        + ": the test found nothing to check and would pass for the wrong reason");
    assertEquals(List.of(), List.copyOf(offenders),
        "the index answers about FILES; a scan task is the asking engine's concept, and a tier "
            + "that names one has taken on the concern of whichever caller came first — which is "
            + "how the hit/unknown fusion got into the seam. Answer in plan ordinals "
            + "(IndexType.FileSet) and let IndexPruner translate. If this class genuinely IS the "
            + "adapter, add it to THE_PLANNING_ADAPTER and say in the commit why the seam grew a "
            + "second translation point.");
  }

  /**
   * The allowlist measures the seam, so it must not outlive it. If the pruner ever stops naming a
   * scan task, the translation moved somewhere this test is not looking.
   */
  @Test
  void theAdapterOnTheListStillNeedsToBeOnIt() throws IOException, URISyntaxException {
    Path root = compiledClasses();
    List<String> stale = new ArrayList<>();
    for (String name : THE_PLANNING_ADAPTER) {
      Path file = root.resolve(name + ".class");
      if (!Files.exists(file)) {
        stale.add(name + " (no such class)");
      } else {
        String pool = new String(Files.readAllBytes(file), StandardCharsets.ISO_8859_1);
        if (PLANNING_TYPES.stream().noneMatch(pool::contains)) {
          stale.add(name + " (no longer translates scan tasks — find where that moved)");
        }
      }
    }
    assertEquals(List.of(), stale,
        "the seam is the thing this list measures, so it must not drift: " + stale);
  }

  /**
   * The half a constant pool cannot see, and the half that failed first.
   *
   * <p>The type check above passes on a `format` whose javadoc says "watch's HuntPass keeps the
   * three verdicts apart" — no type, no import, nothing in the class file, and the boundary broken
   * all the same, because the next person writing a tier now knows which consumer to design for.
   * That sentence was written into this module by the same change that added the test above it,
   * and a reader caught it rather than the suite. So the names are checked too, over SOURCES,
   * where comments live.
   *
   * <p>It is a denylist and not deny-by-default, which is the honest weakness: it catches the
   * consumers that exist, not a concept nobody has named yet. A type check can be total because
   * the compiler enumerates types; prose cannot be, so this narrows the gap rather than closing
   * it. The rest is still review.
   */
  @Test
  void formatNamesNoConsumerOfIt() throws IOException, URISyntaxException {
    Path sources = mainSources();
    List<String> offenders = new ArrayList<>();
    int checked = 0;

    try (Stream<Path> tree = Files.walk(sources)) {
      for (Path file : tree.filter(p -> p.toString().endsWith(".java")).toList()) {
        checked++;
        String text = Files.readString(file);
        String name = sources.relativize(file).toString();
        for (String consumer : CONSUMER_VOCABULARY) {
          if (text.contains(consumer) && !allowed(name, consumer)) {
            offenders.add(name + " names " + consumer);
          }
        }
      }
    }

    assertTrue(checked > 25, "only " + checked + " sources walked from " + sources
        + ": the test found nothing to check and would pass for the wrong reason");
    assertEquals(List.of(), offenders,
        "the index states facts about FILES and must not name who is asking -- not in a type, and "
            + "not in a sentence either. Describe the READING a caller might take, not the caller: "
            + "\"keep it where examining it is merely slower\" survives a consumer nobody has "
            + "written yet, \"a hunt must scan it\" does not. Offenders: " + offenders);
  }

  /**
   * Names belonging to something that CONSUMES the index. Class names and package prefixes rather
   * than concept words, because "rule", "plan" and "evidence" appear in honest prose about the
   * format itself and a check that cries wolf gets deleted.
   */
  private static final List<String> CONSUMER_VOCABULARY = List.of(
      "HuntPass", "WatchEngine", "ScanPass", "RuleScanner", "WatchRule", "ConfirmationSql",
      "CountRoutes", "PlanService", "PlanRoutes", "KahsheHandler", "IndexerService",
      "io.kahshe.watch", "io.kahshe.proxy", "io.kahshe.app");

  /**
   * The one standing exception, recorded rather than silently skipped. {@code BuildReport} carries
   * an {@code alerts} array: consumer vocabulary, and it predates this rule. It is a PERSISTED
   * JSON field name, so renaming it is a format migration rather than a tidy-up -- a name persisted
   * in an artifact is a migration, and that is decided deliberately -- and the module knows only
   * that the document has a list of opaque maps, never what an alert is. It stays until a format change is being made for
   * another reason. Nothing else may join this list without the same kind of argument.
   */
  private static boolean allowed(String file, String consumer) {
    // this test's own denylist and prose necessarily spell the names it forbids
    return file.equals("io/kahshe/format/BuildReport.java") && consumer.equals("alerts");
  }

  /** This module's main sources, found from its compiled classes rather than from a guess at cwd. */
  private static Path mainSources() throws URISyntaxException {
    // <module>/build/classes/java/main -> <module>/src/main/java
    return compiledClasses().getParent().getParent().getParent().getParent()
        .resolve("src").resolve("main").resolve("java");
  }

  /** The directory this module's classes were compiled to, found through a class in it. */
  private static Path compiledClasses() throws URISyntaxException {
    return Path.of(IndexPruner.class.getProtectionDomain().getCodeSource().getLocation().toURI());
  }
}
