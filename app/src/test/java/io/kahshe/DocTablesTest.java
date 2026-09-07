package io.kahshe;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertTrue;

import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.ArrayList;
import java.util.List;
import java.util.regex.Pattern;
import java.util.stream.Stream;

import org.junit.jupiter.api.Test;

/**
 * Every markdown table row in the repository's documents must carry exactly as many cells as its
 * delimiter row.
 *
 * <p>This exists because the failure is silent: GFM ignores cells past the header count, so a row
 * written with extra columns renders as a row whose last columns are simply gone — no warning, no
 * broken layout, just missing text.
 *
 * <p>Three causes, all worth catching: a row written with more columns than the table has; a
 * literal {@code |} in content — a regex alternation, a Sigma field modifier — which splits the
 * cell it sits in and must be written {@code \|}; and a row with no closing pipe, whose last
 * column runs off the row.
 */
class DocTablesTest {

  /** A cell boundary is an UNESCAPED pipe: {@code \|} is content. */
  private static final Pattern CELL = Pattern.compile("(?<!\\\\)\\|");

  private static final Pattern DELIMITER = Pattern.compile(":?-{2,}:?");

  @Test
  void noTableRowLosesCellsWhenRendered() throws IOException {
    List<Path> docs = markdown();
    assertTrue(docs.size() >= 4,
        "only " + docs.size() + " markdown files found from " + Path.of("").toAbsolutePath()
            + ": the check found nothing to read and would pass for the wrong reason");

    List<String> malformed = new ArrayList<>();
    for (Path doc : docs) {
      List<String> lines = Files.readAllLines(doc);
      int width = 0;
      boolean fenced = false;
      for (int n = 0; n < lines.size(); n++) {
        String line = lines.get(n).strip();
        // a shell pipeline in a fenced block and an ASCII diagram both start lines with |
        if (line.startsWith("```") || line.startsWith("~~~")) {
          fenced = !fenced;
          width = 0;
          continue;
        }
        if (fenced || !line.startsWith("|")) {
          width = 0;
          continue;
        }
        List<String> cells = line.endsWith("|") ? cellsOf(line) : List.of();
        if (!cells.isEmpty() && cells.stream().allMatch(c -> DELIMITER.matcher(c).matches())) {
          width = cells.size(); // the delimiter row, not the header, decides the width
          checkHeader(doc, lines, n, width, malformed);
          continue;
        }
        if (width == 0) {
          continue; // not inside a table: nothing has said how wide one would be
        }
        if (!line.endsWith("|")) {
          // width is deliberately KEPT: one bad row must not mask every row below it, or a
          // table is fixed one run at a time. Only a non-table line ends the table.
          malformed.add(doc + ":" + (n + 1) + " has no closing pipe, so its last column runs off "
              + "the row: " + line.substring(0, Math.min(56, line.length())));
          continue;
        }
        if (cells.size() == width) {
          continue;
        }
        malformed.add(doc + ":" + (n + 1) + " has " + cells.size() + " cells, the table has "
            + width + (cells.size() > width
                ? " — the last " + (cells.size() - width) + " will not be rendered at all; either "
                    + "the row was written with extra columns, or a literal | in the content needs "
                    + "escaping as \\|"
                : " — the trailing column(s) render empty")
            + ": " + line.substring(0, Math.min(56, line.length())));
      }
    }
    assertEquals(List.of(), malformed, "malformed table rows drop their own text: " + malformed);
  }

  /** The header sits one line above the delimiter and is as easy to write a cell short. */
  private static void checkHeader(Path doc, List<String> lines, int delimiter, int width,
      List<String> malformed) {
    if (delimiter == 0) {
      return;
    }
    String header = lines.get(delimiter - 1).strip();
    if (!header.startsWith("|") || !header.endsWith("|")) {
      return;
    }
    int cells = cellsOf(header).size();
    if (cells != width) {
      malformed.add(doc + ":" + delimiter + " is a header of " + cells + " cells over a table of "
          + width + ": " + header.substring(0, Math.min(56, header.length())));
    }
  }

  private static List<String> cellsOf(String line) {
    String[] parts = CELL.split(line, -1);
    List<String> cells = new ArrayList<>();
    for (int i = 1; i < parts.length - 1; i++) {
      cells.add(parts[i].strip());
    }
    return cells;
  }

  /** The documents a reader actually opens: the repository root, docs/ and the benchmark notes. */
  private static List<Path> markdown() throws IOException {
    Path root = Path.of("..");
    List<Path> docs = new ArrayList<>();
    List<Path> dirs = new ArrayList<>(List.of(root, root.resolve("docs"),
        root.resolve("benchmark"), root.resolve("dev/trino-patch"),
        root.resolve("helm/kahshe"), root.resolve("sigma")));
    // Every module's README. These carry the operator-facing knob tables, which is exactly the
    // shape that loses a column silently.
    for (String module : List.of("common", "analysis", "format", "indexer", "proxy", "watch",
        "app")) {
      dirs.add(root.resolve(module));
    }
    for (Path dir : dirs) {
      if (!Files.isDirectory(dir)) {
        continue;
      }
      try (Stream<Path> files = Files.list(dir)) {
        files.filter(f -> f.getFileName().toString().endsWith(".md")).forEach(docs::add);
      }
    }
    return docs;
  }
}
