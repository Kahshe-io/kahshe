package io.kahshe.watch.scan;

import java.util.ArrayList;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.CopyOnWriteArrayList;

/**
 * A second {@link Scanner}, in tests only: it declares one column, counts every row it is handed
 * and raises one alert per file.
 *
 * <p>Registered in watch/src/test/resources/META-INF/services and exists to prove the discovery
 * claim — a scanner from a source set the main services file knows nothing about is found by
 * {@link Scanners}, its column joins the projection the pass reads, its row hook sees every row,
 * and what it returns from {@link FileScan#finish} is delivered through the sink. It also proves
 * the read is SHARED: it names the same file the rule scanner does and neither reads it twice.
 */
public final class CountingScanner implements Scanner {
  static final String NAME = "test-counting";
  /** The column it asks for; a table without it must not see this scanner run at all. */
  static final String COLUMN = "other";

  /** What the last scan handed it, per file path. Static because ServiceLoader owns the instance. */
  static final List<String> SEEN = new CopyOnWriteArrayList<>();
  static final Map<String, Integer> ROWS = new java.util.concurrent.ConcurrentHashMap<>();
  /** Off by default so the pass's other tests are not perturbed by a second scanner's alerts. */
  static volatile boolean enabled;

  static void reset() {
    SEEN.clear();
    ROWS.clear();
  }

  @Override
  public String name() {
    return NAME;
  }

  @Override
  public Set<String> columns(TableView table) {
    if (!enabled || !table.has(COLUMN)) {
      return Set.of();
    }
    return Set.of(COLUMN);
  }

  @Override
  public FileScan open(FileScanContext ctx) {
    return new FileScan() {
      private final List<String> values = new ArrayList<>();

      @Override
      public void row(int rowPosition, Row row) {
        values.add(row.canonical(COLUMN));
      }

      @Override
      public List<Map<String, Object>> finish() {
        SEEN.addAll(values);
        ROWS.put(ctx.path(), values.size());
        // No "rule" key: the pass falls back to the scanner's own name for suppression.
        Map<String, Object> payload = new LinkedHashMap<>();
        payload.put("scanner", NAME);
        payload.put("rows", values.size());
        payload.put("file", Map.of("path", ctx.path()));
        return List.of(payload);
      }
    };
  }
}
