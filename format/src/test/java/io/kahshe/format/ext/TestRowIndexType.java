package io.kahshe.format.ext;

import io.kahshe.format.type.IndexType;
import java.util.List;
import java.util.concurrent.atomic.AtomicInteger;
import org.apache.iceberg.FileScanTask;

/**
 * A fourth index type registered only for the format module's tests, through
 * {@code format/src/test/resources/META-INF/services/io.kahshe.format.type.IndexType}. It lives outside
 * {@code io.kahshe.format} so it can only use what the interface makes public.
 *
 * <p>It writes nothing and loads nothing, so every other test's bytes are unaffected; what it does
 * is count the seams it is offered. {@link #MODE} makes its {@code load} throw, which is how
 * {@code IndexTypesTest} holds the pruner to keep-on-doubt.
 */
public final class TestRowIndexType implements IndexType {

  /** This type's key. */
  public static final String KEY = "test-rows";

  /** System property: {@code "throw"} makes {@link #load} fail; anything else is absent. */
  public static final String MODE = "kahshe.test.indextype.mode";

  /** Files this type's collector was opened on. */
  public static final AtomicInteger FILES = new AtomicInteger();

  /** Rows it was handed. */
  public static final AtomicInteger ROWS = new AtomicInteger();

  /** Tokens it was handed with those rows. */
  public static final AtomicInteger TOKENS = new AtomicInteger();

  /** Files it was told were finished. */
  public static final AtomicInteger FILES_DONE = new AtomicInteger();

  /** Publishes it was asked to write in. */
  public static final AtomicInteger WRITES = new AtomicInteger();

  /** Zeroes the counters; call it immediately before the build under test. */
  public static void reset() {
    FILES.set(0);
    ROWS.set(0);
    TOKENS.set(0);
    FILES_DONE.set(0);
    WRITES.set(0);
  }

  @Override
  public String key() {
    return KEY;
  }

  @Override
  public Collector collector(BuildContext ctx) {
    return new Collector() {
      @Override
      public void file(String path, int ordinal) {
        FILES.incrementAndGet();
      }

      @Override
      public void row(int rowPosition, List<String> tokens) {
        ROWS.incrementAndGet();
        TOKENS.addAndGet(tokens.size());
      }

      @Override
      public void fileDone() {
        FILES_DONE.incrementAndGet();
      }
    };
  }

  @Override
  public Leaves write(PublishContext ctx) {
    WRITES.incrementAndGet();
    return Leaves.NONE;
  }

  @Override
  public Loaded load(ReadContext ctx) {
    if ("throw".equals(System.getProperty(MODE))) {
      throw new IllegalStateException("test index type refuses to load");
    }
    return null;
  }

  /** Never reached: an absent type is not asked. Reaching it would prune the whole plan. */
  @Override
  public List<FileScanTask> prune(Loaded loaded, Probe probe, List<FileScanTask> tasks) {
    return List.of();
  }
}
