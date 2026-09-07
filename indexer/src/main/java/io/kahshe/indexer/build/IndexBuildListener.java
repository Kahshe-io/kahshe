package io.kahshe.indexer.build;

import io.kahshe.format.type.gram.Grams;
import io.kahshe.format.type.term.TermIndexWriter;
import java.util.Set;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Observes index builds file by file, at the moment each file's term counts and exact gram set
 * are collected — before leaves are written. Evaluation is prospective only: listeners see files
 * as they are indexed, never retroactively.
 *
 * <p>References passed to {@link BuildContext#file} are borrowed: valid only for the duration of
 * the call. Wrap implementations with {@link #safe} so a listener failure never fails a build.
 */
public interface IndexBuildListener {

  /**
   * Whether the build reads only the files no prior index covered, or every in-scope file. Read
   * together with {@code priorCovered}, it is how a listener tells a file it is seeing for the
   * first time from one a rebuild is re-presenting.
   */
  enum BuildKind {
    /** Only files no prior index covered are read; the prior generation's coverage carries over. */
    INCREMENTAL,
    /** Every in-scope file is read, including files a prior index already covered. */
    FULL
  }

  interface BuildContext {
    /**
     * One data file, at the moment its evidence is in hand: {@code path} is the file's location,
     * {@code terms} its per-file term counts, {@code grams} the exact set of grams it holds.
     *
     * <p>{@code terms} always carries the row and token totals, but the per-token map behind
     * {@link TermIndexWriter.FileTerms#countOf} is populated only for a listener that answered
     * {@link IndexBuildListener#readsTermCounts()} true. Answer false and probe by token anyway
     * and every token reads 0 — no error, no counter.
     */
    void file(String path, TermIndexWriter.FileTerms terms, Set<String> grams);

    /**
     * No more files: called once, after the last {@link #file} and before the build report is
     * written, with {@link #alerts()} read immediately after — so anything held back across files
     * must be flushed here. A build that fails before publishing never reaches it.
     */
    void done();

    /**
     * The alerts this build raised, in the watch webhook's payload shape, for the build report
     * (docs/FORMAT.md section 12): a build's per-file evidence exists only in the read pass, so
     * evaluation rides the build and the report carries the verdicts out of the process.
     */
    default java.util.List<java.util.Map<String, Object>> alerts() {
      return java.util.List.of();
    }
  }

  /**
   * Whether this listener actually reads the per-file TERM COUNTS it is handed.
   *
   * <p>Tokenizing a data file is the most expensive thing the build does, and the counts have only
   * two consumers: the term dictionary, and a watch rule that matches on tokens. A listener that
   * reads only the grams, or reads nothing, must say so, or the build tokenizes every row and
   * discards the result. Defaulting to true is the safe direction: a listener that forgets to
   * answer gets the counts it may need, and pays for them.
   */
  default boolean readsTermCounts() {
    return true;
  }

  /**
   * Called immediately before a build's first publish, on every path that publishes (a build that
   * read files, and a restamp). A seam for the lease's last check: an implementation that takes the
   * column's lease away here makes the build abort with nothing written.
   */
  default void beforePublish() {}

  /**
   * Called once per (table, column) build, after the incremental-vs-full decision is final.
   * {@code priorCovered} is the file set the previous index covered, captured before any
   * full-rebuild fallback clears it.
   */
  BuildContext start(
      String prefix,
      String namespace,
      String tableName,
      String column,
      long snapshotId,
      BuildKind buildKind,
      Set<String> priorCovered,
      Grams.Contract grams);

  /**
   * The context {@link #NONE} hands back, and the one {@link #safe} substitutes when a listener
   * throws out of {@code start}: every hook does nothing and the build reports no alerts.
   */
  BuildContext NOOP_CONTEXT =
      new BuildContext() {
        @Override
        public void file(String path, TermIndexWriter.FileTerms terms, Set<String> grams) {}

        @Override
        public void done() {}
      };

  /**
   * The listener of a build nothing is observing. Its IDENTITY is load-bearing: the build tests
   * {@code listener != NONE} to decide whether to count terms at all, so wrapping or copying it
   * into something that behaves identically still turns tokenizing back on for every build.
   */
  IndexBuildListener NONE =
      (prefix, namespace, tableName, column, snapshotId, buildKind, priorCovered, grams) -> NOOP_CONTEXT;

  /**
   * Wraps a listener so any exception logs WARN and never fails the build — at every hook, not
   * only {@code start}: {@code readsTermCounts} answers true on failure (the counts it may need
   * are read), {@code beforePublish} publishes anyway. Null and {@link #NONE} both wrap to NONE,
   * whose identity the build still tests to skip term counting.
   */
  static IndexBuildListener safe(IndexBuildListener delegate) {
    if (delegate == null || delegate == NONE) {
      return NONE;
    }
    Logger log = LoggerFactory.getLogger(IndexBuildListener.class);
    return new IndexBuildListener() {
      @Override
      public boolean readsTermCounts() {
        try {
          return delegate.readsTermCounts();
        } catch (RuntimeException e) {
          log.warn("build listener failed at readsTermCounts; counting terms as if it reads them", e);
          return true;
        }
      }

      @Override
      public void beforePublish() {
        try {
          delegate.beforePublish();
        } catch (RuntimeException e) {
          log.warn("build listener failed at beforePublish; publishing anyway", e);
        }
      }

      @Override
      public BuildContext start(
          String prefix, String namespace, String tableName, String column, long snapshotId,
          BuildKind buildKind, Set<String> priorCovered, Grams.Contract grams) {
      BuildContext inner;
      try {
        inner = delegate.start(prefix, namespace, tableName, column, snapshotId, buildKind, priorCovered, grams);
      } catch (RuntimeException e) {
        log.warn("build listener failed at start; disabled for this build", e);
        return NOOP_CONTEXT;
      }
      return new BuildContext() {
        private boolean warned;

        @Override
        public void file(String path, TermIndexWriter.FileTerms terms, Set<String> grams) {
          try {
            inner.file(path, terms, grams);
          } catch (RuntimeException e) {
            warnOnce(e);
          }
        }

        @Override
        public void done() {
          try {
            inner.done();
          } catch (RuntimeException e) {
            warnOnce(e);
          }
        }

        @Override
        public java.util.List<java.util.Map<String, Object>> alerts() {
          try {
            return inner.alerts();
          } catch (RuntimeException e) {
            warnOnce(e);
            return java.util.List.of();
          }
        }

        private void warnOnce(RuntimeException e) {
          if (!warned) {
            warned = true;
            log.warn("build listener failed; further failures this build are suppressed", e);
          }
        }
      };
      }
    };
  }
}
