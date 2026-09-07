package io.kahshe.indexer;

import io.kahshe.format.type.gram.Grams;
import io.kahshe.format.type.term.TermIndexWriter;
import java.util.ArrayList;
import java.util.List;
import java.util.Set;
import io.kahshe.indexer.build.IndexBuildListener;

/** A build listener that records what it was told, for tests of the build hook in any module. */
public final class RecordingListener implements IndexBuildListener {
  public int starts;
  public int dones;
  public BuildKind kind;
  public Set<String> priorCovered;
  public final List<String> files = new ArrayList<>();

  @Override
  public BuildContext start(String prefix, String namespace, String tableName, String column,
      long snapshotId, BuildKind buildKind, Set<String> prior, Grams.Contract grams) {
    starts++;
    kind = buildKind;
    priorCovered = Set.copyOf(prior);
    return new BuildContext() {
      @Override
      public void file(String path, TermIndexWriter.FileTerms terms, Set<String> grams) {
        files.add(path);
      }

      @Override
      public void done() {
        dones++;
      }
    };
  }
}
