package io.kahshe;

/**
 * Which watch roles a process runs, from its mode, its indexer flag and whether rules are
 * configured. Detection is the rules' job, not the index's: discovery and the row scan run wherever
 * rules are set and the mode has a watch role, whether or not this process builds anything. The
 * index-riding listener rides this process's builds, so it needs the indexer; the report poller
 * delivers what builds ELSEWHERE raised, so it runs when the indexer is off.
 *
 * @param indexRiding the {@code WatchEngine} listener rides this process's index builds
 * @param discovery {@code TableDiscovery} polls the rule-named tables, with the row scan
 * @param reports {@code ReportPoller} delivers the alerts in build reports written elsewhere
 */
public record WatchRoles(boolean indexRiding, boolean discovery, boolean reports) {

  /** {@code mode} is one of {@code proxy}, {@code watch}, {@code both}, already normalised. */
  public static WatchRoles of(String mode, boolean indexerOn, boolean rulesSet) {
    if (!rulesSet) {
      return new WatchRoles(false, false, false);
    }
    boolean watchRole = !"proxy".equals(mode);
    return new WatchRoles(indexerOn, watchRole, watchRole && !indexerOn);
  }

  public boolean any() {
    return indexRiding || discovery || reports;
  }
}
