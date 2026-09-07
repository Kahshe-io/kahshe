package io.kahshe;

import static org.junit.jupiter.api.Assertions.assertEquals;

import org.junit.jupiter.api.Test;

/**
 * The wiring decision {@link WatchRoles} encodes: a watch instance with the indexer off still
 * discovers and scans, because detection follows the rules rather than the index.
 */
class WatchRolesTest {

  @Test
  void watchModeWithTheIndexerOffScansAndReadsReports() {
    assertEquals(
        new WatchRoles(false, true, true), WatchRoles.of("watch", false, true),
        "detection does not depend on the indexer; the reports of builds elsewhere are delivered");
  }

  @Test
  void watchModeWithTheIndexerOnBuildsScansAndReadsNoReports() {
    assertEquals(new WatchRoles(true, true, false), WatchRoles.of("watch", true, true));
  }

  @Test
  void bothModeFollowsTheIndexerFlagTheSameWay() {
    assertEquals(new WatchRoles(true, true, false), WatchRoles.of("both", true, true));
    assertEquals(new WatchRoles(false, true, true), WatchRoles.of("both", false, true));
  }

  @Test
  void proxyModeHasNoWatchRoleBeyondRidingItsOwnBuilds() {
    assertEquals(new WatchRoles(true, false, false), WatchRoles.of("proxy", true, true));
    assertEquals(new WatchRoles(false, false, false), WatchRoles.of("proxy", false, true));
  }

  @Test
  void noRulesMeansNoRoleWhateverTheMode() {
    for (String mode : new String[] {"proxy", "watch", "both"}) {
      assertEquals(new WatchRoles(false, false, false), WatchRoles.of(mode, true, false), mode);
      assertEquals(new WatchRoles(false, false, false), WatchRoles.of(mode, false, false), mode);
    }
  }
}
