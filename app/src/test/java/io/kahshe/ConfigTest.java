package io.kahshe;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

import org.junit.jupiter.api.Test;

/**
 * The startup refusals: a bad value fails the process before either server binds, with the
 * variable named. Each is a static rule on {@link Kahshe.Config}, checked here without an
 * environment to fake.
 */
class ConfigTest {

  @Test
  void anAdminBindThatDoesNotResolveIsRefusedByName() {
    IllegalArgumentException e = assertThrows(IllegalArgumentException.class,
        () -> Kahshe.Config.adminBind("no.such.host.invalid"));
    assertTrue(e.getMessage().startsWith("KAHSHE_ADMIN_BIND="), e.getMessage());
    assertEquals("127.0.0.1", Kahshe.Config.adminBind("127.0.0.1"));
    assertEquals("0.0.0.0", Kahshe.Config.adminBind("0.0.0.0"));
  }

  @Test
  void aLogFormatLogbackWouldNotKnowIsRefusedRatherThanRunSilent() {
    Kahshe.Config.checkLogFormat("text");
    Kahshe.Config.checkLogFormat("json");
    for (String bad : new String[] {"JSON", "yaml", ""}) {
      IllegalArgumentException e = assertThrows(IllegalArgumentException.class,
          () -> Kahshe.Config.checkLogFormat(bad), bad);
      assertTrue(e.getMessage().startsWith("KAHSHE_LOG_FORMAT"), e.getMessage());
    }
  }

  /**
   * An operator sets this only to NARROW the instruction. Falling back to the permissive default
   * on a typo would do the opposite of what they asked, and nothing would say so.
   */
  @Test
  void anAdvertiseModeThatIsNotOneOfTheThreeIsRefused() {
    assertEquals("all", Kahshe.Config.checkAdvertiseServerMode("all"));
    assertEquals("indexed", Kahshe.Config.checkAdvertiseServerMode("indexed"));
    assertEquals("none", Kahshe.Config.checkAdvertiseServerMode("none"));
    for (String bad : new String[] {"Indexed", "INDEXED", "indexd", "true", ""}) {
      IllegalArgumentException e = assertThrows(IllegalArgumentException.class,
          () -> Kahshe.Config.checkAdvertiseServerMode(bad), bad);
      assertTrue(e.getMessage().startsWith("KAHSHE_ADVERTISE_SERVER_MODE"), e.getMessage());
    }
  }

  /**
   * The reader matches {@code caller} exactly, so any other spelling would plan as the service —
   * one plan shared by every caller — and nothing would say so. A typo fails startup instead.
   */
  @Test
  void aPlanningIdentityThatIsNotOneOfTheTwoIsRefused() {
    assertEquals("caller", Kahshe.Config.checkPlanningIdentity("caller"));
    assertEquals("service", Kahshe.Config.checkPlanningIdentity("service"));
    for (String bad : new String[] {"Caller", "CALLER", "callr", "true", ""}) {
      IllegalArgumentException e = assertThrows(IllegalArgumentException.class,
          () -> Kahshe.Config.checkPlanningIdentity(bad), bad);
      assertTrue(e.getMessage().startsWith("KAHSHE_PLANNING_IDENTITY"), e.getMessage());
    }
  }

  @Test
  void aTokenCapOfZeroIsNotUnlimited() {
    assertThrows(IllegalArgumentException.class, () -> Kahshe.Config.tokenLength(0));
    assertThrows(IllegalArgumentException.class, () -> Kahshe.Config.tokenLength(1L << 40));
    assertEquals(256, Kahshe.Config.tokenLength(256));
  }

  /**
   * The production default is one string literal in {@code fromEnv}, read straight from the
   * environment, and the fixture test cannot see it. Read through the real path: with the variable
   * unset this must be the record's default, and with it set, whatever it says. Verified red with
   * the fallback hardcoded back to {@code service}.
   */
  @Test
  void theProductionDefaultIsCallerUnlessTheEnvironmentSaysOtherwise() {
    String expected = System.getenv().getOrDefault(
        "KAHSHE_PLANNING_IDENTITY", io.kahshe.proxy.ProxyConfig.DEFAULT_PLANNING_IDENTITY);
    assertEquals(expected, Kahshe.Config.fromEnv().proxy().planningIdentity());
  }
}
