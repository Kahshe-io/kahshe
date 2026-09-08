package io.kahshe.proxy;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertTrue;

import org.junit.jupiter.api.Test;

/**
 * The fixture every proxy test runs under carries the production default, and the default is
 * caller. This pins the decision rather than the mechanism: a revert of either — the fixture
 * back to service, or the record's default — is then an edit to a test that says why, not a
 * one-word drift that quietly moves the whole suite off the client production chooses.
 */
class TestConfigsTest {
  @Test
  void theFixtureCarriesTheProductionDefault() {
    ProxyConfig fixture = TestConfigs.proxyConfig();
    assertEquals(ProxyConfig.DEFAULT_PLANNING_IDENTITY, fixture.planningIdentity());
    assertTrue(fixture.callerIdentityPlanning(),
        "the production default is caller; a service fixture would keep the suite on the client "
            + "the served endpoints no longer choose unasked");
  }
}
