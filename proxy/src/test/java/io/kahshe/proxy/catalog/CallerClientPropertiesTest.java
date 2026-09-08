package io.kahshe.proxy.catalog;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNull;

import io.kahshe.proxy.TestConfigs;
import java.util.Map;
import org.apache.iceberg.rest.auth.OAuth2Properties;
import org.junit.jupiter.api.Test;

/**
 * What the caller client is built from: the caller's bearer and nothing that would let the proxy
 * outlive it. Iceberg's REST client refreshes a JWT by token exchange before its {@code exp} unless
 * told not to, which would leave kahshe holding a credential for that principal the caller never
 * presented. Verified red with the refresh switch absent.
 */
class CallerClientPropertiesTest {
  @Test
  void aReplayedTokenIsNeverRefreshedByTheProxy() {
    BackendCatalogs catalogs = new BackendCatalogs(TestConfigs.proxyConfig());
    Map<String, String> props = catalogs.callerProperties("lake", "tok");
    assertEquals("tok", props.get(OAuth2Properties.TOKEN));
    assertEquals("false", props.get(OAuth2Properties.TOKEN_REFRESH_ENABLED),
        "the token is the caller's to renew; the proxy must not exchange it for one of its own");
    assertNull(props.get(OAuth2Properties.CREDENTIAL), "no service credential rides with a caller");
  }
}
