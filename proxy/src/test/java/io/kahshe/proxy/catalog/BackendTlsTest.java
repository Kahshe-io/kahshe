package io.kahshe.proxy.catalog;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

import com.sun.net.httpserver.HttpsConfigurator;
import com.sun.net.httpserver.HttpsServer;
import java.io.IOException;
import java.net.InetSocketAddress;
import java.net.URI;
import java.net.http.HttpClient;
import java.net.http.HttpRequest;
import java.net.http.HttpResponse;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Path;
import java.security.KeyStore;
import java.util.Map;
import javax.net.ssl.KeyManagerFactory;
import javax.net.ssl.SSLContext;
import javax.net.ssl.SSLHandshakeException;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

/**
 * Trusting a backing catalog behind a private certificate authority.
 *
 * <p>The handshakes here are real, against a server holding a self-signed certificate. Only
 * {@link #aRestCatalogReachesASelfSignedBackendThroughTheProperty} exercises the property name
 * itself; the reason is on that test.
 */
class BackendTlsTest {

  @TempDir Path dir;

  /** Without the CA, the JVM's default trust store rejects a self-signed server — as it should. */
  @Test
  void aPrivateAuthorityIsRejectedByDefault() throws Exception {
    Server server = Server.selfSigned(dir);
    try (server) {
      HttpClient plain = HttpClient.newHttpClient();
      assertThrows(SSLHandshakeException.class, () -> plain.send(
          HttpRequest.newBuilder(server.uri()).build(), HttpResponse.BodyHandlers.ofString()),
          "a self-signed backend must not be trusted just because it is the backend");
    }
  }

  /** With the CA, the same connection succeeds. */
  @Test
  void theConfiguredCaMakesTheHandshakeSucceed() throws Exception {
    Server server = Server.selfSigned(dir);
    try (server) {
      HttpClient trusting = HttpClient.newBuilder()
          .sslContext(BackendTls.contextTrusting(server.caPem))
          .build();
      HttpResponse<String> response = trusting.send(
          HttpRequest.newBuilder(server.uri()).build(), HttpResponse.BodyHandlers.ofString());
      assertEquals("ok", response.body());
    }
  }

  /**
   * The Iceberg half: the configurer built reflectively, initialized from a property map, hands
   * back a context that completes the same handshake. This is the path a RESTCatalog takes.
   */
  @Test
  void theConfigurerIcebergConstructsProducesAWorkingContext() throws Exception {
    Server server = Server.selfSigned(dir);
    try (server) {
      BackendTls configurer = BackendTls.class.getDeclaredConstructor().newInstance();
      configurer.initialize(Map.of(BackendTls.CA_PROPERTY, server.caPem.toString()));
      SSLContext context = configurer.sslContext();
      assertNotNull(context);

      HttpResponse<String> response = HttpClient.newBuilder().sslContext(context).build()
          .send(HttpRequest.newBuilder(server.uri()).build(), HttpResponse.BodyHandlers.ofString());
      assertEquals("ok", response.body());
    }
  }

  /** Hostname verification stays Iceberg's: returning a verifier here is how it gets disabled. */
  @Test
  void hostnameVerificationIsLeftToIceberg() {
    assertTrue(new BackendTls().hostnameVerifier() == null,
        "a non-null verifier is the usual way a private-CA setup becomes an any-certificate one");
  }

  /** No CA configured means the JVM default, not a permissive context. */
  @Test
  void withoutACaItFallsBackToTheDefaultRatherThanTrustingEverything() throws Exception {
    BackendTls configurer = new BackendTls();
    configurer.initialize(Map.of());
    assertEquals(SSLContext.getDefault(), configurer.sslContext());
  }

  /** A CA path that is not a certificate fails at initialize, not at the first request. */
  @Test
  void anUnreadableCaFailsLoudlyAtInitialize() throws Exception {
    Path junk = dir.resolve("not-a-ca.pem");
    Files.writeString(junk, "just some text\n");
    IllegalStateException e = assertThrows(IllegalStateException.class,
        () -> new BackendTls().initialize(Map.of(BackendTls.CA_PROPERTY, junk.toString())));
    assertTrue(e.getMessage().contains("not-a-ca.pem"), e.getMessage());
  }

  /**
   * The test that matters most: a real {@code RESTCatalog} reaching a self-signed endpoint
   * through the property.
   *
   * <p>{@code rest.client.tls.configurer-impl} is undocumented upstream and its constant is
   * package-private, so kahshe carries the string as a literal. If Iceberg renames it, every
   * other test here still passes and the feature is silently dead — this is the one that notices.
   * It also pins that the configurer covers the bootstrap {@code /v1/config} call, which happens
   * before any merged configuration exists.
   */
  @Test
  void aRestCatalogReachesASelfSignedBackendThroughTheProperty() throws Exception {
    try (Server server = Server.selfSigned(dir)) {
      java.util.Map<String, String> props = new java.util.HashMap<>();
      props.put("uri", server.uri().toString().replaceAll("/$", ""));
      props.put(BackendTls.CONFIGURER_PROPERTY, BackendTls.class.getName());
      props.put(BackendTls.CA_PROPERTY, server.caPem.toString());

      try (org.apache.iceberg.rest.RESTCatalog catalog =
          new org.apache.iceberg.rest.RESTCatalog()) {
        catalog.setConf(new org.apache.hadoop.conf.Configuration());
        catalog.initialize("tls-test", props);
      }
    }
  }

  /** And without it, the same catalog cannot reach the same endpoint. */
  @Test
  void withoutThePropertyTheSameCatalogCannotReachIt() throws Exception {
    try (Server server = Server.selfSigned(dir)) {
      java.util.Map<String, String> props = new java.util.HashMap<>();
      props.put("uri", server.uri().toString().replaceAll("/$", ""));

      Exception e = assertThrows(Exception.class, () -> {
        try (org.apache.iceberg.rest.RESTCatalog catalog =
            new org.apache.iceberg.rest.RESTCatalog()) {
          catalog.setConf(new org.apache.hadoop.conf.Configuration());
          catalog.initialize("tls-test-untrusted", props);
        }
      });
      String chain = "";
      for (Throwable c = e; c != null; c = c.getCause()) {
        chain += c.getClass().getSimpleName() + ": " + c.getMessage() + " | ";
      }
      assertTrue(chain.toLowerCase().contains("ssl") || chain.toLowerCase().contains("certif")
              || chain.toLowerCase().contains("trust"),
          "the failure should be a trust failure, not something else: " + chain);
    }
  }

  private static final class Server implements AutoCloseable {
    private final HttpsServer server;
    final Path caPem;

    private Server(HttpsServer server, Path caPem) {
      this.server = server;
      this.caPem = caPem;
    }

    static Server selfSigned(Path dir) throws Exception {
      Path store = dir.resolve("backend.p12");
      run("keytool", "-genkeypair", "-alias", "backend", "-keyalg", "RSA", "-keysize", "2048",
          "-validity", "1", "-dname", "CN=localhost", "-ext", "SAN=dns:localhost,ip:127.0.0.1",
          "-keystore", store.toString(), "-storetype", "PKCS12",
          "-storepass", "changeit", "-keypass", "changeit");
      Path caPem = dir.resolve("ca.crt");
      run("keytool", "-exportcert", "-rfc", "-alias", "backend", "-keystore", store.toString(),
          "-storepass", "changeit", "-file", caPem.toString());

      KeyStore ks = KeyStore.getInstance("PKCS12");
      try (var in = Files.newInputStream(store)) {
        ks.load(in, "changeit".toCharArray());
      }
      KeyManagerFactory keys =
          KeyManagerFactory.getInstance(KeyManagerFactory.getDefaultAlgorithm());
      keys.init(ks, "changeit".toCharArray());
      SSLContext context = SSLContext.getInstance("TLS");
      context.init(keys.getKeyManagers(), null, null);

      HttpsServer server = HttpsServer.create(new InetSocketAddress("127.0.0.1", 0), 0);
      server.setHttpsConfigurator(new HttpsConfigurator(context));
      server.createContext("/v1/config", exchange -> {
        byte[] body = "{\"defaults\":{},\"overrides\":{}}".getBytes(StandardCharsets.UTF_8);
        exchange.getResponseHeaders().add("Content-Type", "application/json");
        exchange.sendResponseHeaders(200, body.length);
        exchange.getResponseBody().write(body);
        exchange.close();
      });
      server.createContext("/", exchange -> {
        byte[] body = "ok".getBytes(StandardCharsets.UTF_8);
        exchange.sendResponseHeaders(200, body.length);
        exchange.getResponseBody().write(body);
        exchange.close();
      });
      server.start();
      return new Server(server, caPem);
    }

    URI uri() {
      // 127.0.0.1, not localhost: the server binds IPv4 and localhost may resolve to ::1
      // first, which would make a trust test fail with "connection refused" and prove nothing.
      // The certificate's SAN covers this address.
      return URI.create("https://127.0.0.1:" + server.getAddress().getPort() + "/");
    }

    @Override
    public void close() {
      server.stop(0);
    }

    private static void run(String... command) throws Exception {
      Process p = new ProcessBuilder(command).redirectErrorStream(true).start();
      String out = new String(p.getInputStream().readAllBytes(), StandardCharsets.UTF_8);
      if (p.waitFor() != 0) {
        throw new IOException(command[0] + " failed: " + out);
      }
    }
  }
}
