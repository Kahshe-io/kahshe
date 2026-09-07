package io.kahshe;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertNotSame;
import static org.junit.jupiter.api.Assertions.assertSame;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

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
import java.util.Map;
import javax.net.ssl.SSLContext;
import javax.net.ssl.TrustManager;
import javax.net.ssl.X509TrustManager;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

/**
 * TLS on the ports kahshe serves.
 *
 * <p>The handshake tests stand a real {@link HttpsServer} up and connect a real client to it,
 * because everything that goes wrong with TLS goes wrong at the handshake: a key that does not
 * match its certificate, a chain in the wrong order, a keystore whose password is right for the
 * store and wrong for the entry. A test that only checked the configuration parsed would have
 * passed for all of those.
 */
class ServerTlsTest {

  @TempDir Path dir;

  // ------------------------------------------------------------------ configuration refusals

  @Test
  void absentConfigurationIsPlaintextRatherThanAnError() {
    ServerTls.Settings s = ServerTls.fromEnv(Map.of());
    assertFalse(s.enabled(), "no certificate configured must stay the quickstart's plaintext");
  }

  @Test
  void twoWaysOfSupplyingACertificateIsRefusedRatherThanRanked() {
    IllegalArgumentException e = assertThrows(IllegalArgumentException.class,
        () -> ServerTls.fromEnv(Map.of(
            "KAHSHE_TLS_CERT", "/c.pem", "KAHSHE_TLS_KEY", "/k.pem",
            "KAHSHE_TLS_KEYSTORE", "/k.p12", "KAHSHE_TLS_KEYSTORE_PASSWORD", "x")));
    assertTrue(e.getMessage().contains("use one"), e.getMessage());
  }

  @Test
  void halfOfThePemPairIsRefused() {
    assertTrue(assertThrows(IllegalArgumentException.class,
        () -> ServerTls.fromEnv(Map.of("KAHSHE_TLS_CERT", "/c.pem")))
        .getMessage().contains("together"));
    assertTrue(assertThrows(IllegalArgumentException.class,
        () -> ServerTls.fromEnv(Map.of("KAHSHE_TLS_KEY", "/k.pem")))
        .getMessage().contains("together"));
  }

  /** Demanding a client certificate with nothing to verify it against fails every connection. */
  @Test
  void clientAuthWithoutAClientCaIsRefusedAtStartup() {
    IllegalArgumentException e = assertThrows(IllegalArgumentException.class,
        () -> ServerTls.fromEnv(Map.of(
            "KAHSHE_TLS_CERT", "/c.pem", "KAHSHE_TLS_KEY", "/k.pem",
            "KAHSHE_TLS_CLIENT_AUTH", "need")));
    assertTrue(e.getMessage().contains("KAHSHE_TLS_CLIENT_CA"), e.getMessage());
  }

  @Test
  void anUnknownClientAuthModeNamesTheThreeThatExist() {
    assertTrue(assertThrows(IllegalArgumentException.class,
        () -> ServerTls.fromEnv(Map.of("KAHSHE_TLS_CLIENT_AUTH", "maybe")))
        .getMessage().contains("none, want or need"));
  }

  // ------------------------------------------------------------------ real handshakes

  /** A PEM pair — what cert-manager writes into a Secret — serves a real TLS connection. */
  @Test
  void aPemPairServesARealHandshake() throws Exception {
    Certs certs = Certs.selfSigned(dir, "localhost");
    ServerTls tls = new ServerTls(ServerTls.fromEnv(Map.of(
        "KAHSHE_TLS_CERT", certs.certPem.toString(),
        "KAHSHE_TLS_KEY", certs.keyPem.toString())));

    try (Server server = Server.start(tls)) {
      assertEquals("ok", server.get(certs));
    }
  }

  /** And so does a keystore, which is the shape a Java shop already has. */
  @Test
  void aKeystoreServesARealHandshake() throws Exception {
    Certs certs = Certs.selfSigned(dir, "localhost");
    Path store = certs.toPkcs12(dir.resolve("kahshe.p12"), "secret");
    ServerTls tls = new ServerTls(ServerTls.fromEnv(Map.of(
        "KAHSHE_TLS_KEYSTORE", store.toString(),
        "KAHSHE_TLS_KEYSTORE_PASSWORD", "secret")));

    try (Server server = Server.start(tls)) {
      assertEquals("ok", server.get(certs));
    }
  }

  /**
   * A PKCS#1 key is the one an operator is most likely to have lying around, and the JDK cannot
   * read it. The message must name the conversion rather than a parse failure — this is the
   * error most likely to be met at 2 a.m. during a migration.
   */
  @Test
  void aPkcs1KeyIsRefusedWithTheConversionCommand() throws Exception {
    Certs certs = Certs.selfSigned(dir, "localhost");
    Path pkcs1 = dir.resolve("rsa.pem");
    Files.writeString(pkcs1, "-----BEGIN RSA PRIVATE KEY-----\nAAAA\n-----END RSA PRIVATE KEY-----\n");
    IOException e = assertThrows(IOException.class,
        () -> new ServerTls(ServerTls.fromEnv(Map.of(
            "KAHSHE_TLS_CERT", certs.certPem.toString(),
            "KAHSHE_TLS_KEY", pkcs1.toString()))));
    assertTrue(e.getMessage().contains("openssl pkcs8"), e.getMessage());
  }

  // ------------------------------------------------------------------ rotation

  /**
   * cert-manager renews on its own schedule, so a certificate loaded once is a 90-day fuse. A
   * changed file is picked up without a restart; an unchanged one is not re-read.
   */
  @Test
  void arotatedCertificateIsPickedUpWithoutARestart() throws Exception {
    Certs first = Certs.selfSigned(dir, "localhost");
    ServerTls tls = new ServerTls(ServerTls.fromEnv(Map.of(
        "KAHSHE_TLS_CERT", first.certPem.toString(),
        "KAHSHE_TLS_KEY", first.keyPem.toString(),
        "KAHSHE_TLS_RELOAD_MS", "0")));
    SSLContext before = tls.maybeReload();
    assertSame(before, tls.maybeReload(), "an unchanged file must not rebuild the context");

    Certs renewed = Certs.selfSigned(dir.resolve("renewed"), "localhost");
    Files.write(first.certPem, Files.readAllBytes(renewed.certPem));
    Files.write(first.keyPem, Files.readAllBytes(renewed.keyPem));
    Files.setLastModifiedTime(first.certPem,
        java.nio.file.attribute.FileTime.fromMillis(System.currentTimeMillis() + 10_000));

    assertNotSame(before, tls.maybeReload(), "a rotated certificate must be picked up");
  }

  /**
   * A rotation that writes a broken file must not take TLS down with it. Serving the previous
   * certificate is recoverable; falling back to plaintext, or refusing every connection, is the
   * kind of outage a renewal should never be able to cause.
   */
  @Test
  void aBrokenRotationKeepsThePreviousCertificateServing() throws Exception {
    Certs certs = Certs.selfSigned(dir, "localhost");
    ServerTls tls = new ServerTls(ServerTls.fromEnv(Map.of(
        "KAHSHE_TLS_CERT", certs.certPem.toString(),
        "KAHSHE_TLS_KEY", certs.keyPem.toString(),
        "KAHSHE_TLS_RELOAD_MS", "0")));
    SSLContext before = tls.maybeReload();

    Files.writeString(certs.certPem, "not a certificate at all\n");
    Files.setLastModifiedTime(certs.certPem,
        java.nio.file.attribute.FileTime.fromMillis(System.currentTimeMillis() + 10_000));

    assertSame(before, tls.maybeReload(), "a broken rotation must leave the old context serving");
  }


  // ------------------------------------------------------------------ mutual TLS

  /**
   * The three cases that decide whether "mutual TLS" is a real claim.
   *
   * <p>Accepting the right certificate proves only that the plumbing runs. What separates mTLS
   * from a server that accepts any certificate at all is the third case — a certificate that is
   * perfectly valid, signed by a real CA, and NOT the CA this server was told to trust. A
   * misconfiguration there does not fail loudly; it quietly authenticates strangers.
   */
  @Test
  void clientAuthNeedAcceptsTheRightCertificate() throws Exception {
    Ca ca = Ca.create(dir.resolve("ca"), "kahshe-clients");
    Certs server = Certs.selfSigned(dir.resolve("server"), "localhost");
    Path clientStore = ca.issueClient(dir.resolve("client"), "engine-1");

    ServerTls tls = new ServerTls(ServerTls.fromEnv(Map.of(
        "KAHSHE_TLS_CERT", server.certPem.toString(),
        "KAHSHE_TLS_KEY", server.keyPem.toString(),
        "KAHSHE_TLS_CLIENT_AUTH", "need",
        "KAHSHE_TLS_CLIENT_CA", ca.caPem.toString())));

    try (Server s = Server.start(tls)) {
      assertEquals("ok", s.get(server, clientStore));
    }
  }

  @Test
  void clientAuthNeedRejectsAClientWithNoCertificate() throws Exception {
    Ca ca = Ca.create(dir.resolve("ca"), "kahshe-clients");
    Certs server = Certs.selfSigned(dir.resolve("server"), "localhost");

    ServerTls tls = new ServerTls(ServerTls.fromEnv(Map.of(
        "KAHSHE_TLS_CERT", server.certPem.toString(),
        "KAHSHE_TLS_KEY", server.keyPem.toString(),
        "KAHSHE_TLS_CLIENT_AUTH", "need",
        "KAHSHE_TLS_CLIENT_CA", ca.caPem.toString())));

    try (Server s = Server.start(tls)) {
      assertThrows(IOException.class, () -> s.get(server, null),
          "need must reject a client that presents nothing");
    }
  }

  /** The one that matters: a valid certificate from an authority this server was not told about. */
  @Test
  void clientAuthNeedRejectsACertificateFromAnotherAuthority() throws Exception {
    Ca ours = Ca.create(dir.resolve("ca"), "kahshe-clients");
    Ca stranger = Ca.create(dir.resolve("other-ca"), "somebody-else");
    Certs server = Certs.selfSigned(dir.resolve("server"), "localhost");
    Path strangerCert = stranger.issueClient(dir.resolve("stranger"), "engine-x");

    ServerTls tls = new ServerTls(ServerTls.fromEnv(Map.of(
        "KAHSHE_TLS_CERT", server.certPem.toString(),
        "KAHSHE_TLS_KEY", server.keyPem.toString(),
        "KAHSHE_TLS_CLIENT_AUTH", "need",
        "KAHSHE_TLS_CLIENT_CA", ours.caPem.toString())));

    try (Server s = Server.start(tls)) {
      assertThrows(IOException.class, () -> s.get(server, strangerCert),
          "a certificate signed by another CA must not authenticate");
    }
  }

  /** `want` is the migration setting: a client with no certificate is still served. */
  @Test
  void clientAuthWantStillServesAClientWithNoCertificate() throws Exception {
    Ca ca = Ca.create(dir.resolve("ca"), "kahshe-clients");
    Certs server = Certs.selfSigned(dir.resolve("server"), "localhost");

    ServerTls tls = new ServerTls(ServerTls.fromEnv(Map.of(
        "KAHSHE_TLS_CERT", server.certPem.toString(),
        "KAHSHE_TLS_KEY", server.keyPem.toString(),
        "KAHSHE_TLS_CLIENT_AUTH", "want",
        "KAHSHE_TLS_CLIENT_CA", ca.caPem.toString())));

    try (Server s = Server.start(tls)) {
      assertEquals("ok", s.get(server, null), "want must not turn away an unauthenticated client");
    }
  }

  /** A CA that can issue client certificates, built with keytool. */
  private record Ca(Path caPem, Path caStore, String password) {

    static Ca create(Path dir, String cn) throws Exception {
      Files.createDirectories(dir);
      Path store = dir.resolve("ca.p12");
      Certs.run("keytool", "-genkeypair", "-alias", "ca", "-keyalg", "RSA", "-keysize", "2048",
          "-validity", "2", "-dname", "CN=" + cn, "-ext", "bc:c",
          "-keystore", store.toString(), "-storetype", "PKCS12",
          "-storepass", "changeit", "-keypass", "changeit");
      Path pem = dir.resolve("ca.crt");
      Certs.run("keytool", "-exportcert", "-rfc", "-alias", "ca", "-keystore", store.toString(),
          "-storepass", "changeit", "-file", pem.toString());
      return new Ca(pem, store, "changeit");
    }

    /** A client keystore holding a key and a certificate this CA signed. */
    Path issueClient(Path dir, String cn) throws Exception {
      Files.createDirectories(dir);
      Path store = dir.resolve("client.p12");
      Certs.run("keytool", "-genkeypair", "-alias", "client", "-keyalg", "RSA", "-keysize", "2048",
          "-validity", "1", "-dname", "CN=" + cn,
          "-keystore", store.toString(), "-storetype", "PKCS12",
          "-storepass", "changeit", "-keypass", "changeit");
      Path csr = dir.resolve("client.csr");
      Certs.run("keytool", "-certreq", "-alias", "client", "-keystore", store.toString(),
          "-storepass", "changeit", "-file", csr.toString());
      Path signed = dir.resolve("client.crt");
      Certs.run("keytool", "-gencert", "-alias", "ca", "-keystore", caStore.toString(),
          "-storepass", password, "-infile", csr.toString(), "-outfile", signed.toString(),
          "-rfc", "-validity", "1");
      // The chain must be imported CA-first so the client can present a complete one.
      Certs.run("keytool", "-importcert", "-noprompt", "-alias", "ca", "-file", caPem.toString(),
          "-keystore", store.toString(), "-storepass", "changeit");
      Certs.run("keytool", "-importcert", "-noprompt", "-alias", "client", "-file",
          signed.toString(), "-keystore", store.toString(), "-storepass", "changeit");
      return store;
    }
  }

  // ------------------------------------------------------------------ helpers

  /** A running HttpsServer that answers "ok", closed with the test. */
  private static final class Server implements AutoCloseable {
    private final HttpsServer server;

    private Server(HttpsServer server) {
      this.server = server;
    }

    static Server start(ServerTls tls) throws IOException {
      HttpsServer server = HttpsServer.create(new InetSocketAddress("127.0.0.1", 0), 0);
      server.setHttpsConfigurator(tls.configurator());
      server.createContext("/", exchange -> {
        byte[] body = "ok".getBytes(StandardCharsets.UTF_8);
        exchange.sendResponseHeaders(200, body.length);
        exchange.getResponseBody().write(body);
        exchange.close();
      });
      server.start();
      return new Server(server);
    }

    String get(Certs certs) throws Exception {
      return get(certs, null);
    }

    /** @param clientStore a PKCS12 to present as a client certificate, or null to present none */
    String get(Certs certs, Path clientStore) throws Exception {
      HttpClient client = HttpClient.newBuilder()
          .sslContext(certs.trustingContext(clientStore)).build();
      HttpResponse<String> response = client.send(
          HttpRequest.newBuilder(URI.create(
              "https://127.0.0.1:" + server.getAddress().getPort() + "/")).build(),
          HttpResponse.BodyHandlers.ofString());
      return response.body();
    }

    @Override
    public void close() {
      server.stop(0);
    }
  }

  /** A self-signed certificate and key on disk, generated with the JDK's own keytool. */
  private record Certs(Path certPem, Path keyPem, Path keystore, String password) {

    static Certs selfSigned(Path dir, String cn) throws Exception {
      Files.createDirectories(dir);
      Path store = dir.resolve("gen.p12");
      run("keytool", "-genkeypair", "-alias", "kahshe", "-keyalg", "RSA", "-keysize", "2048",
          "-validity", "1", "-dname", "CN=" + cn, "-ext", "SAN=dns:localhost,ip:127.0.0.1",
          "-keystore", store.toString(), "-storetype", "PKCS12",
          "-storepass", "changeit", "-keypass", "changeit");

      Path certPem = dir.resolve("tls.crt");
      run("keytool", "-exportcert", "-rfc", "-alias", "kahshe", "-keystore", store.toString(),
          "-storepass", "changeit", "-file", certPem.toString());

      // keytool cannot export a private key, so read it out of the store and write PKCS#8 PEM.
      java.security.KeyStore ks = java.security.KeyStore.getInstance("PKCS12");
      try (var in = Files.newInputStream(store)) {
        ks.load(in, "changeit".toCharArray());
      }
      java.security.Key key = ks.getKey("kahshe", "changeit".toCharArray());
      Path keyPem = dir.resolve("tls.key");
      Files.writeString(keyPem, "-----BEGIN PRIVATE KEY-----\n"
          + java.util.Base64.getMimeEncoder(64, new byte[] {'\n'}).encodeToString(key.getEncoded())
          + "\n-----END PRIVATE KEY-----\n");
      return new Certs(certPem, keyPem, store, "changeit");
    }

    Path toPkcs12(Path target, String password) throws Exception {
      java.security.KeyStore ks = java.security.KeyStore.getInstance("PKCS12");
      try (var in = Files.newInputStream(keystore)) {
        ks.load(in, this.password.toCharArray());
      }
      java.security.KeyStore out = java.security.KeyStore.getInstance("PKCS12");
      out.load(null, null);
      out.setKeyEntry("kahshe", ks.getKey("kahshe", this.password.toCharArray()),
          password.toCharArray(), ks.getCertificateChain("kahshe"));
      try (var os = Files.newOutputStream(target)) {
        out.store(os, password.toCharArray());
      }
      return target;
    }

    SSLContext trustingContext() throws Exception {
      return trustingContext(null);
    }

    /** A client context that trusts anything: the server's identity is not what is under test. */
    SSLContext trustingContext(Path clientStore) throws Exception {
      TrustManager[] all = new TrustManager[] {
        new X509TrustManager() {
          public void checkClientTrusted(java.security.cert.X509Certificate[] c, String a) {}
          public void checkServerTrusted(java.security.cert.X509Certificate[] c, String a) {}
          public java.security.cert.X509Certificate[] getAcceptedIssuers() {
            return new java.security.cert.X509Certificate[0];
          }
        }
      };
      javax.net.ssl.KeyManager[] keys = null;
      if (clientStore != null) {
        java.security.KeyStore ks = java.security.KeyStore.getInstance("PKCS12");
        try (var in = Files.newInputStream(clientStore)) {
          ks.load(in, "changeit".toCharArray());
        }
        javax.net.ssl.KeyManagerFactory kmf = javax.net.ssl.KeyManagerFactory.getInstance(
            javax.net.ssl.KeyManagerFactory.getDefaultAlgorithm());
        kmf.init(ks, "changeit".toCharArray());
        keys = kmf.getKeyManagers();
      }
      SSLContext context = SSLContext.getInstance("TLS");
      context.init(keys, all, new java.security.SecureRandom());
      return context;
    }

    static void run(String... command) throws Exception {
      Process p = new ProcessBuilder(command).redirectErrorStream(true).start();
      String out = new String(p.getInputStream().readAllBytes(), StandardCharsets.UTF_8);
      assertEquals(0, p.waitFor(), command[0] + " failed: " + out);
      assertNotNull(out);
    }
  }
}
