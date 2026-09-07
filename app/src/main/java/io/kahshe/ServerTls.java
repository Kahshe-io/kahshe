package io.kahshe;

import com.sun.net.httpserver.HttpsConfigurator;
import com.sun.net.httpserver.HttpsParameters;
import java.io.IOException;
import java.io.InputStream;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Path;
import java.security.KeyFactory;
import java.security.KeyStore;
import java.security.PrivateKey;
import java.security.cert.Certificate;
import java.security.cert.X509Certificate;
import java.security.spec.PKCS8EncodedKeySpec;
import java.util.List;
import java.util.Locale;
import java.util.Map;
import java.util.concurrent.atomic.AtomicReference;
import javax.net.ssl.KeyManagerFactory;
import javax.net.ssl.SSLContext;
import javax.net.ssl.SSLParameters;
import javax.net.ssl.TrustManagerFactory;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * TLS for the ports kahshe serves.
 *
 * <p>Absent configuration means plaintext, which is what kahshe did before this existed and what
 * a local `docker compose` still does. It is opt-in rather than default because the quickstart
 * has no certificate to present and demanding one would make the first five minutes harder for
 * no security gain on a loopback socket.
 *
 * <p>Two ways to supply a certificate, and exactly one may be used at a time:
 *
 * <ul>
 *   <li><b>PEM</b> — {@code KAHSHE_TLS_CERT} and {@code KAHSHE_TLS_KEY}. This is the Kubernetes
 *       shape: cert-manager writes {@code tls.crt} and {@code tls.key} into a Secret and the
 *       chart mounts them as files, so nothing has to be converted.
 *   <li><b>Keystore</b> — {@code KAHSHE_TLS_KEYSTORE} with its password and optional type
 *       (PKCS12 by default). This is the shape a Java shop already has.
 * </ul>
 *
 * <p>Supplying both is refused at startup rather than resolved by precedence: two answers to
 * "which certificate does this serve" is a configuration that half works, and the half that
 * works is whichever the code happened to check first.
 *
 * <p><b>Certificates are re-read while the process runs.</b> A proxy that loads its certificate
 * once serves an expired one the day cert-manager renews it, which is a 3 a.m. failure with a
 * 90-day fuse. The files' modification times are checked on a connection at most once every
 * {@code KAHSHE_TLS_RELOAD_MS}; a changed file is loaded and takes effect on the next handshake,
 * and one that fails to load leaves the previous context serving rather than dropping TLS.
 */
public final class ServerTls {
  private static final Logger LOG = LoggerFactory.getLogger(ServerTls.class);

  /** Whether, and how strictly, a client certificate is demanded. */
  public enum ClientAuth {
    NONE,
    /** Requested; a client that presents none is still served. */
    WANT,
    /** Required; a client that presents none is rejected at the handshake. */
    NEED
  }

  /**
   * What was configured. {@link #enabled()} false is the plaintext case.
   *
   * @param certPem PEM certificate chain, leaf first
   * @param keyPem PKCS#8 private key for that chain
   * @param keystore a PKCS12 or JKS keystore holding both
   * @param clientCaPem the CA that must have signed a client certificate, for mTLS
   * @param reloadMs how often the certificate files are re-checked for a rotation
   * @param admin whether the admin port is served over TLS as well
   */
  public record Settings(
      Path certPem,
      Path keyPem,
      Path keystore,
      String keystorePassword,
      String keystoreType,
      Path clientCaPem,
      ClientAuth clientAuth,
      long reloadMs,
      boolean admin) {

    public boolean enabled() {
      return certPem != null || keystore != null;
    }
  }

  /**
   * Reads the settings, refusing a contradictory combination by name.
   *
   * <p>Every refusal here is at startup and says what to fix. The alternative — starting and
   * serving plaintext when the operator asked for TLS — is the failure this method exists to
   * prevent, and it is silent.
   */
  public static Settings fromEnv(Map<String, String> env) {
    Path cert = path(env.get("KAHSHE_TLS_CERT"));
    Path key = path(env.get("KAHSHE_TLS_KEY"));
    Path keystore = path(env.get("KAHSHE_TLS_KEYSTORE"));
    String keystorePassword = env.getOrDefault("KAHSHE_TLS_KEYSTORE_PASSWORD", "");
    String keystoreType = env.getOrDefault("KAHSHE_TLS_KEYSTORE_TYPE", "PKCS12");
    Path clientCa = path(env.get("KAHSHE_TLS_CLIENT_CA"));
    String authText = env.getOrDefault("KAHSHE_TLS_CLIENT_AUTH", "none").toLowerCase(Locale.ROOT);
    long reloadMs = longEnv(env.get("KAHSHE_TLS_RELOAD_MS"), 60_000);
    boolean admin = Boolean.parseBoolean(env.getOrDefault("KAHSHE_TLS_ADMIN", "false"));

    if (cert != null && keystore != null) {
      throw new IllegalArgumentException(
          "both KAHSHE_TLS_CERT and KAHSHE_TLS_KEYSTORE are set; use one. The PEM pair is the "
              + "Kubernetes shape, the keystore the Java one, and there is no sensible precedence "
              + "between them");
    }
    if ((cert == null) != (key == null)) {
      throw new IllegalArgumentException(
          "KAHSHE_TLS_CERT and KAHSHE_TLS_KEY must be set together; one without the other cannot "
              + "serve TLS");
    }
    if (keystore != null && keystorePassword.isEmpty()) {
      throw new IllegalArgumentException(
          "KAHSHE_TLS_KEYSTORE is set without KAHSHE_TLS_KEYSTORE_PASSWORD");
    }

    ClientAuth clientAuth =
        switch (authText) {
          case "none" -> ClientAuth.NONE;
          case "want" -> ClientAuth.WANT;
          case "need", "require", "required" -> ClientAuth.NEED;
          default -> throw new IllegalArgumentException(
              "KAHSHE_TLS_CLIENT_AUTH must be none, want or need; got '" + authText + "'");
        };
    if (clientAuth != ClientAuth.NONE && clientCa == null) {
      throw new IllegalArgumentException(
          "KAHSHE_TLS_CLIENT_AUTH=" + authText + " needs KAHSHE_TLS_CLIENT_CA: without a CA to "
              + "check against, no client certificate can be accepted and every connection would "
              + "fail the handshake");
    }
    if (clientAuth != ClientAuth.NONE && cert == null && keystore == null) {
      throw new IllegalArgumentException(
          "KAHSHE_TLS_CLIENT_AUTH is set but no server certificate is configured; mutual TLS "
              + "still needs this side to present one");
    }
    return new Settings(cert, key, keystore, keystorePassword, keystoreType, clientCa, clientAuth,
        reloadMs, admin);
  }

  private final Settings settings;
  private final AtomicReference<SSLContext> current = new AtomicReference<>();
  private volatile long checkedAtMs;
  private volatile long loadedStamp;

  public ServerTls(Settings settings) throws IOException {
    this.settings = settings;
    this.current.set(build());
    this.loadedStamp = stamp();
    this.checkedAtMs = System.currentTimeMillis();
    LOG.info(
        "TLS enabled on the data plane ({}), client auth {}, certificates re-read every {} ms{}",
        settings.keystore() != null ? "keystore " + settings.keystore() : "PEM " + settings.certPem(),
        settings.clientAuth().name().toLowerCase(Locale.ROOT),
        settings.reloadMs(),
        settings.admin() ? ", admin port too" : "");
  }

  /** The configurator to hand {@code HttpsServer}; it re-reads rotated certificates. */
  public HttpsConfigurator configurator() {
    return new HttpsConfigurator(current.get()) {
      @Override
      public void configure(HttpsParameters params) {
        SSLContext context = maybeReload();
        SSLParameters ssl = context.getDefaultSSLParameters();
        // TLS 1.2 is kept for engines on older JDKs; anything below it is not offered at all.
        ssl.setProtocols(new String[] {"TLSv1.3", "TLSv1.2"});
        switch (settings.clientAuth()) {
          case NEED -> ssl.setNeedClientAuth(true);
          case WANT -> ssl.setWantClientAuth(true);
          case NONE -> { }
        }
        params.setSSLParameters(ssl);
      }
    };
  }

  /**
   * Returns the current context, re-reading the files first if they have changed and the reload
   * interval has elapsed. A load that fails is logged and the previous context kept: serving the
   * old certificate is recoverable, and dropping TLS mid-rotation is not.
   */
  SSLContext maybeReload() {
    long now = System.currentTimeMillis();
    if (now - checkedAtMs < settings.reloadMs()) {
      return current.get();
    }
    checkedAtMs = now;
    long stamp = stamp();
    if (stamp == loadedStamp) {
      return current.get();
    }
    try {
      SSLContext rebuilt = build();
      current.set(rebuilt);
      loadedStamp = stamp;
      LOG.info("TLS certificate reloaded after a change on disk");
      return rebuilt;
    } catch (Exception e) {
      LOG.warn("TLS certificate changed on disk but failed to load; keeping the previous one", e);
      loadedStamp = stamp; // do not retry a broken file every interval
      return current.get();
    }
  }

  /** A cheap fingerprint of the certificate files: last-modified plus size, summed. */
  private long stamp() {
    long stamp = 0;
    for (Path p : new Path[] {settings.certPem(), settings.keyPem(), settings.keystore(),
        settings.clientCaPem()}) {
      if (p == null) {
        continue;
      }
      try {
        stamp = stamp * 31 + Files.getLastModifiedTime(p).toMillis() + Files.size(p);
      } catch (IOException e) {
        // A file that cannot be stat'ed is a change worth reacting to, not one worth throwing on.
        stamp = stamp * 31 - 1;
      }
    }
    return stamp;
  }

  private SSLContext build() throws IOException {
    try {
      KeyStore store =
          settings.keystore() != null ? loadKeystore() : keyStoreFromPem();
      KeyManagerFactory keys = KeyManagerFactory.getInstance(KeyManagerFactory.getDefaultAlgorithm());
      keys.init(store, keyPassword());

      TrustManagerFactory trust = null;
      if (settings.clientCaPem() != null) {
        KeyStore trusted = KeyStore.getInstance(KeyStore.getDefaultType());
        trusted.load(null, null);
        int i = 0;
        for (X509Certificate ca : io.kahshe.common.Pem.certificates(settings.clientCaPem())) {
          trusted.setCertificateEntry("client-ca-" + i++, ca);
        }
        trust = TrustManagerFactory.getInstance(TrustManagerFactory.getDefaultAlgorithm());
        trust.init(trusted);
      }

      SSLContext context = SSLContext.getInstance("TLS");
      context.init(keys.getKeyManagers(), trust == null ? null : trust.getTrustManagers(), null);
      return context;
    } catch (IOException e) {
      throw e;
    } catch (Exception e) {
      throw new IOException("could not build the TLS context: " + e.getMessage(), e);
    }
  }

  private char[] keyPassword() {
    return settings.keystore() != null ? settings.keystorePassword().toCharArray() : new char[0];
  }

  private KeyStore loadKeystore() throws Exception {
    KeyStore store = KeyStore.getInstance(settings.keystoreType());
    try (InputStream in = Files.newInputStream(settings.keystore())) {
      store.load(in, settings.keystorePassword().toCharArray());
    }
    return store;
  }

  /** A PEM chain plus a PKCS#8 key, assembled into an in-memory keystore. */
  private KeyStore keyStoreFromPem() throws Exception {
    List<X509Certificate> chain = io.kahshe.common.Pem.certificates(settings.certPem());
    if (chain.isEmpty()) {
      throw new IOException("no certificate found in " + settings.certPem());
    }
    PrivateKey key = privateKey(settings.keyPem());
    KeyStore store = KeyStore.getInstance(KeyStore.getDefaultType());
    store.load(null, null);
    store.setKeyEntry("kahshe", key, new char[0], chain.toArray(new Certificate[0]));
    return store;
  }



  private static PrivateKey privateKey(Path pem) throws Exception {
    String text = Files.readString(pem, StandardCharsets.UTF_8);
    if (text.contains("BEGIN RSA PRIVATE KEY") || text.contains("BEGIN EC PRIVATE KEY")) {
      throw new IOException(
          pem + " is a PKCS#1 key; kahshe reads PKCS#8. Convert it once with: openssl pkcs8 "
              + "-topk8 -nocrypt -in " + pem + " -out key.pk8.pem");
    }
    if (text.contains("BEGIN ENCRYPTED PRIVATE KEY")) {
      throw new IOException(
          pem + " is an encrypted private key, which kahshe cannot open. Decrypt it, or use "
              + "KAHSHE_TLS_KEYSTORE, which takes a password");
    }
    List<byte[]> blocks = io.kahshe.common.Pem.blocks(pem, "PRIVATE KEY");
    if (blocks.isEmpty()) {
      throw new IOException("no PRIVATE KEY block in " + pem);
    }
    PKCS8EncodedKeySpec spec = new PKCS8EncodedKeySpec(blocks.get(0));
    // The key's own algorithm is in the PKCS#8 header; try the ones a TLS certificate uses.
    for (String algorithm : new String[] {"RSA", "EC", "Ed25519"}) {
      try {
        return KeyFactory.getInstance(algorithm).generatePrivate(spec);
      } catch (Exception ignored) {
        // try the next
      }
    }
    throw new IOException("unsupported private key algorithm in " + pem + "; RSA, EC and Ed25519 "
        + "are read");
  }



  private static Path path(String value) {
    return value == null || value.isBlank() ? null : Path.of(value.trim());
  }

  private static long longEnv(String value, long fallback) {
    if (value == null || value.isBlank()) {
      return fallback;
    }
    try {
      return Long.parseLong(value.trim());
    } catch (NumberFormatException e) {
      return fallback;
    }
  }
}
