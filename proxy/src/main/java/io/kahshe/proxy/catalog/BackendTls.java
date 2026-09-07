package io.kahshe.proxy.catalog;

import io.kahshe.common.Pem;
import java.io.IOException;
import java.nio.file.Path;
import java.util.Map;
import javax.net.ssl.HostnameVerifier;
import javax.net.ssl.SSLContext;
import org.apache.iceberg.rest.auth.TLSConfigurer;

/**
 * Trusting a backing catalog that presents a certificate from a PRIVATE authority.
 *
 * <p>Common in enterprises and not served by the JVM's default trust store, which knows only
 * public roots. kahshe talks to the backend through two different clients and both need the same
 * trust: Iceberg's {@code RESTCatalog}, which wraps Apache HttpClient 5, and the passthrough
 * {@code Forwarder}, which uses the JDK's own client. This class is the Iceberg half.
 *
 * <p><b>Why this shape.</b> Iceberg 1.11.0 has exactly one seam for it — the catalog property
 * {@code rest.client.tls.configurer-impl}, naming a {@link TLSConfigurer} with a no-arg
 * constructor whose {@code initialize} receives the whole property map. There is no truststore
 * path or CA property to set; a property-driven implementation is proposed upstream but is not in
 * 1.11.0, so its names are deliberately not used here.
 *
 * <p>The alternative was a JVM-wide default {@code SSLContext} — which does work, because
 * Iceberg's client calls {@code useSystemProperties()} — and was rejected: it would also
 * re-trust, or fail to trust, every other TLS client in the process, the S3 client that reads and
 * writes index files included. A catalog's private CA has no business changing how object storage
 * is trusted.
 */
public final class BackendTls implements TLSConfigurer {

  /** The Iceberg property that names this class. Package-private upstream, so it is a literal. */
  public static final String CONFIGURER_PROPERTY = "rest.client.tls.configurer-impl";

  /** kahshe's own property, read out of the map Iceberg hands {@link #initialize}. */
  public static final String CA_PROPERTY = "kahshe.backend.ca";

  private volatile SSLContext context;

  /** Required: Iceberg constructs this reflectively. */
  public BackendTls() {}

  @Override
  public void initialize(Map<String, String> properties) {
    String ca = properties == null ? null : properties.get(CA_PROPERTY);
    if (ca == null || ca.isBlank()) {
      return; // no private CA configured: sslContext() falls through to the default
    }
    try {
      this.context = contextTrusting(Path.of(ca.trim()));
    } catch (IOException e) {
      // Failing loudly here beats trusting the default and failing at the first handshake with a
      // message about an untrusted certificate, which sends the reader looking in the wrong place.
      throw new IllegalStateException(
          "could not build backend TLS trust from " + ca + ": " + e.getMessage(), e);
    }
  }

  @Override
  public SSLContext sslContext() {
    SSLContext configured = context;
    if (configured != null) {
      return configured;
    }
    try {
      return SSLContext.getDefault();
    } catch (Exception e) {
      throw new IllegalStateException("no default SSLContext", e);
    }
  }

  /**
   * Null on purpose: it keeps Iceberg's built-in hostname verification. Returning a permissive
   * verifier here is the usual way a private-CA setup quietly becomes one that accepts any
   * certificate at all.
   */
  @Override
  public HostnameVerifier hostnameVerifier() {
    return null;
  }

  /** A context trusting the CAs in {@code caBundle} and no others; shared with the Forwarder. */
  public static SSLContext contextTrusting(Path caBundle) throws IOException {
    try {
      SSLContext context = SSLContext.getInstance("TLS");
      context.init(null, Pem.trustManagersFor(caBundle), null);
      return context;
    } catch (IOException e) {
      throw e;
    } catch (Exception e) {
      throw new IOException("could not build an SSL context: " + e.getMessage(), e);
    }
  }
}
