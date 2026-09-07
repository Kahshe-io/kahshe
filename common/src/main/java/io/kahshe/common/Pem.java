package io.kahshe.common;

import java.io.ByteArrayInputStream;
import java.io.IOException;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Path;
import java.security.KeyStore;
import java.security.cert.CertificateFactory;
import java.security.cert.X509Certificate;
import java.util.ArrayList;
import java.util.Base64;
import java.util.List;
import javax.net.ssl.TrustManager;
import javax.net.ssl.TrustManagerFactory;

/**
 * PEM reading for both ends of kahshe's TLS: the server loads the certificate it presents, the
 * client the CA bundle it trusts a backing catalog through. One implementation of
 * base64-between-markers, so one set of bugs.
 */
public final class Pem {

  private Pem() {}

  /** Every certificate in a PEM file, in order; a chain is leaf first. */
  public static List<X509Certificate> certificates(Path pem) throws IOException {
    try {
      CertificateFactory factory = CertificateFactory.getInstance("X.509");
      List<X509Certificate> out = new ArrayList<>();
      for (byte[] der : blocks(pem, "CERTIFICATE")) {
        out.add((X509Certificate) factory.generateCertificate(new ByteArrayInputStream(der)));
      }
      return out;
    } catch (IOException e) {
      throw e;
    } catch (Exception e) {
      throw new IOException("could not read certificates from " + pem + ": " + e.getMessage(), e);
    }
  }

  /**
   * Trust managers that accept a chain signed by any CA in {@code caBundle}, and nothing else.
   *
   * <p>Deliberately NOT additive with the JVM's default trust store. A private CA is usually the
   * only authority that should be trusted on that connection, and silently also trusting every
   * public root would make a misissued public certificate indistinguishable from the intended one.
   */
  public static TrustManager[] trustManagersFor(Path caBundle) throws IOException {
    try {
      List<X509Certificate> cas = certificates(caBundle);
      if (cas.isEmpty()) {
        throw new IOException("no CERTIFICATE block in " + caBundle);
      }
      KeyStore trusted = KeyStore.getInstance(KeyStore.getDefaultType());
      trusted.load(null, null);
      int i = 0;
      for (X509Certificate ca : cas) {
        trusted.setCertificateEntry("ca-" + i++, ca);
      }
      TrustManagerFactory factory =
          TrustManagerFactory.getInstance(TrustManagerFactory.getDefaultAlgorithm());
      factory.init(trusted);
      return factory.getTrustManagers();
    } catch (IOException e) {
      throw e;
    } catch (Exception e) {
      throw new IOException("could not build trust from " + caBundle + ": " + e.getMessage(), e);
    }
  }

  /** The base64 payloads of every {@code -----BEGIN <label>-----} block, in order. */
  public static List<byte[]> blocks(Path pem, String label) throws IOException {
    String text = Files.readString(pem, StandardCharsets.UTF_8);
    String begin = "-----BEGIN " + label + "-----";
    String end = "-----END " + label + "-----";
    List<byte[]> out = new ArrayList<>();
    int at = 0;
    while (true) {
      int start = text.indexOf(begin, at);
      if (start < 0) {
        return out;
      }
      int stop = text.indexOf(end, start);
      if (stop < 0) {
        throw new IOException(pem + " has a " + begin + " with no matching " + end);
      }
      out.add(Base64.getDecoder().decode(text.substring(start + begin.length(), stop)
          .replaceAll("\\s", "")));
      at = stop + end.length();
    }
  }
}
