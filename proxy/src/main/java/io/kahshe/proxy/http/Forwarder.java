package io.kahshe.proxy.http;

import java.io.IOException;
import java.net.URI;
import java.net.http.HttpClient;
import java.net.http.HttpRequest;
import java.net.http.HttpResponse;
import java.time.Duration;
import java.util.List;
import java.util.Locale;
import java.util.Map;
import java.util.Set;

/** Forwards a request verbatim to the backing catalog and returns the raw response. */
public final class Forwarder {
  /**
   * Headers not copied to the backend request.
   *
   * <p>{@code expect} and {@code upgrade} must stay listed: {@code HttpRequest.Builder.header}
   * throws on them as restricted names, while the JDK's HttpServer answers {@code 100 Continue}
   * itself and still shows the header to this handler. Clients that send {@code Expect:
   * 100-continue} for larger bodies — curl does, above roughly 1 KB — would otherwise fail every
   * passthrough and plan request. The rest are hop-by-hop or recomputed per request.
   */
  private static final Set<String> SKIP_REQUEST_HEADERS =
      Set.of(
          "host", "connection", "content-length", "accept-encoding", "transfer-encoding",
          "expect", "upgrade");
  private static final Set<String> SKIP_RESPONSE_HEADERS =
      Set.of("connection", "content-length", "transfer-encoding", "content-encoding");

  private final HttpClient client;
  private final String backendBase;
  private final long timeoutMs;

  public Forwarder(String backendBase, long timeoutMs) {
    this(backendBase, timeoutMs, "");
  }

  /**
   * @param backendCa a PEM CA bundle to trust the backend's certificate, or empty for the JVM
   *     default. The passthrough uses the JDK's client while the catalog clients use Iceberg's,
   *     so both have to be told about a private authority separately or half the requests fail.
   */
  public Forwarder(String backendBase, long timeoutMs, String backendCa) {
    this.backendBase = backendBase;
    this.timeoutMs = timeoutMs;
    HttpClient.Builder builder = HttpClient.newBuilder().connectTimeout(Duration.ofSeconds(5));
    if (backendCa != null && !backendCa.isBlank()) {
      try {
        builder = builder.sslContext(
            io.kahshe.proxy.catalog.BackendTls.contextTrusting(java.nio.file.Path.of(backendCa.trim())));
      } catch (java.io.IOException e) {
        // At construction, so a bad CA path fails startup rather than every passthrough.
        throw new IllegalStateException(
            "could not trust the backend CA at " + backendCa + ": " + e.getMessage(), e);
      }
    }
    this.client = builder.build();
  }

  record Response(int status, Map<String, List<String>> headers, byte[] body) {}

  Response forward(String method, String pathWithQuery, Map<String, List<String>> headers, byte[] body)
      throws IOException, InterruptedException {
    HttpRequest.Builder builder =
        HttpRequest.newBuilder(URI.create(backendBase + pathWithQuery))
            .timeout(Duration.ofMillis(timeoutMs));

    HttpRequest.BodyPublisher publisher =
        body.length == 0
            ? HttpRequest.BodyPublishers.noBody()
            : HttpRequest.BodyPublishers.ofByteArray(body);
    builder.method(method, publisher);

    headers.forEach(
        (name, values) -> {
          if (!SKIP_REQUEST_HEADERS.contains(name.toLowerCase(Locale.ROOT))) {
            values.forEach(value -> builder.header(name, value));
          }
        });

    HttpResponse<byte[]> response =
        client.send(builder.build(), HttpResponse.BodyHandlers.ofByteArray());

    Map<String, List<String>> responseHeaders =
        response.headers().map().entrySet().stream()
            .filter(e -> !SKIP_RESPONSE_HEADERS.contains(e.getKey().toLowerCase(Locale.ROOT)))
            .filter(e -> !e.getKey().startsWith(":"))
            .collect(
                java.util.stream.Collectors.toMap(Map.Entry::getKey, Map.Entry::getValue));
    return new Response(response.statusCode(), responseHeaders, response.body());
  }
}
