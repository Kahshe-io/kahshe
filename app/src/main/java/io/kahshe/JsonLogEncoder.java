package io.kahshe;

import ch.qos.logback.classic.spi.ILoggingEvent;
import ch.qos.logback.classic.spi.IThrowableProxy;
import ch.qos.logback.classic.spi.ThrowableProxyUtil;
import ch.qos.logback.core.encoder.EncoderBase;
import java.nio.charset.StandardCharsets;
import java.time.Instant;
import java.time.ZoneOffset;
import java.time.format.DateTimeFormatter;

/**
 * One JSON object per log line, for a collector that indexes fields rather than greps them:
 * {@code ts}, {@code level}, {@code logger}, {@code thread}, {@code msg}, and when the event
 * carries one {@code exception} ({@code class}, {@code message}, {@code stack} as one string),
 * in that order. {@code logback.xml} selects it with {@code KAHSHE_LOG_FORMAT=json}.
 *
 * <p>Hand-written rather than a JSON-logging library: the object is five fixed fields, and the
 * escaping is the whole of RFC 8259's string grammar, which is shorter than the dependency.
 */
public final class JsonLogEncoder extends EncoderBase<ILoggingEvent> {

  // Always millis, always Z: ISO_INSTANT drops trailing zero millis, which makes the column
  // ragged and a fixed-width parser fail on the short ones.
  private static final DateTimeFormatter TS =
      DateTimeFormatter.ofPattern("yyyy-MM-dd'T'HH:mm:ss.SSS'Z'").withZone(ZoneOffset.UTC);

  @Override
  public byte[] headerBytes() {
    return null;
  }

  @Override
  public byte[] footerBytes() {
    return null;
  }

  @Override
  public byte[] encode(ILoggingEvent event) {
    StringBuilder out = new StringBuilder(256);
    out.append("{\"ts\":\"").append(TS.format(Instant.ofEpochMilli(event.getTimeStamp())))
        .append('"');
    field(out, "level", String.valueOf(event.getLevel()));
    field(out, "logger", event.getLoggerName());
    field(out, "thread", event.getThreadName());
    field(out, "msg", event.getFormattedMessage());
    IThrowableProxy thrown = event.getThrowableProxy();
    if (thrown != null) {
      out.append(",\"exception\":{\"class\":");
      string(out, thrown.getClassName());
      field(out, "message", thrown.getMessage());
      field(out, "stack", ThrowableProxyUtil.asString(thrown));
      out.append('}');
    }
    out.append("}\n");
    return out.toString().getBytes(StandardCharsets.UTF_8);
  }

  private static void field(StringBuilder out, String name, String value) {
    out.append(",\"").append(name).append("\":");
    string(out, value);
  }

  /** RFC 8259 §7: the quote, the backslash and the control characters are all that must escape. */
  private static void string(StringBuilder out, String value) {
    if (value == null) {
      out.append("null");
      return;
    }
    out.append('"');
    for (int i = 0; i < value.length(); i++) {
      char c = value.charAt(i);
      switch (c) {
        case '"' -> out.append("\\\"");
        case '\\' -> out.append("\\\\");
        case '\n' -> out.append("\\n");
        case '\r' -> out.append("\\r");
        case '\t' -> out.append("\\t");
        default -> {
          if (c < 0x20) {
            out.append(String.format("\\u%04x", (int) c));
          } else {
            out.append(c);
          }
        }
      }
    }
    out.append('"');
  }
}
