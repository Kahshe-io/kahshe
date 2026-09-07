package io.kahshe;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertInstanceOf;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.junit.jupiter.api.Assumptions.assumeTrue;

import ch.qos.logback.classic.Level;
import ch.qos.logback.classic.Logger;
import ch.qos.logback.classic.LoggerContext;
import ch.qos.logback.classic.encoder.PatternLayoutEncoder;
import ch.qos.logback.classic.joran.JoranConfigurator;
import ch.qos.logback.classic.spi.ILoggingEvent;
import ch.qos.logback.classic.spi.LoggingEvent;
import ch.qos.logback.core.ConsoleAppender;
import ch.qos.logback.core.joran.spi.JoranException;
import ch.qos.logback.core.status.Status;
import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import java.io.IOException;
import java.nio.charset.StandardCharsets;
import java.util.ArrayList;
import java.util.List;
import java.util.regex.Pattern;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.Test;

/**
 * The JSON encoder must emit what a collector can parse, whatever the message holds, and
 * logback.xml must still give a person the text line they had under slf4j-simple.
 */
class JsonLogEncoderTest {

  private static final ObjectMapper JSON = new ObjectMapper();
  private static final long TS = 1_757_000_000_123L; // 2025-09-04T15:33:20.123Z
  private static final Pattern TEXT_LINE = Pattern.compile(
      "^\\d{4}-\\d{2}-\\d{2}T\\d{2}:\\d{2}:\\d{2}\\.\\d{3} \\[worker-1] INFO +io\\.kahshe\\.Probe - hello\n");

  private final LoggerContext context = new LoggerContext();

  @AfterEach
  void clearFormat() {
    System.clearProperty("KAHSHE_LOG_FORMAT");
  }

  @Test
  void plainEventIsOneJsonObjectPerLineWithFixedFieldOrder() throws IOException {
    String line = encode(event("hello", null));

    assertTrue(line.endsWith("\n"), "newline-terminated: " + line);
    assertEquals(line.length() - 1, line.indexOf('\n'), "one line: " + line);
    JsonNode node = JSON.readTree(line);
    assertEquals(List.of("ts", "level", "logger", "thread", "msg"), fields(node));
    assertEquals("2025-09-04T15:33:20.123Z", node.get("ts").asText());
    assertEquals("INFO", node.get("level").asText());
    assertEquals("io.kahshe.Probe", node.get("logger").asText());
    assertEquals("worker-1", node.get("thread").asText());
    assertEquals("hello", node.get("msg").asText());
  }

  @Test
  void exceptionCarriesClassMessageAndStackAsOneString() throws IOException {
    Throwable t = new IllegalStateException("boom", new IOException("root"));
    String line = encode(event("failed", t));

    assertEquals(line.length() - 1, line.indexOf('\n'), "the stack must not break the line");
    JsonNode node = JSON.readTree(line);
    assertEquals(List.of("ts", "level", "logger", "thread", "msg", "exception"), fields(node));
    JsonNode ex = node.get("exception");
    assertEquals(List.of("class", "message", "stack"), fields(ex));
    assertEquals("java.lang.IllegalStateException", ex.get("class").asText());
    assertEquals("boom", ex.get("message").asText());
    String stack = ex.get("stack").asText();
    assertTrue(stack.startsWith("java.lang.IllegalStateException: boom"), stack);
    assertTrue(stack.contains("at io.kahshe.JsonLogEncoderTest"), stack);
    assertTrue(stack.contains("Caused by: java.io.IOException: root"), stack);
  }

  @Test
  void quotesBackslashesNewlinesTabsAndControlCharactersSurviveAParse() throws IOException {
    String awkward = "say \"hi\" to C:\\temp\nsecond line\ttabbed\u0001bell\u001f";
    String line = encode(event(awkward, null));

    assertEquals(line.length() - 1, line.indexOf('\n'), "the message's newline is escaped");
    assertEquals(awkward, JSON.readTree(line).get("msg").asText());
  }

  @Test
  void nullMessageIsJsonNull() throws IOException {
    JsonNode node = JSON.readTree(encode(event(null, null)));
    assertTrue(node.get("msg").isNull(), node.toString());
  }

  @Test
  void parameterisedMessageIsFormatted() throws IOException {
    Logger logger = context.getLogger("io.kahshe.Probe");
    LoggingEvent e = new LoggingEvent(Logger.FQCN, logger, Level.WARN, "kept {} of {}", null,
        new Object[] {3, 10});
    JsonNode node = JSON.readTree(encode(e));
    assertEquals("kept 3 of 10", node.get("msg").asText());
    assertEquals("WARN", node.get("level").asText());
  }

  @Test
  void textFormatKeepsTheTimestampLevelLoggerMessageLine() throws JoranException {
    System.setProperty("KAHSHE_LOG_FORMAT", "text");
    ConsoleAppender<ILoggingEvent> appender = stderrAppender(configured());
    assertInstanceOf(PatternLayoutEncoder.class, appender.getEncoder());

    String line = new String(appender.getEncoder().encode(event("hello", null)),
        StandardCharsets.UTF_8);
    assertTrue(TEXT_LINE.matcher(line).lookingAt(), line);
    String withStack = new String(
        appender.getEncoder().encode(event("hello", new IllegalStateException("boom"))),
        StandardCharsets.UTF_8);
    assertTrue(withStack.contains("java.lang.IllegalStateException: boom"), withStack);
    assertTrue(withStack.contains("\tat io.kahshe.JsonLogEncoderTest"), withStack);
  }

  @Test
  void jsonFormatSelectsTheJsonEncoder() throws JoranException, IOException {
    System.setProperty("KAHSHE_LOG_FORMAT", "json");
    ConsoleAppender<ILoggingEvent> appender = stderrAppender(configured());

    assertInstanceOf(JsonLogEncoder.class, appender.getEncoder());
    JsonNode node = JSON.readTree(new String(appender.getEncoder().encode(event("hello", null)),
        StandardCharsets.UTF_8));
    assertEquals("hello", node.get("msg").asText());
  }

  @Test
  void textIsTheDefault() throws JoranException {
    assumeTrue(System.getenv("KAHSHE_LOG_FORMAT") == null, "the shell has chosen a format");
    assertInstanceOf(PatternLayoutEncoder.class, stderrAppender(configured()).getEncoder());
  }

  @Test
  void configurationIsCleanSoLogbackDoesNotDumpItsStatusAtStartup() throws JoranException {
    for (String format : List.of("text", "json")) {
      System.setProperty("KAHSHE_LOG_FORMAT", format);
      List<Status> noisy = new ArrayList<>();
      for (Status s : configured().getStatusManager().getCopyOfStatusList()) {
        if (s.getLevel() >= Status.WARN) {
          noisy.add(s);
        }
      }
      assertEquals(List.of(), noisy, "one WARN and logback prints its whole status log: " + noisy);
    }
  }

  @Test
  void unknownFormatIsAnErrorNamingTheValueAndAttachesNothing() throws JoranException {
    System.setProperty("KAHSHE_LOG_FORMAT", "bogus");
    LoggerContext configured = configured();
    assertNull(configured.getLogger(Logger.ROOT_LOGGER_NAME).getAppender("stderr"));
    assertTrue(configured.getStatusManager().getCopyOfStatusList().stream()
        .anyMatch(s -> s.getLevel() >= Status.WARN && s.getMessage().contains("bogus")),
        "nothing named the bad value: " + configured.getStatusManager().getCopyOfStatusList());
  }

  @Test
  void logsGoToStderrAndTheIcebergReporterStaysQuiet() throws JoranException {
    System.setProperty("KAHSHE_LOG_FORMAT", "json");
    LoggerContext configured = configured();
    assertEquals(Level.INFO, configured.getLogger(Logger.ROOT_LOGGER_NAME).getLevel());
    // the CLI subcommands answer on stdout; a log line among them breaks whoever parses that
    assertEquals("System.err", stderrAppender(configured).getTarget());
    assertEquals(Level.WARN,
        configured.getLogger("org.apache.iceberg.metrics.LoggingMetricsReporter").getLevel());
  }

  private String encode(ILoggingEvent event) {
    JsonLogEncoder encoder = new JsonLogEncoder();
    encoder.setContext(context);
    encoder.start();
    return new String(encoder.encode(event), StandardCharsets.UTF_8);
  }

  private LoggingEvent event(String message, Throwable t) {
    Logger logger = context.getLogger("io.kahshe.Probe");
    LoggingEvent e = new LoggingEvent(Logger.FQCN, logger, Level.INFO, message, t, null);
    e.setTimeStamp(TS);
    e.setThreadName("worker-1");
    return e;
  }

  private static List<String> fields(JsonNode node) {
    List<String> names = new ArrayList<>();
    node.fieldNames().forEachRemaining(names::add);
    return names;
  }

  /** The one appender logback.xml attaches to root, whichever fragment the format chose. */
  @SuppressWarnings("unchecked")
  private static ConsoleAppender<ILoggingEvent> stderrAppender(LoggerContext configured) {
    ConsoleAppender<ILoggingEvent> appender = (ConsoleAppender<ILoggingEvent>)
        configured.getLogger(Logger.ROOT_LOGGER_NAME).getAppender("stderr");
    assertNotNull(appender, "root has no stderr appender");
    assertNotNull(appender.getEncoder());
    return appender;
  }

  /** logback.xml applied to a fresh context; the format comes from the system property. */
  private static LoggerContext configured() throws JoranException {
    LoggerContext fresh = new LoggerContext();
    JoranConfigurator configurator = new JoranConfigurator();
    configurator.setContext(fresh);
    configurator.doConfigure(JsonLogEncoderTest.class.getResource("/logback.xml"));
    return fresh;
  }
}
