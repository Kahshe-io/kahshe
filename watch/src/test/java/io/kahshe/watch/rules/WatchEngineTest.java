package io.kahshe.watch.rules;

import io.kahshe.format.type.gram.Grams;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertDoesNotThrow;
import static org.junit.jupiter.api.Assertions.assertTrue;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.sun.net.httpserver.HttpServer;
import io.kahshe.common.Metrics;
import io.kahshe.indexer.build.IndexBuildListener;
import io.kahshe.format.type.bloom.NgramBloom;
import io.kahshe.format.type.term.TermIndexWriter;
import java.net.InetSocketAddress;
import java.net.URI;
import java.util.List;
import java.util.Set;
import java.util.concurrent.LinkedBlockingQueue;
import java.util.concurrent.TimeUnit;
import org.junit.jupiter.api.Test;
import io.kahshe.watch.RecordingSink;
import io.kahshe.watch.WatchEngine;
import io.kahshe.watch.sink.AlertSink;
import io.kahshe.watch.sink.WebhookSink;

class WatchEngineTest {

  private static WatchRule rule(String id, List<String> match, List<String> contains,
      WatchRule.Condition condition, long minCount) {
    return WatchRule.singleColumn(id, id, WatchRule.Severity.HIGH, "lakehouse", "logs.events", "msg",
        match, contains, condition, minCount);
  }

  /**
   * Tokenizing is the most expensive thing a build does, and a contains-only rule set does not
   * need it.
   *
   * <p>Producing per-file term counts makes {@code IndexBuilder} build a {@code TermCounts} map of
   * every distinct term in the file — an unbounded per-file structure, and the one a build is
   * most likely to be OOM-killed for. {@code IndexBuildListener.readsTermCounts} defaults to TRUE
   * so a forgetful listener is safe rather than silently wrong, which without this override makes
   * a watch config of nothing but {@code contains} rules pay for that map on every file and
   * discard it. {@code contains} evidence is probed against the 3-gram set the build produces
   * anyway.
   *
   * <p>Verified by breaking it: removing the override returns true for every configuration and the
   * contains-only case below fails.
   */
  @Test
  void containsOnlyRulesDoNotAskTheBuildForTermCounts() {
    Metrics metrics = new Metrics();
    assertFalse(
        engine(metrics, false, rule("c", List.of(), List.of("BEGIN RSA PRIVATE KEY"),
            WatchRule.Condition.ANY_OF, 0)).readsTermCounts(),
        "a contains-only rule set still asks for per-file term counts, so every build pays for a "
            + "map of every distinct term in every file and then throws it away");

    assertTrue(
        engine(metrics, false, rule("m", List.of("error"), List.of(), WatchRule.Condition.ANY_OF, 1))
            .readsTermCounts(),
        "a token rule needs exact per-file counts; refusing them would make it never fire");

    // a mixed set must still pay: one token rule is enough to need the map
    assertTrue(
        engine(metrics, false,
            rule("c", List.of(), List.of("literal"), WatchRule.Condition.ANY_OF, 0),
            rule("m", List.of("error"), List.of(), WatchRule.Condition.ANY_OF, 1))
            .readsTermCounts());
  }

  private static WatchEngine engine(Metrics metrics, boolean realert, WatchRule... rules) {
    // the engine holds the seam, not a webhook: these cases assert on metrics and ctx.alerts()
    return new WatchEngine(WatchRules.fixed(List.of(rules), metrics), new RecordingSink(),
        metrics, realert, "iceberg");
  }

  /** A resolver that has already made up its mind, standing in for the catalog read. */
  private static WatchEngine.Deletes deletes(boolean bearing) {
    return (prefix, namespace, tableName, snapshotId) -> bearing;
  }

  private static WatchEngine engine(Metrics metrics, WatchEngine.Deletes deletes,
      WatchRule... rules) {
    return new WatchEngine(WatchRules.fixed(List.of(rules), metrics), new RecordingSink(),
        metrics, false, "iceberg", deletes);
  }

  /** The confidence the alert actually carries out of the build, read off the payload. */
  private static String tokenConfidence(IndexBuildListener.BuildContext ctx) {
    java.util.List<?> evidence =
        (java.util.List<?>) ctx.alerts().get(0).get("evidence");
    return (String) ((java.util.Map<?, ?>) evidence.get(0)).get("confidence");
  }

  private static IndexBuildListener.BuildContext fire(WatchEngine engine, List<String> tokens) {
    IndexBuildListener.BuildContext ctx = engine.start(
        "lakehouse", "logs", "events", "msg", 7, IndexBuildListener.BuildKind.INCREMENTAL,
        Set.of(), Grams.Contract.current(3));
    ctx.file("s3://b/f1.parquet", terms(tokens), Set.of());
    ctx.done();
    return ctx;
  }

  /**
   * {@code IndexBuilder.readFile} applies no delete file, so on a merge-on-read snapshot a term
   * count includes rows the engine no longer returns. The confirmation SQL DOES apply them and
   * comes back short — and a short confirmation reads as a false positive, not a caveat — so the
   * count must say advisory.
   */
  @Test
  void tokenEvidenceOverADeleteBearingSnapshotIsAdvisory() {
    Metrics metrics = new Metrics();
    WatchEngine engine = engine(metrics, deletes(true),
        rule("burst", List.of("error"), List.of(), WatchRule.Condition.ANY_OF, 1));
    IndexBuildListener.BuildContext ctx = fire(engine, List.of("error", "error", "ok"));

    assertEquals(1, metrics.watchAlerts.sum(),
        "labelled, not suppressed: a detection that goes silent on a merge-on-read table is "
            + "worse than one that over-counts and says so");
    assertEquals("advisory", tokenConfidence(ctx));
  }

  /**
   * A snapshot that PROVES it carries no delete files still earns {@code exact}: there the count
   * really is every occurrence the engine would return.
   */
  @Test
  void tokenEvidenceOverAProvablyDeleteFreeSnapshotStaysExact() {
    Metrics metrics = new Metrics();
    WatchEngine engine = engine(metrics, deletes(false),
        rule("burst", List.of("error"), List.of(), WatchRule.Condition.ANY_OF, 1));
    assertEquals("exact", tokenConfidence(fire(engine, List.of("error", "ok"))));
  }

  /**
   * Everything short of an answer is delete-bearing. The first case is the one that ships today:
   * {@code IndexBuildListener.start} hands a listener a snapshot ID and nothing else, and
   * PortBoundaryTest forbids this class the table format it would take to resolve one, so an
   * engine nobody told may not claim exactness on the strength of a lookup that never happened.
   */
  @Test
  void aSnapshotNobodyProvedDeleteFreeIsTreatedAsDeleteBearing() {
    WatchRule burst = rule("burst", List.of("error"), List.of(), WatchRule.Condition.ANY_OF, 1);

    assertTrue(WatchEngine.Deletes.UNPROVEN.bearing("lakehouse", "logs", "events", 7),
        "the default must fail closed, or every unwired engine claims exactness");
    assertEquals("advisory",
        tokenConfidence(fire(engine(new Metrics(), (WatchEngine.Deletes) null, burst),
            List.of("error"))),
        "no resolver wired, so nothing is proven");
    assertEquals("advisory",
        tokenConfidence(fire(engine(new Metrics(), (prefix, namespace, tableName, snapshotId) -> {
          throw new IllegalStateException("catalog down");
        }, burst), List.of("error"))),
        "a resolver that throws must not be read as a delete-free answer");
  }

  /**
   * {@code min_count} is a threshold on the same upper-bound total, so on a delete-bearing
   * snapshot it can be crossed by occurrences a delete file already removed. It still fires — the
   * whole point of labelling rather than refusing — and the alert it raises says advisory, which
   * is the only thing that tells the analyst why the confirmation comes back short of three.
   */
  @Test
  void aMinCountThresholdCrossedOnUpperBoundCountsFiresButSaysAdvisory() {
    Metrics metrics = new Metrics();
    WatchEngine engine = engine(metrics, deletes(true),
        rule("burst", List.of("error"), List.of(), WatchRule.Condition.ANY_OF, 3));
    IndexBuildListener.BuildContext ctx = fire(engine, List.of("error", "error", "error"));

    assertEquals(1, metrics.watchAlerts.sum());
    assertEquals(3L, ((java.util.Map<?, ?>) ((java.util.List<?>) ctx.alerts().get(0)
        .get("evidence")).get(0)).get("count"),
        "the count is still reported in full — the label is what carries the caveat");
    assertEquals("advisory", tokenConfidence(ctx));
  }

  private static TermIndexWriter.FileTerms terms(List<String> tokens) {
    TermIndexWriter.FileTerms terms = new TermIndexWriter.FileTerms();
    terms.row(0, tokens);
    return terms;
  }

  @Test
  void anyOfTokenFiresWithExactCount() throws Exception {
    // real webhook target so the payload's evidence can be asserted
    LinkedBlockingQueue<byte[]> received = new LinkedBlockingQueue<>();
    HttpServer server = HttpServer.create(new InetSocketAddress(0), 0);
    server.createContext("/", exchange -> {
      received.add(exchange.getRequestBody().readAllBytes());
      exchange.sendResponseHeaders(200, -1);
      exchange.close();
    });
    server.start();
    try {
      Metrics metrics = new Metrics();
      AlertSink webhook = new WebhookSink(
          URI.create("http://127.0.0.1:" + server.getAddress().getPort() + "/hook"),
          "", 2_000, new long[] {10, 10}, metrics);
      WatchEngine engine = new WatchEngine(
          WatchRules.fixed(
              List.of(rule("burst", List.of("error"), List.of(), WatchRule.Condition.ANY_OF, 1)),
              metrics),
          // over a snapshot proven delete-free, which is what entitles the evidence below to say
          // exact rather than advisory
          webhook, metrics, false, "iceberg", deletes(false));
      IndexBuildListener.BuildContext ctx = engine.start(
          "lakehouse", "logs", "events", "msg", 42, IndexBuildListener.BuildKind.INCREMENTAL,
          Set.of(), Grams.Contract.current(3));
      ctx.file("s3://b/f1.parquet", terms(List.of("error", "error", "error", "ok")), Set.of());
      ctx.done();
      assertEquals(1, metrics.watchAlerts.sum());
      assertEquals(1, ctx.alerts().size(), "the alert is kept for the build report as well as sent");
      assertEquals("burst", ((java.util.Map<?, ?>) ctx.alerts().get(0).get("rule")).get("id"));

      byte[] payload = received.poll(10, TimeUnit.SECONDS);
      JsonNode alert = new ObjectMapper().readTree(payload);
      assertEquals("burst", alert.path("rule").path("id").asText());
      assertEquals("high", alert.path("rule").path("severity").asText());
      assertEquals(42, alert.path("table").path("snapshot_id").asLong());
      assertEquals("s3://b/f1.parquet", alert.path("file").path("path").asText());
      JsonNode evidence = alert.path("evidence").path(0);
      assertEquals("token", evidence.path("kind").asText());
      assertEquals(3, evidence.path("count").asLong());
      assertEquals("exact", evidence.path("confidence").asText());
      assertEquals("incremental", alert.path("build_kind").asText());
      assertTrue(alert.path("confirmation_sql").asText()
          .contains("position('error' IN lower(\"msg\")) > 0"));
      assertTrue(alert.path("confirmation_sql").asText().contains("FOR VERSION AS OF 42"));
    } finally {
      server.stop(0);
    }
  }

  @Test
  void minCountRespected() {
    Metrics metrics = new Metrics();
    WatchEngine engine = engine(metrics, false,
        rule("burst", List.of("error"), List.of(), WatchRule.Condition.ANY_OF, 3));
    IndexBuildListener.BuildContext ctx = engine.start(
        "lakehouse", "logs", "events", "msg", 1, IndexBuildListener.BuildKind.FULL, Set.of(), Grams.Contract.current(3));
    ctx.file("f1", terms(List.of("error", "error")), Set.of());
    assertEquals(0, metrics.watchAlerts.sum());
    ctx.file("f2", terms(List.of("error", "error", "error")), Set.of());
    assertEquals(1, metrics.watchAlerts.sum());
  }

  /**
   * An all-of pair must not ride the index: a file with an error row and a timeout row is not a
   * row with both, and the row scan -- which has the row -- would answer the same rule
   * differently. The scan owns it.
   */
  @Test
  void allOfOverTwoFieldsDoesNotRideTheIndex() {
    Metrics metrics = new Metrics();
    WatchRule pair = rule("pair", List.of("error", "timeout"), List.of(), WatchRule.Condition.ALL_OF, 1);
    assertFalse(pair.ridesIndex(), "a file with each token somewhere is not a row with both");
    WatchEngine engine = engine(metrics, false, pair);
    IndexBuildListener.BuildContext ctx = engine.start(
        "lakehouse", "logs", "events", "msg", 1, IndexBuildListener.BuildKind.FULL, Set.of(), Grams.Contract.current(3));
    ctx.file("f1", terms(List.of("error")), Set.of());
    ctx.file("f2", terms(List.of("timeout")), Set.of());
    ctx.file("f3", terms(List.of("error", "timeout")), Set.of());
    assertEquals(0, metrics.watchAlerts.sum(),
        "the index path must stay silent even on the file holding both, or it and the scan disagree");
  }

  /**
   * One field with two values under all-of: a field's values are OR'ed by the record's own
   * contract, so a file holding either satisfies the field -- and the rule, there being only the
   * one field. Demanding both would be a silent under-match, and with the scan off nothing else
   * would catch it.
   */
  @Test
  void oneFieldWithTwoValuesUnderAllOfFiresOnEitherLikeTheRowScanDoes() {
    Metrics metrics = new Metrics();
    WatchRule oneField = new WatchRule("either", "t", WatchRule.Severity.LOW, "lakehouse",
        "logs.events",
        List.of(new WatchRule.Field("msg", WatchRule.Op.MATCH, List.of("error", "timeout"))),
        WatchRule.Condition.ALL_OF, 1);
    assertTrue(oneField.ridesIndex(), "one field is the index's, whatever the condition says");
    WatchEngine engine = engine(metrics, false, oneField);
    IndexBuildListener.BuildContext ctx = engine.start(
        "lakehouse", "logs", "events", "msg", 1, IndexBuildListener.BuildKind.FULL, Set.of(), Grams.Contract.current(3));
    ctx.file("f1", terms(List.of("error")), Set.of());
    assertEquals(1, metrics.watchAlerts.sum(),
        "a file holding ONE of the field's values satisfies the field, as it does on the scan");
  }

  @Test
  void containsGramProbeFiresAdvisory() {
    Metrics metrics = new Metrics();
    WatchEngine engine = engine(metrics, false,
        rule("sub", List.of(), List.of("disk pressure"), WatchRule.Condition.ANY_OF, 1));
    IndexBuildListener.BuildContext ctx = engine.start(
        "lakehouse", "logs", "events", "msg", 1, IndexBuildListener.BuildKind.FULL, Set.of(), Grams.Contract.current(3));
    Set<String> without = NgramBloom.gramsOf("all healthy here", Grams.Contract.current(3));
    ctx.file("f1", terms(List.of()), without);
    assertEquals(0, metrics.watchAlerts.sum());
    Set<String> with = NgramBloom.gramsOf("node under disk pressure now", Grams.Contract.current(3));
    ctx.file("f2", terms(List.of()), with);
    assertEquals(1, metrics.watchAlerts.sum());
  }

  @Test
  void fullRebuildSuppressionOnPriorCoveredFile() {
    Metrics metrics = new Metrics();
    WatchEngine engine = engine(metrics, false,
        rule("burst", List.of("error"), List.of(), WatchRule.Condition.ANY_OF, 1));
    IndexBuildListener.BuildContext ctx = engine.start(
        "lakehouse", "logs", "events", "msg", 1, IndexBuildListener.BuildKind.FULL,
        Set.of("f-covered"), Grams.Contract.current(3));
    ctx.file("f-covered", terms(List.of("error")), Set.of());
    assertEquals(0, metrics.watchAlerts.sum());
    assertEquals(1, metrics.watchSuppressed.sum());
    ctx.file("f-new", terms(List.of("error")), Set.of());
    assertEquals(1, metrics.watchAlerts.sum());
  }

  @Test
  void realertOverrideDisablesSuppression() {
    Metrics metrics = new Metrics();
    WatchEngine engine = engine(metrics, true,
        rule("burst", List.of("error"), List.of(), WatchRule.Condition.ANY_OF, 1));
    IndexBuildListener.BuildContext ctx = engine.start(
        "lakehouse", "logs", "events", "msg", 1, IndexBuildListener.BuildKind.FULL,
        Set.of("f-covered"), Grams.Contract.current(3));
    ctx.file("f-covered", terms(List.of("error")), Set.of());
    assertEquals(1, metrics.watchAlerts.sum());
    assertEquals(0, metrics.watchSuppressed.sum());
  }

  @Test
  void dedupSuppressesSecondIdenticalRuleAndFile() {
    Metrics metrics = new Metrics();
    WatchEngine engine = engine(metrics, false,
        rule("burst", List.of("error"), List.of(), WatchRule.Condition.ANY_OF, 1));
    IndexBuildListener.BuildContext ctx = engine.start(
        "lakehouse", "logs", "events", "msg", 1, IndexBuildListener.BuildKind.INCREMENTAL,
        Set.of(), Grams.Contract.current(3));
    ctx.file("f1", terms(List.of("error")), Set.of());
    ctx.file("f1", terms(List.of("error")), Set.of());
    assertEquals(1, metrics.watchAlerts.sum());
  }

  @Test
  void poisonedRuleNeverPropagates() {
    Metrics metrics = new Metrics();
    WatchEngine engine = engine(metrics, false,
        rule("poison", List.of("error"), List.of(), WatchRule.Condition.ANY_OF, 1));
    IndexBuildListener.BuildContext ctx = engine.start(
        "lakehouse", "logs", "events", "msg", 1, IndexBuildListener.BuildKind.FULL, Set.of(), Grams.Contract.current(3));
    // null terms NPEs inside rule evaluation; the engine contains it per rule
    assertDoesNotThrow(() -> ctx.file("f1", null, Set.of()));
    // and the safe() wrapper contains anything a listener itself throws
    IndexBuildListener throwing = (p, n, t, c, s, k, prior, g) -> {
      throw new IllegalStateException("boom");
    };
    assertDoesNotThrow(() -> IndexBuildListener.safe(throwing)
        .start("p", "n", "t", "c", 1, IndexBuildListener.BuildKind.FULL, Set.of(), Grams.Contract.current(3))
        .file("f", null, Set.of()));
  }

  @Test
  void unmatchedTableIsNoop() {
    Metrics metrics = new Metrics();
    WatchEngine engine = engine(metrics, false,
        rule("burst", List.of("error"), List.of(), WatchRule.Condition.ANY_OF, 1));
    IndexBuildListener.BuildContext ctx = engine.start(
        "lakehouse", "other", "events", "msg", 1, IndexBuildListener.BuildKind.FULL, Set.of(), Grams.Contract.current(3));
    ctx.file("f1", terms(List.of("error")), Set.of());
    assertEquals(0, metrics.watchAlerts.sum());
  }
}
