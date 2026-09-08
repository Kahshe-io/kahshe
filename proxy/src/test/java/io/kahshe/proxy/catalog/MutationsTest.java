package io.kahshe.proxy.catalog;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.junit.jupiter.api.Assertions.assertTrue;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import java.nio.charset.StandardCharsets;
import org.junit.jupiter.api.Test;
import io.kahshe.proxy.http.KahsheHandler;
import io.kahshe.proxy.plan.PlanService;

class MutationsTest {
  private static final ObjectMapper MAPPER = new ObjectMapper();

  @Test
  void mergesPlanEndpointsIntoExistingList() throws Exception {
    byte[] in = "{\"defaults\":{},\"overrides\":{},\"endpoints\":[\"GET /v1/{prefix}/namespaces\"]}"
        .getBytes(StandardCharsets.UTF_8);
    JsonNode out = MAPPER.readTree(Mutations.mergeConfigEndpoints(in));
    var endpoints = out.get("endpoints");
    assertEquals(5, endpoints.size());
    boolean hasPlan = false;
    for (JsonNode e : endpoints) {
      hasPlan |= e.asText().equals("POST /v1/{prefix}/namespaces/{namespace}/tables/{table}/plan");
    }
    assertTrue(hasPlan);
  }

  @Test
  void mergeIsIdempotent() throws Exception {
    byte[] once = Mutations.mergeConfigEndpoints("{\"endpoints\":[]}".getBytes(StandardCharsets.UTF_8));
    byte[] twice = Mutations.mergeConfigEndpoints(once);
    assertEquals(
        MAPPER.readTree(once).get("endpoints").size(),
        MAPPER.readTree(twice).get("endpoints").size());
  }

  @Test
  void synthesizesBaselineWhenBackendAdvertisesNone() throws Exception {
    JsonNode out = MAPPER.readTree(Mutations.mergeConfigEndpoints("{}".getBytes(StandardCharsets.UTF_8)));
    assertTrue(out.get("endpoints").size() > 15, "baseline + plan endpoints expected");
  }

  @Test
  void stripsBackendAdvertisedUri() throws Exception {
    String body = "{\"defaults\":{\"uri\":\"http://backend:19120/iceberg/\"},"
        + "\"overrides\":{\"uri\":\"http://backend:19120/iceberg/\",\"prefix\":\"main\"},\"endpoints\":[]}";
    JsonNode out = MAPPER.readTree(Mutations.mergeConfigEndpoints(body.getBytes(StandardCharsets.UTF_8)));
    assertTrue(out.path("overrides").path("uri").isMissingNode(), "overrides.uri must be stripped");
    assertTrue(out.path("defaults").path("uri").isMissingNode(), "defaults.uri must be stripped");
    assertEquals("main", out.path("overrides").path("prefix").asText());
  }

  @Test
  void injectsServerPlanningMode() throws Exception {
    Mutations.ServerPlanning out =
        Mutations.injectServerPlanning(
            "{\"metadata-location\":\"x\",\"metadata\":{}}".getBytes(StandardCharsets.UTF_8));
    assertEquals("server", MAPPER.readTree(out.body()).get("config").get("scan-planning-mode").asText());
    assertTrue(out.injected(), "it injected, so it must say so");
  }

  @Test
  void preservesExistingConfigKeys() throws Exception {
    Mutations.ServerPlanning out =
        Mutations.injectServerPlanning(
            "{\"config\":{\"client.region\":\"us-east-1\"}}".getBytes(StandardCharsets.UTF_8));
    JsonNode config = MAPPER.readTree(out.body()).get("config");
    assertEquals("us-east-1", config.get("client.region").asText());
    assertEquals("server", config.get("scan-planning-mode").asText());
  }

  @Test
  void malformedBodyPassesThroughUnmodified() {
    byte[] junk = "not json".getBytes(StandardCharsets.UTF_8);
    assertEquals(junk, Mutations.mergeConfigEndpoints(junk));
    Mutations.ServerPlanning planning = Mutations.injectServerPlanning(junk);
    assertEquals(junk, planning.body());
    assertFalse(planning.injected(), "a body it could not parse was not injected into");
  }

  @Test
  void injectionSkippedForDeleteBearingTables() throws Exception {
    String body = "{\"metadata\":{\"current-snapshot-id\":7,\"snapshots\":[{\"snapshot-id\":7,"
        + "\"summary\":{\"total-delete-files\":\"2\"}}]}}";
    Mutations.ServerPlanning out = Mutations.injectServerPlanning(body.getBytes(StandardCharsets.UTF_8));
    assertTrue(MAPPER.readTree(out.body()).path("config").path("scan-planning-mode").isMissingNode());
    // KahsheHandler logs "injected scan-planning-mode=server" off this flag; true here would have
    // the log claim server planning was advertised for a merge-on-read table when it was not.
    // Verified by breaking it: returning true here fails this line.
    assertFalse(
        out.injected(),
        "a delete-bearing table must report that it was NOT advertised, or the log lies about it");
  }

  /**
   * The opt-in has to move both halves or it moves nothing.
   *
   * <p>{@code PlanService} gates the SERVING of a delete-bearing snapshot on
   * {@code KAHSHE_SERVE_DELETE_BEARING}. Gating only that half leaves the flag inert in practice:
   * a client that was never told {@code scan-planning-mode=server} never asks for a plan, so the
   * lifted refusal is never reached and the engine simply carries on planning locally.
   *
   * <p>Verified by breaking it: ignoring the parameter here leaves this body without a
   * {@code scan-planning-mode}.
   */
  @Test
  void theOptInAdvertisesDeleteBearingTablesToo() throws Exception {
    String body = "{\"metadata\":{\"current-snapshot-id\":7,\"snapshots\":[{\"snapshot-id\":7,"
        + "\"summary\":{\"total-delete-files\":\"2\"}}]}}";
    Mutations.ServerPlanning out =
        Mutations.injectServerPlanning(body.getBytes(StandardCharsets.UTF_8), true);
    assertTrue(out.injected(), "the opt-in must advertise, or the serving half is unreachable");
    assertEquals("server",
        MAPPER.readTree(out.body()).path("config").path("scan-planning-mode").asText());
  }

  private static final String INDEXED =
      "{\"metadata\":{\"current-snapshot-id\":7,\"properties\":{\"kahshe.index\":\"msg\"},"
          + "\"snapshots\":[{\"snapshot-id\":7,\"summary\":{\"total-delete-files\":\"0\"}}]}}";
  private static final String UNINDEXED =
      "{\"metadata\":{\"current-snapshot-id\":7,"
          + "\"snapshots\":[{\"snapshot-id\":7,\"summary\":{\"total-delete-files\":\"0\"}}]}}";

  private static Mutations.ServerPlanning advertise(String body, String mode) {
    return Mutations.injectServerPlanning(body.getBytes(StandardCharsets.UTF_8), false, mode);
  }

  /**
   * {@code indexed} narrows the MUST to the tables kahshe actually accelerates.
   *
   * <p>An unindexed table planned server-side reads exactly the files it would have read locally,
   * so the only thing given up by skipping it is the manifest fetch. What is bought back is every
   * client behaviour that a table saying "you MUST plan server-side" forecloses.
   *
   * <p>Verified by breaking it: ignoring the mode advertises to both bodies and fails the second
   * assertion.
   */
  @Test
  void indexedModeAdvertisesOnlyToTablesDeclaringAnIndex() throws Exception {
    Mutations.ServerPlanning indexed = advertise(INDEXED, Mutations.ADVERTISE_INDEXED);
    assertTrue(indexed.injected(), "a table declaring kahshe.index is what the mode is for");
    assertEquals("server",
        MAPPER.readTree(indexed.body()).path("config").path("scan-planning-mode").asText());

    Mutations.ServerPlanning plain = advertise(UNINDEXED, Mutations.ADVERTISE_INDEXED);
    assertFalse(plain.injected(), "no kahshe.index means nothing to accelerate, so no MUST");
    assertTrue(MAPPER.readTree(plain.body()).path("config").path("scan-planning-mode").isMissingNode());
  }

  @Test
  void allModeIsTheDefaultAndAdvertisesToBoth() throws Exception {
    assertTrue(advertise(INDEXED, Mutations.ADVERTISE_ALL).injected());
    assertTrue(advertise(UNINDEXED, Mutations.ADVERTISE_ALL).injected());
    // The two-argument overload must keep meaning what it meant before the mode existed.
    assertTrue(
        Mutations.injectServerPlanning(UNINDEXED.getBytes(StandardCharsets.UTF_8), false).injected());
  }

  @Test
  void noneModeAdvertisesToNothingAndDoesNotNeedToParseTheBody() {
    assertFalse(advertise(INDEXED, Mutations.ADVERTISE_NONE).injected());
    Mutations.ServerPlanning junk =
        Mutations.injectServerPlanning(
            "not json".getBytes(StandardCharsets.UTF_8), false, Mutations.ADVERTISE_NONE);
    assertFalse(junk.injected());
    assertEquals("KAHSHE_ADVERTISE_SERVER_MODE=none", junk.declinedBecause());
  }

  /** The two gates compose: narrowing never widens what the delete-bearing guard refuses. */
  @Test
  void indexedModeStillRefusesADeleteBearingIndexedTable() {
    String deleteBearing =
        "{\"metadata\":{\"current-snapshot-id\":7,\"properties\":{\"kahshe.index\":\"msg\"},"
            + "\"snapshots\":[{\"snapshot-id\":7,\"summary\":{\"total-delete-files\":\"2\"}}]}}";
    Mutations.ServerPlanning out = advertise(deleteBearing, Mutations.ADVERTISE_INDEXED);
    assertFalse(out.injected());
    assertEquals("the snapshot is not provably delete-free", out.declinedBecause());
  }

  /**
   * KahsheHandler logs {@code declinedBecause} verbatim, so a wrong reason misdirects whoever is
   * asking why a table is not being accelerated. Each refusal must name its own cause.
   */
  @Test
  void eachRefusalNamesItsOwnCause() {
    assertEquals(
        "KAHSHE_ADVERTISE_SERVER_MODE=indexed and this table declares no kahshe.index",
        advertise(UNINDEXED, Mutations.ADVERTISE_INDEXED).declinedBecause());
    assertNull(advertise(INDEXED, Mutations.ADVERTISE_INDEXED).declinedBecause(),
        "a table that was advertised to has no refusal to explain");
    assertEquals(
        "the LoadTableResponse could not be rewritten",
        Mutations.injectServerPlanning("not json".getBytes(StandardCharsets.UTF_8)).declinedBecause());
  }

  @Test
  void injectionRequiresProvablyZeroDeletes() throws Exception {
    String noSummary = "{\"metadata\":{\"current-snapshot-id\":7,\"snapshots\":[{\"snapshot-id\":7}]}}";
    Mutations.ServerPlanning unproven =
        Mutations.injectServerPlanning(noSummary.getBytes(StandardCharsets.UTF_8));
    assertTrue(MAPPER.readTree(unproven.body()).path("config").path("scan-planning-mode").isMissingNode());
    assertFalse(unproven.injected());

    String clean = "{\"metadata\":{\"current-snapshot-id\":7,\"snapshots\":[{\"snapshot-id\":7,"
        + "\"summary\":{\"total-delete-files\":\"0\"}}]}}";
    Mutations.ServerPlanning proven =
        Mutations.injectServerPlanning(clean.getBytes(StandardCharsets.UTF_8));
    assertEquals("server",
        MAPPER.readTree(proven.body()).path("config").path("scan-planning-mode").asText());
    assertTrue(proven.injected());
  }
}
