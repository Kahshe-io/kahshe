package io.kahshe.proxy.catalog;

import io.kahshe.common.Metrics;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.junit.jupiter.api.Assertions.assertTrue;

import java.nio.charset.StandardCharsets;
import java.util.List;
import org.apache.iceberg.catalog.Namespace;
import org.apache.iceberg.catalog.TableIdentifier;
import org.apache.iceberg.catalog.TableIdentifierParser;
import org.apache.iceberg.rest.requests.CommitTransactionRequest;
import org.apache.iceberg.rest.requests.CommitTransactionRequestParser;
import org.apache.iceberg.rest.requests.UpdateTableRequest;
import org.junit.jupiter.api.Test;

/**
 * Which tables a mutating request changed, for the two endpoints that name them in the body.
 *
 * <p>Every request here is serialized by ICEBERG'S OWN parser from identifiers built in the test,
 * not hand-written JSON. That is the point rather than convenience: the way body-reading goes
 * wrong is the wire format moving underneath a hand-matched field name. An oracle in both
 * directions cannot drift, which a bare assertion could.
 */
class MutatedTablesTest {
  private final Metrics metrics = new Metrics();

  private static byte[] transactionBody(TableIdentifier... tables) {
    List<UpdateTableRequest> changes =
        java.util.Arrays.stream(tables)
            .map(ident -> UpdateTableRequest.create(ident, List.of(), List.of()))
            .toList();
    return CommitTransactionRequestParser.toJson(new CommitTransactionRequest(changes))
        .getBytes(StandardCharsets.UTF_8);
  }

  private static byte[] renameBody(TableIdentifier source, TableIdentifier destination) {
    return ("{\"source\":"
            + TableIdentifierParser.toJson(source)
            + ",\"destination\":"
            + TableIdentifierParser.toJson(destination)
            + "}")
        .getBytes(StandardCharsets.UTF_8);
  }

  /**
   * The trap: a multi-table commit whose body goes unread leaves every table it changed cached.
   *
   * <p>One of these carries a multi-level namespace, because that is the case a path-shaped
   * assumption gets wrong — a namespace is a list, not a segment.
   *
   * <p>Verified by breaking it: with the {@code transactions/commit} branch removed from
   * {@code MutatedTables.of}, this returns null and fails on the first assertion.
   */
  @Test
  void aTransactionNamesEveryTableItChanges() {
    TableIdentifier a = TableIdentifier.of(Namespace.of("logs"), "events");
    TableIdentifier b = TableIdentifier.of(Namespace.of("logs", "raw"), "spans");
    TableIdentifier c = TableIdentifier.of(Namespace.of("ops"), "audit");

    MutatedTables.Mutation mutation =
        MutatedTables.of("POST", "/v1/prod/transactions/commit", transactionBody(a, b, c), metrics);

    assertNotNull(mutation, "a transaction commit changes tables and must say which");
    assertEquals("prod", mutation.prefixRaw());
    assertEquals(List.of(a, b, c), mutation.tables());
    assertEquals(0, metrics.invalidationParseFailures.sum());
  }

  /**
   * A rename invalidates BOTH ends, and the destination is the one that matters most.
   *
   * <p>Stale source is ordinary staleness: the name stops resolving. A stale destination is worse
   * — drop {@code archive}, rename {@code live} onto that freed name, and a cache still holding
   * the dropped {@code archive} serves another table's files for the rest of the TTL.
   */
  @Test
  void aRenameNamesBothItsSourceAndItsDestination() {
    TableIdentifier source = TableIdentifier.of(Namespace.of("logs"), "live");
    TableIdentifier destination = TableIdentifier.of(Namespace.of("logs"), "archive");

    MutatedTables.Mutation mutation =
        MutatedTables.of(
            "POST", "/v1/prod/tables/rename", renameBody(source, destination), metrics);

    assertNotNull(mutation);
    assertEquals(List.of(source, destination), mutation.tables());
  }

  /**
   * A body that cannot be read must be counted, not passed over.
   *
   * <p>Returning an empty list here is no worse than the 10 s TTL exposure of not reading the body
   * at all — but it is invisible, and "the exact invalidation quietly stopped" and "nobody is
   * committing" produce identical observations.
   *
   * <p>Verified by breaking it: dropping the {@code metrics.invalidationParseFailures.increment()}
   * from {@code unreadable} leaves the count at 0 and this fails.
   */
  @Test
  void aBodyThatCannotBeReadIsCountedRatherThanPassedOverInSilence() {
    byte[] garbage = "{\"table-changes\": \"not a list\"}".getBytes(StandardCharsets.UTF_8);

    MutatedTables.Mutation mutation =
        MutatedTables.of("POST", "/v1/prod/transactions/commit", garbage, metrics);

    assertNotNull(mutation, "the request still changed tables; only their names were lost");
    assertTrue(mutation.tables().isEmpty());
    assertEquals(1, metrics.invalidationParseFailures.sum());

    MutatedTables.of("POST", "/v1/prod/tables/rename", garbage, metrics);
    assertEquals(2, metrics.invalidationParseFailures.sum(), "the rename half counts too");
  }

  /**
   * Requests whose tables come from the URL, and reads, are not this class's business.
   *
   * <p>Returning a mutation for the ordinary table path would double-invalidate and, worse, hide
   * that the path-driven branch had stopped working.
   */
  @Test
  void requestsThatDoNotNameTablesInTheirBodyAreNotClaimed() {
    assertNull(
        MutatedTables.of("POST", "/v1/prod/namespaces/logs/tables/events", new byte[0], metrics));
    assertNull(MutatedTables.of("GET", "/v1/prod/transactions/commit", new byte[0], metrics));
    assertNull(MutatedTables.of("POST", "/v1/config", new byte[0], metrics));
    assertEquals(0, metrics.invalidationParseFailures.sum(), "nothing was parsed, so nothing failed");
  }
}
