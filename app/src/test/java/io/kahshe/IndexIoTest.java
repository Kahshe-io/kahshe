package io.kahshe;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertInstanceOf;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

import io.kahshe.format.FormatConfig;
import java.util.Map;
import org.apache.iceberg.io.FileIO;
import org.junit.jupiter.api.Test;

/**
 * The index store's FileIO is resolved the way Iceberg resolves one — an implementation class
 * plus dotted properties — and the {@code KAHSHE_INDEX_S3_*} variables survive as a shorthand for
 * four of those keys. Two rules are load-bearing and each is verified red: static keys are passed
 * only when given, so an unconfigured pair means the AWS default credential provider chain (IRSA,
 * workload identity, instance profile) rather than a blank — and invalid — credential; and an
 * explicit {@code KAHSHE_INDEX_IO_PROPERTIES} entry beats the shorthand for the same key, which
 * fails if the merge puts the shorthand second.
 */
class IndexIoTest {

  /** Every field but the four IO ones held at a value no assertion here depends on. */
  private static FormatConfig config(
      String root, String endpoint, String key, String secret, String region,
      String impl, Map<String, String> properties) {
    return new FormatConfig(
        root, endpoint, key, secret, region, impl, properties,
        "table", 64L * 1024 * 1024, true, true, 100_000);
  }

  @Test
  void unsetKeysMeanTheDefaultProviderChainNotBlankCredentials() {
    Map<String, String> props = IndexIo.s3Properties("", "", "", "us-east-1");
    assertFalse(props.containsKey("s3.access-key-id"), "no key means no key, not an empty one");
    assertFalse(props.containsKey("s3.secret-access-key"));
    assertFalse(props.containsKey("s3.endpoint"), "plain AWS needs no endpoint");
    assertEquals("us-east-1", props.get("client.region"));
  }

  @Test
  void staticKeysAndAnEndpointAreHonouredWhenGiven() {
    Map<String, String> props =
        IndexIo.s3Properties("http://minio:9000", "AKIA", "s3cr3t", "eu-west-1");
    assertEquals("AKIA", props.get("s3.access-key-id"));
    assertEquals("s3cr3t", props.get("s3.secret-access-key"));
    assertEquals("http://minio:9000", props.get("s3.endpoint"));
    assertTrue(Boolean.parseBoolean(props.get("s3.path-style-access")));
  }

  @Test
  void theConvenienceVariablesStillReachTheMergedProperties() {
    Map<String, String> props = IndexIo.properties(
        config("s3://b/i", "http://minio:9000", "AKIA", "s3cr3t", "eu-west-1", IndexIo.S3_FILE_IO,
            Map.of()));
    assertEquals("http://minio:9000", props.get("s3.endpoint"));
    assertEquals("AKIA", props.get("s3.access-key-id"));
    assertEquals("s3cr3t", props.get("s3.secret-access-key"));
    assertEquals("eu-west-1", props.get("client.region"));
  }

  @Test
  void theListIsSplitOnCommasAndTrimmedAroundEitherSideOfTheEquals() {
    Map<String, String> props = IndexIo.parseProperties(
        " s3.endpoint = http://minio:9000 , s3.path-style-access=true ,, gcs.project-id=p1,");
    assertEquals(
        Map.of(
            "s3.endpoint", "http://minio:9000",
            "s3.path-style-access", "true",
            "gcs.project-id", "p1"),
        props);
  }

  @Test
  void aValueMayCarryItsOwnEqualsAndAnEntryWithoutOneIsRefused() {
    assertEquals(
        "https://sts.example/?a=1&b=2",
        IndexIo.parseProperties("client.assume-role-arn=https://sts.example/?a=1&b=2")
            .get("client.assume-role-arn"));
    assertThrows(IllegalArgumentException.class, () -> IndexIo.parseProperties("s3.endpoint"));
    assertThrows(IllegalArgumentException.class, () -> IndexIo.parseProperties("=nokey"));
    assertTrue(IndexIo.parseProperties("").isEmpty());
    assertTrue(IndexIo.parseProperties(null).isEmpty());
  }

  /**
   * The precedence rule, and the one this file was written red for: the shorthand is a default,
   * so a key spelled out by hand must survive the merge. Reversing the two putAll calls in
   * {@link IndexIo#properties} fails every assertion below.
   */
  @Test
  void anExplicitPropertyBeatsTheConvenienceShorthandForTheSameKey() {
    Map<String, String> props = IndexIo.properties(
        config("s3://b/i", "http://minio:9000", "AKIA", "s3cr3t", "eu-west-1", IndexIo.S3_FILE_IO,
            Map.of(
                "s3.endpoint", "https://ceph.internal:8443",
                "s3.access-key-id", "EXPLICIT",
                "client.region", "us-east-2",
                "s3.path-style-access", "false")));
    assertEquals("https://ceph.internal:8443", props.get("s3.endpoint"));
    assertEquals("EXPLICIT", props.get("s3.access-key-id"));
    assertEquals("us-east-2", props.get("client.region"));
    assertEquals("false", props.get("s3.path-style-access"));
    // and a shorthand key the explicit list did not name is still there
    assertEquals("s3cr3t", props.get("s3.secret-access-key"));
  }

  @Test
  void anS3RootDefaultsToIcebergsOwnS3FileIoAndNothingElseDefaultsAtAll() {
    assertEquals(IndexIo.S3_FILE_IO, IndexIo.defaultImpl("s3://bucket/indexes"));
    assertEquals(IndexIo.S3_FILE_IO, IndexIo.defaultImpl("s3a://bucket/indexes"));
    assertEquals("", IndexIo.defaultImpl("gs://bucket/indexes"));
    assertEquals("", IndexIo.defaultImpl("file:/var/lib/kahshe"));
    assertEquals("", IndexIo.defaultImpl(""));
    assertEquals("", IndexIo.defaultImpl(null));
  }

  /**
   * The class named is loaded and initialized with the merged properties. ResolvingFileIO is a
   * real FileIO in iceberg-core that touches no network on construction, which is the point: the
   * app does not know the implementation, it only passes the name along.
   */
  @Test
  void theNamedImplementationIsLoadedAndInitialisedWithTheMergedProperties() {
    FileIO io = IndexIo.open(
        config("s3://bucket/indexes", "http://minio:9000", "", "", "eu-west-1",
            "org.apache.iceberg.io.ResolvingFileIO", Map.of("gcs.project-id", "p1")));
    assertInstanceOf(org.apache.iceberg.io.ResolvingFileIO.class, io);
    assertEquals("http://minio:9000", io.properties().get("s3.endpoint"));
    assertEquals("eu-west-1", io.properties().get("client.region"));
    assertEquals("p1", io.properties().get("gcs.project-id"));
  }

  @Test
  void anEmptyImplementationIsRefusedRatherThanGuessedAt() {
    IllegalStateException e = assertThrows(
        IllegalStateException.class,
        () -> IndexIo.open(config("gs://bucket/i", "", "", "", "us-east-1", "", Map.of())));
    assertTrue(e.getMessage().contains("KAHSHE_INDEX_IO_IMPL"), e.getMessage());
  }
}
