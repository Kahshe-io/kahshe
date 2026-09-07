package io.kahshe;

import io.kahshe.format.FormatConfig;
import io.kahshe.format.IndexPaths;
import java.util.LinkedHashMap;
import java.util.Map;
import org.apache.iceberg.CatalogUtil;
import org.apache.iceberg.io.FileIO;

/**
 * The app's external index IO, resolved the way Iceberg resolves one: an implementation class name
 * plus dotted properties, handed to {@link CatalogUtil#loadFileIO}. Registered with
 * {@link IndexPaths#externalIo} at startup, because the format module never constructs a storage
 * client of its own.
 *
 * <p>{@code KAHSHE_INDEX_IO_IMPL} names any {@code FileIO} on the classpath and
 * {@code KAHSHE_INDEX_IO_PROPERTIES} carries its configuration, so a GCS or ADLS index root needs
 * no code here. The {@code KAHSHE_INDEX_S3_*} settings are the shorthand for the common case: they
 * map onto the same dotted keys Iceberg's own {@code S3FileIO} reads, and an explicit properties
 * entry wins over them.
 */
final class IndexIo {
  /** Iceberg's own S3 FileIO, the default implementation for an {@code s3} index root. */
  static final String S3_FILE_IO = "org.apache.iceberg.aws.s3.S3FileIO";

  private IndexIo() {}

  static FileIO open(FormatConfig config) {
    String impl = config.indexIoImpl();
    if (impl.isBlank()) {
      throw new IllegalStateException(
          "the index root " + config.indexRoot() + " needs a FileIO implementation: set "
              + "KAHSHE_INDEX_IO_IMPL (an s3 root defaults to " + S3_FILE_IO + ")");
    }
    // null hadoopConf: loadFileIO forwards only a non-null one to a Configurable FileIO, and
    // nothing here holds a Configuration to give. Built once; IndexPaths caches the client.
    return CatalogUtil.loadFileIO(impl, properties(config), null);
  }

  /**
   * The properties the implementation is initialized with: the {@code KAHSHE_INDEX_S3_*}
   * shorthand keys first, then {@code KAHSHE_INDEX_IO_PROPERTIES} over them. Explicit wins, so a
   * key spelled out by hand is never overwritten by the shorthand for it.
   */
  static Map<String, String> properties(FormatConfig config) {
    Map<String, String> props = new LinkedHashMap<>(
        s3Properties(
            config.indexS3Endpoint(),
            config.indexS3AccessKey(),
            config.indexS3SecretKey(),
            config.indexS3Region()));
    props.putAll(config.indexIoProperties());
    return props;
  }

  /**
   * The default implementation for an index root: Iceberg's S3 FileIO for an {@code s3} URI, and
   * nothing otherwise. Nothing means the table's own IO, which is what an in-table index root uses.
   */
  static String defaultImpl(String indexRoot) {
    return indexRoot != null && indexRoot.startsWith("s3") ? S3_FILE_IO : "";
  }

  /**
   * Parses {@code KAHSHE_INDEX_IO_PROPERTIES}: comma-separated {@code key=value} pairs carrying
   * Iceberg's dotted keys, e.g. {@code s3.endpoint=http://minio:9000,s3.path-style-access=true}.
   * Keys and values are trimmed and empty entries skipped, so a trailing comma or a value spread
   * over a YAML block scalar is not an error. A value may itself contain {@code =} (a query
   * string, a base64 tail): only the first one splits.
   */
  static Map<String, String> parseProperties(String spec) {
    Map<String, String> props = new LinkedHashMap<>();
    if (spec == null || spec.isBlank()) {
      return props;
    }
    for (String entry : spec.split(",")) {
      String pair = entry.trim();
      if (pair.isEmpty()) {
        continue;
      }
      int eq = pair.indexOf('=');
      if (eq <= 0) {
        throw new IllegalArgumentException(
            "KAHSHE_INDEX_IO_PROPERTIES entry is not key=value: " + pair);
      }
      props.put(pair.substring(0, eq).trim(), pair.substring(eq + 1).trim());
    }
    return props;
  }

  /**
   * The S3 client's properties for the external index root. Static keys are included only when
   * both are set; otherwise nothing credential-shaped is passed and the client falls back to the
   * AWS default provider chain — IRSA, workload identity, instance profile, environment — the only
   * one that rotates without a restart. A blank key is not "no key": it is an invalid credential.
   * The endpoint is likewise only set when given, so plain AWS needs none.
   */
  static Map<String, String> s3Properties(
      String endpoint, String accessKey, String secretKey, String region) {
    Map<String, String> props = new LinkedHashMap<>();
    if (endpoint != null && !endpoint.isBlank()) {
      props.put("s3.endpoint", endpoint);
      props.put("s3.path-style-access", "true"); // MinIO, Ceph and friends need it; AWS does not
    }
    if (accessKey != null && !accessKey.isBlank() && secretKey != null && !secretKey.isBlank()) {
      props.put("s3.access-key-id", accessKey);
      props.put("s3.secret-access-key", secretKey);
    }
    props.put("client.region", region);
    return props;
  }
}
