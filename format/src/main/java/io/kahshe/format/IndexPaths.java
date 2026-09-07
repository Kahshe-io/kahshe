package io.kahshe.format;

import org.apache.iceberg.Table;
import org.apache.iceberg.io.FileIO;

/**
 * Resolves the index root for a table and the {@link FileIO} that reaches it. The default root is
 * {@code <table location>/_index} — zero configuration, but exposed to table-maintenance orphan
 * cleanup, which deletes unreferenced files under the table location. Setting
 * {@code KAHSHE_INDEX_ROOT} moves indexes to {@code <root>/<table-uuid>-<location-hash>}, outside
 * any table location.
 *
 * <p>Index discovery is by path convention here, and nothing but this class assumes it: a resolver
 * that asked a catalog instead would replace it without touching readers or writers.
 */
public final class IndexPaths {
  private IndexPaths() {}

  private static volatile FileIO externalIo;

  private static volatile java.util.function.Function<FormatConfig, FileIO> externalIoFactory;

  /**
   * How an external index root is reached. The format constructs no storage client of its own: the
   * app registers a factory at startup, resolving {@link FormatConfig#indexIoImpl} and
   * {@link FormatConfig#indexIoProperties} the way Iceberg resolves a FileIO; an embedder registers
   * its own instead. The factory is applied once and its result cached for the process.
   */
  public static void externalIo(java.util.function.Function<FormatConfig, FileIO> factory) {
    externalIoFactory = factory;
  }

  /**
   * The FileIO for reading data files during a build.
   *
   * <p>Normally the table's own IO: the catalog vends whatever credentials and endpoint the data
   * needs. But a catalog is not obliged to vend a complete client — one that omits the endpoint or
   * path-style addressing leaves the SDK resolving a bucket-prefixed virtual host that does not
   * exist, and every build then fails on its first data file. {@code KAHSHE_DATA_IO=index} reads
   * data through the fully configured client kahshe already holds for its index root instead. It is
   * opt-in because it is correct only when the data and the index root live on the same store under
   * the same credentials.
   */
  public static FileIO dataIo(Table table, FormatConfig config) {
    return "index".equalsIgnoreCase(config.dataIo()) ? io(table, config) : table.io();
  }

  /**
   * The FileIO for index artifacts. Default root: the table's own IO — index files sit inside
   * the table location, covered by whatever the catalog vends. External root: table-vended
   * credentials are scoped to the table location (remote signers refuse foreign URIs), so
   * kahshe needs its own client, named by {@link FormatConfig#indexIoImpl}.
   */
  public static FileIO io(Table table, FormatConfig config) {
    // A named implementation is what decides, so an index root on any store the classpath can
    // reach gets the external client, not only an s3 one. The s3-prefix test covers the remaining
    // case: a config that names no implementation, with an embedder's own factory registered
    // against an s3 root.
    if (config.indexRoot().isBlank()) {
      return table.io();
    }
    if (config.indexIoImpl().isBlank() && !config.indexRoot().startsWith("s3")) {
      return table.io();
    }
    FileIO io = externalIo;
    if (io == null) {
      synchronized (IndexPaths.class) {
        if (externalIo == null) {
          java.util.function.Function<FormatConfig, FileIO> factory = externalIoFactory;
          if (factory == null) {
            throw new IllegalStateException(
                "the index root " + config.indexRoot() + " needs an external FileIO and none is "
                    + "registered: IndexPaths.externalIo(...) is the app's job at startup");
          }
          externalIo = factory.apply(config);
        }
        io = externalIo;
      }
    }
    return io;
  }

  public static String root(Table table, String configuredRoot) {
    if (configuredRoot == null || configuredRoot.isBlank()) {
      String location = table.location();
      String base = location.endsWith("/") ? location.substring(0, location.length() - 1) : location;
      return base + "/_index";
    }
    String base = configuredRoot.endsWith("/")
        ? configuredRoot.substring(0, configuredRoot.length() - 1)
        : configuredRoot;
    // uuid alone collides for registerTable-style clones that preserve the source uuid; a
    // location-derived suffix keeps diverged clones from rebuilding over each other's indexes
    return base + "/" + table.uuid() + "-" + locationHash(table.location());
  }

  private static String locationHash(String location) {
    try {
      byte[] digest = java.security.MessageDigest.getInstance("SHA-256")
          .digest(location.getBytes(java.nio.charset.StandardCharsets.UTF_8));
      StringBuilder hex = new StringBuilder(8);
      for (int i = 0; i < 4; i++) {
        hex.append(String.format("%02x", digest[i]));
      }
      return hex.toString();
    } catch (java.security.NoSuchAlgorithmException e) {
      throw new IllegalStateException(e);
    }
  }
}
