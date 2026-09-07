package io.kahshe;

import java.util.Map;
import org.apache.hadoop.conf.Configuration;
import org.apache.iceberg.CatalogProperties;
import org.apache.iceberg.FileScanTask;
import org.apache.iceberg.Table;
import org.apache.iceberg.catalog.TableIdentifier;
import org.apache.iceberg.expressions.Expressions;
import org.apache.iceberg.io.CloseableIterable;
import org.apache.iceberg.rest.RESTCatalog;

/**
 * Demo: a completely stock Iceberg REST client pointed at kahshe. No scan-planning config on the
 * client side — the proxy's LoadTableResponse injection flips it to server-side planning.
 */
public final class ClientDemo {
  private ClientDemo() {}

  public static void main(String[] args) throws Exception {
    String proxy = args.length > 0 ? args[0] : "http://localhost:8282";
    try (RESTCatalog catalog = new RESTCatalog()) {
      catalog.setConf(new Configuration());
      catalog.initialize(
          "demo",
          Map.of(
              CatalogProperties.URI, proxy,
              CatalogProperties.WAREHOUSE_LOCATION, "lakehouse",
              "credential", "root:s3cr3t",
              "oauth2-server-uri", proxy + "/v1/oauth/tokens",
              "scope", "PRINCIPAL_ROLE:ALL"));

      Table table = catalog.loadTable(TableIdentifier.of("logs", "events"));
      System.out.println("loaded table class: " + table.getClass().getName());

      int unfiltered = countTasks(table, null);
      int filtered = countTasks(table, "id>=3000");
      System.out.println("planFiles() unfiltered tasks: " + unfiltered);
      System.out.println("planFiles() id>=3000 tasks:   " + filtered);
    }
  }

  private static int countTasks(Table table, String filter) throws Exception {
    var scan = table.newScan();
    if (filter != null) {
      scan = scan.filter(Expressions.greaterThanOrEqual("id", 3000));
    }
    int count = 0;
    try (CloseableIterable<FileScanTask> tasks = scan.planFiles()) {
      for (FileScanTask ignored : tasks) {
        count++;
      }
    }
    return count;
  }
}
