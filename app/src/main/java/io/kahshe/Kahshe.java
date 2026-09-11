package io.kahshe;

import io.kahshe.common.Metrics;
import io.kahshe.format.BuildLease;
import io.kahshe.analysis.analyzer.Analyzer;
import io.kahshe.format.FormatConfig;
import io.kahshe.format.type.gram.Grams;
import io.kahshe.format.IndexPaths;
import io.kahshe.indexer.BuildConfig;
import io.kahshe.indexer.TableSource;
import io.kahshe.indexer.build.IndexBuildListener;
import io.kahshe.indexer.build.IndexBuilder;
import io.kahshe.indexer.maintain.IndexStatus;
import io.kahshe.indexer.maintain.IndexerService;
import io.kahshe.proxy.http.AdminAuth;
import io.kahshe.proxy.http.AdminHandler;
import io.kahshe.proxy.catalog.BackendCatalogs;
import io.kahshe.proxy.catalog.Mutations;
import io.kahshe.proxy.http.Forwarder;
import io.kahshe.proxy.http.KahsheHandler;
import io.kahshe.proxy.plan.PlanService;
import io.kahshe.proxy.ProxyConfig;
import io.kahshe.watch.sink.AlertSink;
import io.kahshe.watch.sink.AlertSinks;
import io.kahshe.watch.Alerts;
import io.kahshe.watch.ReportPoller;
import io.kahshe.watch.scan.ScanContext;
import io.kahshe.watch.scan.ScanPass;
import io.kahshe.watch.TableDiscovery;
import io.kahshe.watch.WatchConfig;
import io.kahshe.watch.WatchEngine;
import io.kahshe.watch.rules.WatchRules;
import com.sun.net.httpserver.HttpServer;
import com.sun.net.httpserver.HttpsServer;
import java.io.IOException;
import java.net.InetSocketAddress;
import java.util.Map;
import java.util.concurrent.ArrayBlockingQueue;
import java.util.concurrent.Executors;
import java.util.concurrent.ThreadPoolExecutor;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;
import io.kahshe.format.type.term.TermIndex;
import io.kahshe.watch.scan.HuntPass;
import org.apache.iceberg.Table;
import org.apache.iceberg.catalog.TableIdentifier;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Entry point: transparent passthrough to a backing Iceberg REST catalog, plus an implementation
 * of the spec's server-side scan-planning endpoints.
 *
 * <p>This is the only place the environment is read and the only place the roles are wired
 * together; which of them a process runs is decided by {@code KAHSHE_MODE}. The module README
 * describes the resulting deployment shapes.
 */
public final class Kahshe {
  private static final Logger LOG = LoggerFactory.getLogger(Kahshe.class);

  private Kahshe() {}

  public static void main(String[] args) throws Exception {
    Config config = Config.fromEnv();
    // The format never builds a storage client; the app says how an external index root is
    // reached -- by FileIO implementation class and dotted properties, as Iceberg resolves one.
    IndexPaths.externalIo(IndexIo::open);
    if (args.length > 0 && "index".equals(args[0])) {
      if (args.length != 4) {
        System.err.println("usage: kahshe index <prefix> <namespace.table> <column>");
        System.exit(2);
      }
      // CLI builds alert too when rules are configured: alerts follow builds wherever they happen
      BackendCatalogs cliCatalogs = new BackendCatalogs(config.proxy());
      WatchWiring wiring = watchWiring(config, new Metrics(), cliCatalogs);
      IndexBuilder.run(
          cliCatalogs, config.build(),
          args[1], args[2], args[3], wiring.listener());
      // an async sink drains on a daemon thread; flush before exit so queued alerts are not lost
      wiring.sink().awaitDrain(15_000);
      return;
    }
    if (args.length > 0 && "status".equals(args[0])) {
      if (args.length != 3) {
        System.err.println("usage: kahshe status <prefix> <namespace.table>");
        System.exit(2);
      }
      // The same document GET /index serves, from storage: this process observed nothing and
      // built nothing, so the catalog and the build reports are all it has, and the output says
      // so field by field.
      BackendCatalogs cliCatalogs = new BackendCatalogs(config.proxy());
      TableIdentifier ident = TableIdentifier.parse(args[2]);
      Table table = cliCatalogs.load(args[1], ident);
      IndexStatus status = IndexStatus.fromStorage(
          table, args[1], ident.namespace().toString(), ident.name(), config.build(),
          System.currentTimeMillis());
      System.out.println(
          status.toJson(IndexStatus.storageSources(
                  config.proxy().backendBase(),
                  IndexPaths.root(table, config.format().indexRoot())))
              .toPrettyString());
      return;
    }
    if (args.length > 0 && "hunt".equals(args[0])) {
      // A trailing `--out <path>` writes the result set as JSON lines; the summary still prints.
      boolean toFile = args.length >= 3 && "--out".equals(args[args.length - 2]);
      String outPath = toFile ? args[args.length - 1] : null;
      final String[] a = toFile ? java.util.Arrays.copyOf(args, args.length - 2) : args;
      boolean byRule = a.length == 3 && "--rule".equals(a[1]);
      if (!byRule && a.length != 5) {
        System.err.println(
            "usage: kahshe hunt <prefix> <namespace.table> <column> <term> [--out <file.jsonl>]");
        System.err.println(
            "       kahshe hunt --rule <id> [--out <file.jsonl>]      (from KAHSHE_WATCH_RULES)");
        System.err.println("  exit 3: refused -- the index cannot answer this without approximating");
        System.exit(2);
      }
      // One term, or one rule of the shape the index answers, over every data file the table
      // holds, from the term index alone: the retroactive question the watcher's prospective
      // paths cannot ask. Loads the table as the caller, reads the index the same way the proxy
      // does, reads no data file, alerts nothing.
      BackendCatalogs cliCatalogs = new BackendCatalogs(config.proxy());
      Metrics cliMetrics = new Metrics();
      HuntPass hunt = new HuntPass(new TermIndex(config.format(), cliMetrics));
      try {
        HuntPass.Result result;
        if (byRule) {
          // The rule is the whole specification -- it names its prefix and table -- and it is
          // read through the loader the watcher uses, so a rule this refuses is one the watcher
          // would have refused too.
          String rulesPath = config.watch().watchRulesPath();
          io.kahshe.watch.rules.WatchRule rule =
              new WatchRules(rulesPath, cliMetrics).current().stream()
                  .filter(r -> r.id().equals(a[2]))
                  .findFirst()
                  .orElseThrow(() -> new HuntPass.Refused(
                      "no rule '" + a[2] + "' loaded from KAHSHE_WATCH_RULES=" + rulesPath));
          TableIdentifier ident = TableIdentifier.parse(rule.table());
          Table table = cliCatalogs.load(rule.prefix(), ident);
          result = hunt.hunt(table, rule).withConfirmationSql(
              config.watch().watchSqlCatalog(), ident.namespace().toString(), ident.name());
        } else {
          TableIdentifier ident = TableIdentifier.parse(a[2]);
          Table table = cliCatalogs.load(a[1], ident);
          result = hunt.hunt(table, a[3], a[4]).withConfirmationSql(
              config.watch().watchSqlCatalog(), ident.namespace().toString(), ident.name());
        }
        System.out.println(result.toJson().toPrettyString());
        if (outPath != null) {
          // The file is the deliverable; stdout is the glance. Written whole after the hunt
          // returned, so a refusal leaves no half-written result behind.
          try (java.io.Writer out = java.nio.file.Files.newBufferedWriter(
              java.nio.file.Path.of(outPath))) {
            result.writeJsonl(out);
          }
          System.err.println("wrote " + (1 + result.hit().size() + result.unresolved().size())
              + " line(s) to " + outPath);
        }
      } catch (HuntPass.Refused e) {
        // A refusal is the answer, not a crash: it says why, and a distinct exit code lets a
        // script tell "refused" from "usage" without parsing the message.
        System.err.println("refused: " + e.getMessage());
        System.exit(3);
      }
      return;
    }
    AtomicBoolean shuttingDown = new AtomicBoolean(false);
    Metrics metrics = new Metrics();

    String mode = config.mode();
    if (!"proxy".equals(mode) && !"watch".equals(mode) && !"both".equals(mode)) {
      LOG.warn("KAHSHE_MODE={} is not proxy|watch|both; using both", mode);
      mode = "both";
    }
    boolean indexerOn = config.indexerEnabled();
    boolean rulesSet = !config.watch().watchRulesPath().isBlank();
    if ("watch".equals(mode) && !rulesSet) {
      LOG.warn("KAHSHE_MODE=watch but KAHSHE_WATCH_RULES is unset; this instance will do "
          + "nothing until rules are configured (no data plane, nothing to discover)");
    }
    // Detection does not depend on the indexer; WatchRoles says which roles each mode runs.
    WatchRoles roles = WatchRoles.of(mode, indexerOn, rulesSet);
    if (rulesSet && !roles.any()) {
      LOG.warn("KAHSHE_WATCH_RULES is set but KAHSHE_MODE=proxy with the indexer off has no watch "
          + "role here: rules neither ride builds nor scan on this instance");
    }

    BackendCatalogs catalogs = new BackendCatalogs(config.proxy());
    // A process with no data plane only needs TABLES, so it may use any Iceberg catalog — Glue,
    // Hive, JDBC — through KAHSHE_CATALOG_IMPL. The proxy cannot: it IS a REST catalog and
    // forwards everything it does not serve, so a non-REST backend would half work. Hence the
    // mode test, not a plain if-configured.
    CatalogSource foreign = "watch".equals(mode)
        ? CatalogSource.fromEnv(config.catalogImpl(), config.catalogProperties(),
            config.proxy().backendWarehouse())
        : null;
    if (foreign == null && !config.catalogImpl().isBlank()) {
      LOG.warn("KAHSHE_CATALOG_IMPL is set but KAHSHE_MODE={} serves the REST data plane, which "
          + "needs the REST backend client; using it and ignoring the setting", mode);
    }
    TableSource tables = foreign != null ? foreign : catalogs;
    WatchRules watchRules = null;
    Alerts alerts = null;
    IndexBuildListener listener = IndexBuildListener.NONE;
    if (roles.any()) {
      watchRules = new WatchRules(config.watch().watchRulesPath(), metrics);
      // One Alerts for every path: one payload shape, one sink, and one (rule, file) claim, so a
      // rule the build path, the row scan and a build report can all answer alerts once.
      alerts = new Alerts(AlertSinks.fromEnv(config.watch(), metrics), metrics,
          config.watch().watchSqlCatalog());
      if (roles.indexRiding()) {
        listener = new WatchEngine(
            watchRules, alerts, metrics, config.watch().watchRealertOnRebuild(),
            new SnapshotDeletes(tables));
      }
    }
    IndexerService indexer =
        new IndexerService(tables, metrics, indexerOn, config.build(), listener);
    if (roles.reports()) {
      new ReportPoller(watchRules, tables, config.format(), config.watch(), alerts, metrics)
          .start();
    }
    if (roles.discovery()) {
      ScanPass scan = null;
      if (config.watch().watchScan()) {
        scan = new ScanPass(
            new ScanContext(watchRules, alerts, metrics, config.watch().watchWindowMaxKeys(),
                config.watch().watchReplayMaxFiles()),
            config.format(), config.watch().watchScanThreads());
      } else {
        LOG.info("KAHSHE_WATCH_SCAN=false: no row scan. Rules spanning columns, and rules on "
            + "columns kahshe.index does not name, cannot fire");
      }
      new TableDiscovery(watchRules, tables, indexer, config.watch(), metrics, scan).start();
    }

    // TLS, if configured. Built before either server binds so a bad certificate fails startup
    // rather than the first handshake — an operator reading "started" must not then discover
    // that the port they secured is answering plaintext.
    ServerTls.Settings tlsSettings = ServerTls.fromEnv(System.getenv());
    ServerTls tls = tlsSettings.enabled() ? new ServerTls(tlsSettings) : null;
    if (tls == null) {
      LOG.warn("TLS is not configured: ports {} and {} serve plaintext HTTP, and the data plane "
              + "carries bearer tokens. Set KAHSHE_TLS_CERT and KAHSHE_TLS_KEY, or terminate TLS "
              + "in front of this process.",
          config.port(), config.adminPort());
    }

    HttpServer server = null;
    if (!"watch".equals(mode)) {
      if (!config.proxy().callerIdentityPlanning()) {
        // Watch mode has no served path, so the identity is only worth announcing here. The
        // other two posture warnings above and below are about what is exposed; this one is
        // about who is trusted to read on a caller's behalf.
        LOG.warn("KAHSHE_PLANNING_IDENTITY=service: /plan and /_count read table metadata as "
                + "kahshe (KAHSHE_CREDENTIAL) once the caller's own token has loaded the table, "
                + "and one plan per table is shared by every caller the backend admits. Row "
                + "filters, column masks and vended-credential scoping at the catalog do not "
                + "reach the served endpoints. Right only where table-level read means "
                + "see-everything; the default, caller, is the other case.");
      }
      server = create(new InetSocketAddress(config.port()), tls);
      server.createContext("/", new KahsheHandler(config.proxy(), config.format(), metrics, catalogs, indexer));
      // bounded pool: saturation queues briefly, then connections are refused — never unbounded
      server.setExecutor(
          new ThreadPoolExecutor(
              config.workerThreads(),
              config.workerThreads(),
              30,
              TimeUnit.SECONDS,
              new ArrayBlockingQueue<>(256),
              new ThreadPoolExecutor.AbortPolicy()));
      server.start();
    }

    AdminAuth adminAuth = AdminAuth.fromToken(config.adminToken(), metrics);
    if (!adminAuth.enabled()) {
      LOG.warn("KAHSHE_ADMIN_TOKEN is unset: the admin port {}:{} serves /metrics to anyone who "
              + "can reach it. Set it, or narrow the bind with KAHSHE_ADMIN_BIND.",
          config.adminBind(), config.adminPort());
    }
    AdminHandler adminHandler =
        new AdminHandler(
            metrics,
            new Forwarder(config.proxy().backendBase(), 3_000, config.proxy().backendCa()),
            shuttingDown,
            adminAuth,
            indexer);
    InetSocketAddress adminAddress = new InetSocketAddress(config.adminBind(), config.adminPort());
    // The admin port stays plaintext unless asked otherwise: it is usually scraped by a
    // collector inside the same pod, and forcing TLS there breaks that for no gain.
    HttpServer admin = create(adminAddress, tlsSettings.admin() ? tls : null);
    admin.createContext("/", adminHandler);
    // One thread is enough because no admin handler blocks on anything: the readiness probe runs
    // on AdminHandler's own thread.
    admin.setExecutor(Executors.newSingleThreadExecutor());
    admin.start();

    HttpServer dataPlane = server;
    Runtime.getRuntime()
        .addShutdownHook(
            new Thread(
                () -> {
                  // First, before any draining: a build this process holds the lease for is about
                  // to die with the JVM, and without this the next process to try that column is
                  // refused for the rest of the lease's TTL.
                  int leases = BuildLease.releaseAll();
                  if (leases > 0) {
                    LOG.info("shutdown: released {} build lease(s) held by this process", leases);
                  }
                  LOG.info("shutdown: flipping readiness, draining for 5s");
                  shuttingDown.set(true);
                  try {
                    Thread.sleep(2_000); // let the endpoint drop propagate before severing
                  } catch (InterruptedException ignored) {
                    Thread.currentThread().interrupt();
                  }
                  if (dataPlane != null) {
                    dataPlane.stop(5);
                  }
                  admin.stop(1);
                  adminHandler.close();
                }));

    if (dataPlane == null) {
      LOG.info(
          "kahshe watch mode: admin on {}:{} -> backend {} (no data plane; {})",
          config.adminBind(),
          config.adminPort(),
          config.proxy().backendBase(),
          indexerOn
              ? "building, scanning and alerting in process"
              : "scanning, and delivering the build reports of builds elsewhere; indexer off");
    } else {
      LOG.info(
          "kahshe listening on :{} (admin {}:{}) -> backend {} (inject planning: {}, planning "
              + "identity: {}, mode: {})",
          config.port(),
          config.adminBind(),
          config.adminPort(),
          config.proxy().backendBase(),
          config.proxy().injectPlanning(),
          config.proxy().planningIdentity(),
          mode);
    }
  }

  /** The build-listener wiring for the CLI index path: active when KAHSHE_WATCH_RULES is set. */
  private record WatchWiring(
      IndexBuildListener listener, AlertSink sink) {}

  private static WatchWiring watchWiring(Config config, Metrics metrics, TableSource tables) {
    AlertSink sink = AlertSinks.fromEnv(config.watch(), metrics);
    if (config.watch().watchRulesPath().isBlank()) {
      return new WatchWiring(IndexBuildListener.NONE, sink);
    }
    return new WatchWiring(
        new WatchEngine(
            new WatchRules(config.watch().watchRulesPath(), metrics),
            sink,
            metrics,
            config.watch().watchRealertOnRebuild(),
            config.watch().watchSqlCatalog(),
            new SnapshotDeletes(tables)),
        sink);
  }

  /** An HTTP or HTTPS server on {@code address}, depending on whether TLS was configured. */
  private static HttpServer create(InetSocketAddress address, ServerTls tls) throws IOException {
    if (tls == null) {
      return HttpServer.create(address, 0);
    }
    HttpsServer server = HttpsServer.create(address, 0);
    server.setHttpsConfigurator(tls.configurator());
    return server;
  }

  /**
   * Runtime configuration, read from the environment exactly once (the variables are listed in the
   * README). The fields outside the nested records are the app's own — which roles this process
   * runs, where it listens, and which catalog a process with no data plane reads. Everything else
   * is partitioned by the module that reads it, so a class is handed the record it needs and
   * nothing else: {@link ProxyConfig} for serving, {@link FormatConfig} for reading and writing the
   * index (carried inside {@link BuildConfig} for a build) and {@link WatchConfig} for alerting.
   *
   * @param adminBind the address the admin port binds; every interface by default, which is what
   *     it always did. The data plane has no such setting because it is the service.
   * @param adminToken bearer the admin port requires on everything but its probes; empty is open
   * @param catalogImpl a non-REST Iceberg catalog for a process with no data plane: any
   *     {@code Catalog} on the classpath, loaded the way Iceberg loads one. Empty means the REST
   *     client, which is the default and the only thing the proxy can use. See
   *     {@link CatalogSource}.
   */
  public record Config(
      int port,
      int adminPort,
      String adminBind,
      String adminToken,
      int workerThreads,
      String mode,
      boolean indexerEnabled,
      ProxyConfig proxy,
      BuildConfig build,
      WatchConfig watch,
      String catalogImpl,
      Map<String, String> catalogProperties) {

    /** The one {@link FormatConfig}: the build carries it, and readers take it from here. */
    public FormatConfig format() {
      return build.format();
    }

    public static Config fromEnv() {
      // Logback read this before main ran and, for a name it does not know, attached no
      // appender: the process would run and log nothing. Refused here instead, like every other
      // bad value.
      checkLogFormat(env("KAHSHE_LOG_FORMAT", "text"));
      String backend = env("KAHSHE_BACKEND", "http://localhost:8181/api/catalog");
      // strip trailing slash so path concatenation is uniform
      if (backend.endsWith("/")) {
        backend = backend.substring(0, backend.length() - 1);
      }
      ProxyConfig proxy = new ProxyConfig(
          backend,
          env("KAHSHE_CREDENTIAL", ""),
          env("KAHSHE_SCOPE", "PRINCIPAL_ROLE:ALL"),
          Boolean.parseBoolean(env("KAHSHE_INJECT_PLANNING", "true")),
          intEnv("KAHSHE_AUTH_CACHE_TTL_MS", 60000),
          intEnv("KAHSHE_BACKEND_TIMEOUT_MS", 5000),
          intEnv("KAHSHE_MAX_BODY_BYTES", 16 * 1024 * 1024),
          // caller (default) or service: whose credentials the served endpoints read metadata
          // with. The one spelling of the default is the record's; a service opt-in is announced
          // at startup, since it is the mode under which the catalog's per-principal rules stop
          // at the table.
          checkPlanningIdentity(
              env("KAHSHE_PLANNING_IDENTITY", ProxyConfig.DEFAULT_PLANNING_IDENTITY)),
          env("KAHSHE_BACKEND_WAREHOUSE", ""),
          // Whether to plan snapshots that carry delete files; refused by default. See
          // PlanService's guard for what the refusal protects and who can safely lift it.
          Boolean.parseBoolean(env("KAHSHE_SERVE_DELETE_BEARING", "false")),
          // all (default), indexed or none: which tables are told they MUST plan server-side.
          // Narrow it when a client cannot do something under that instruction; a table not
          // advertised to still reads the same files, it just plans them itself.
          checkAdvertiseServerMode(env("KAHSHE_ADVERTISE_SERVER_MODE", "all")),
          // 0 disables table caching entirely, which is what a multi-replica deployment needs.
          longEnv("KAHSHE_TABLE_CACHE_TTL_MS", 10_000),
          // strip (default) or requested: whether a plan response carries per-file column
          // statistics for the columns the request names. Trading disclosure for engine-side
          // speed, hence the conservative default.
          env("KAHSHE_PLAN_STATS", "strip"),
          // A backend behind a private CA. Scoped to the catalog clients on purpose: it
          // does not change how the S3 client that reads index files is trusted.
          env("KAHSHE_BACKEND_CA", ""));
      String indexRoot = env("KAHSHE_INDEX_ROOT", "");
      FormatConfig format = new FormatConfig(
          indexRoot,
          env("KAHSHE_INDEX_S3_ENDPOINT", ""),
          env("KAHSHE_INDEX_S3_ACCESS_KEY", ""),
          env("KAHSHE_INDEX_S3_SECRET_KEY", ""),
          env("KAHSHE_INDEX_S3_REGION", "us-east-1"),
          // Any FileIO on the classpath; an s3 root defaults to Iceberg's own S3FileIO, so a
          // deployment that sets only the KAHSHE_INDEX_S3_* shorthand needs nothing here.
          env("KAHSHE_INDEX_IO_IMPL", IndexIo.defaultImpl(indexRoot)),
          // Iceberg's dotted keys, comma separated: s3.endpoint=...,s3.path-style-access=true.
          // These win over the KAHSHE_INDEX_S3_* shorthand for the same keys; see IndexIo.
          IndexIo.parseProperties(env("KAHSHE_INDEX_IO_PROPERTIES", "")),
          // "index" reads data files through the index client; see IndexPaths.dataIo
          env("KAHSHE_DATA_IO", "table"),
          // unset or "auto" (non-numeric) falls back to 40% of max heap
          longEnv("KAHSHE_CACHE_BYTES", (long) (Runtime.getRuntime().maxMemory() * 0.40)),
          Boolean.parseBoolean(env("KAHSHE_GRAM_INDEX", "true")),
          Boolean.parseBoolean(env("KAHSHE_TERM_INDEX", "true")),
          intEnv("KAHSHE_PREFIX_MAX_TERMS", 100_000));
      BuildConfig build = new BuildConfig(
          format,
          // Default cap on an indexed token. Per-column overrides ride on table properties; see
          // Analyzer. Changing this changes the analyzer id and needs a reindex.
          tokenLength(longEnv("KAHSHE_MAX_TOKEN_LENGTH", Analyzer.DEFAULT_MAX_TOKEN_LEN)),
          // Default gram size; per-column overrides ride on table properties (kahshe.index.<column>.ngram).
          // Changing it changes the gram rule id, and the affected columns rebuild rolling.
          intEnv("KAHSHE_NGRAM", Grams.DEFAULT_SIZE),
          longEnv("KAHSHE_GRAM_BUILD_MAX_BYTES", 1024L * 1024 * 1024),
          longEnv("KAHSHE_TERM_BUILD_MAX_SPILL_BYTES", 16L * 1024 * 1024 * 1024),
          env("KAHSHE_TERM_BUILD_DIR",
              System.getProperty("java.io.tmpdir") + java.io.File.separator + "kahshe-term-build"),
          // Threaded through config rather than read at the point of use, so a test can vary them:
          // a static final read at class initialization is pinned by the first build in the JVM.
          intEnv("KAHSHE_INDEX_THREADS", 8),
          longEnv("KAHSHE_TERM_BUFFER_BYTES", 64L * 1024 * 1024),
          longEnv("KAHSHE_INDEX_STALE_WARN_MS", 300_000),
          // A member of a builder fleet skips the columns another member holds, rather than
          // refusing loudly: the refusal is right for a lone builder and wrong for one of N.
          Boolean.parseBoolean(env("KAHSHE_INDEX_FLEET", "false")),
          intEnv("KAHSHE_INDEX_FLEET_ORDINAL", -1));
      WatchConfig watch = new WatchConfig(
          env("KAHSHE_WATCH_RULES", ""),
          // which AlertSink delivers; an unknown name fails startup rather than falling back
          env("KAHSHE_WATCH_SINK", "webhook"),
          env("KAHSHE_WATCH_WEBHOOK", ""),
          env("KAHSHE_WATCH_WEBHOOK_AUTH", ""),
          longEnv("KAHSHE_WATCH_WEBHOOK_TIMEOUT_MS", 5000),
          longEnv("KAHSHE_WATCH_POLL_MS", 60000),
          env("KAHSHE_WATCH_SQL_CATALOG", "iceberg"),
          Boolean.parseBoolean(env("KAHSHE_WATCH_REALERT_ON_REBUILD", "false")),
          // The row scan reads the columns the rules name out of every new data file. On by
          // default: without it a rule spanning columns cannot be evaluated at all, and one on an
          // unindexed column never fires. false leaves only the index-riding path.
          Boolean.parseBoolean(env("KAHSHE_WATCH_SCAN", "true")),
          intEnv("KAHSHE_WATCH_SCAN_THREADS", 4),
          // Live keys a window rule may track. An eviction loses a partial window, which is a
          // miss, so this is a memory-versus-misses dial and both ends are counted.
          intEnv("KAHSHE_WATCH_WINDOW_MAX_KEYS", 200_000),
          // A day-long timeframe on a busy table would otherwise read the world at startup.
          intEnv("KAHSHE_WATCH_REPLAY_MAX_FILES", 2_000));
      return new Config(
          intEnv("KAHSHE_PORT", 8282),
          intEnv("KAHSHE_ADMIN_PORT", 8283),
          // 127.0.0.1 or the pod's own address in production, so /metrics is reachable by the
          // collector and nothing else; the wildcard default changes nothing for a quickstart.
          adminBind(env("KAHSHE_ADMIN_BIND", "0.0.0.0")),
          env("KAHSHE_ADMIN_TOKEN", ""),
          intEnv("KAHSHE_WORKER_THREADS", 32),
          env("KAHSHE_MODE", "both"),
          Boolean.parseBoolean(env("KAHSHE_INDEXER", "true")),
          proxy,
          build,
          watch,
          env("KAHSHE_CATALOG_IMPL", ""),
          IndexIo.parseProperties(env("KAHSHE_CATALOG_PROPERTIES", "")));
    }

    /**
     * Kubernetes service links inject variables like KAHSHE_PORT=tcp://ip:port when a Service
     * shares the app's name; a non-numeric value falls back to the default instead of crashing.
     */
    private static int intEnv(String key, int defaultValue) {
      try {
        return Integer.parseInt(env(key, String.valueOf(defaultValue)));
      } catch (NumberFormatException e) {
        return defaultValue;
      }
    }

    /**
     * The admin bind address, resolved now rather than at the bind: the data plane starts first,
     * and an "Unresolved address" thrown after it is up leaves a process serving with no admin
     * port and no variable named in the message.
     */
    static String adminBind(String value) {
      if (new InetSocketAddress(value, 0).isUnresolved()) {
        throw new IllegalArgumentException("KAHSHE_ADMIN_BIND=" + value
            + " does not resolve; give an interface address, 127.0.0.1, or 0.0.0.0");
      }
      return value;
    }

    /**
     * The three tables-to-advertise-to modes, refused at startup rather than silently widened.
     * An operator who sets this has set it to narrow the instruction; a typo that fell back to
     * the permissive default would do the opposite of what they asked, quietly.
     */
    static String checkAdvertiseServerMode(String value) {
      if (!value.equals(Mutations.ADVERTISE_ALL)
          && !value.equals(Mutations.ADVERTISE_INDEXED)
          && !value.equals(Mutations.ADVERTISE_NONE)) {
        throw new IllegalArgumentException("KAHSHE_ADVERTISE_SERVER_MODE must be all, indexed or "
            + "none (lowercase), not '" + value + "'");
      }
      return value;
    }

    /**
     * The two planning identities, refused at startup rather than silently widened. The reader
     * is an exact match on {@code caller}, so any other spelling would plan as the service — the
     * mode an operator who typed {@code Caller} was asking to leave.
     */
    static String checkPlanningIdentity(String value) {
      if (!value.equals(ProxyConfig.PLANNING_CALLER)
          && !value.equals(ProxyConfig.PLANNING_SERVICE)) {
        throw new IllegalArgumentException("KAHSHE_PLANNING_IDENTITY must be caller or service "
            + "(lowercase), not '" + value + "'");
      }
      return value;
    }

    /** The fragment names logback.xml can select by {@code KAHSHE_LOG_FORMAT}; nothing else. */
    static void checkLogFormat(String value) {
      if (!value.equals("text") && !value.equals("json")) {
        throw new IllegalArgumentException("KAHSHE_LOG_FORMAT must be text or json (lowercase), "
            + "not '" + value + "': logback selects a fragment by that name and attaches no "
            + "appender for one it lacks");
      }
    }

    /**
     * The token cap, refused at startup rather than at the first build. {@code Analyzer.Contract}
     * says why 0 is not "unlimited"; a value past int range would wrap on the cast to 0, to a
     * negative, or to a cap nobody asked for. Both fail here, the way an unknown KAHSHE_WATCH_SINK
     * does, with the reason.
     */
    static int tokenLength(long value) {
      if (value <= 0 || value > Integer.MAX_VALUE) {
        throw new IllegalArgumentException("KAHSHE_MAX_TOKEN_LENGTH must be between 1 and "
            + Integer.MAX_VALUE + " (a cap of 0 is not 'unlimited'; the default is "
            + Analyzer.DEFAULT_MAX_TOKEN_LEN + "): " + value);
      }
      return (int) value;
    }

    private static long longEnv(String key, long defaultValue) {
      try {
        return Long.parseLong(env(key, String.valueOf(defaultValue)));
      } catch (NumberFormatException e) {
        return defaultValue;
      }
    }

    private static String env(String key, String defaultValue) {
      String value = System.getenv(key);
      return value == null || value.isBlank() ? defaultValue : value;
    }
  }
}
