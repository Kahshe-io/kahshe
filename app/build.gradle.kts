// kahshe-app: the one distribution. main, the roles (KAHSHE_MODE and the index subcommand),
// the environment read once into the per-module config records, and the app's own external
// index IO (the only place iceberg-aws is named). Depends on everything; nothing depends on it.
plugins {
    java
    application
}

dependencies {
    // The SHADED Hadoop client, not hadoop-common: ClientDemo hands Iceberg a Configuration, so
    // this module compiles against the API jar and the runtime jar carries the relocated
    // dependencies — nothing leaks its own Guava or commons-collections onto a host classpath.
    implementation("org.apache.hadoop:hadoop-client-api:3.4.1")
    runtimeOnly("org.apache.hadoop:hadoop-client-runtime:3.4.1")
    implementation(project(":common"))
    implementation(project(":analysis"))
    implementation(project(":format"))
    implementation(project(":indexer"))
    implementation(project(":proxy"))
    implementation(project(":watch"))
    implementation("org.apache.iceberg:iceberg-api:1.11.0")
    implementation("org.apache.iceberg:iceberg-core:1.11.0")
    implementation("org.apache.iceberg:iceberg-aws:1.11.0")
    runtimeOnly("org.apache.iceberg:iceberg-aws-bundle:1.11.0")
    implementation("org.slf4j:slf4j-simple:2.0.13")
    testImplementation("org.junit.jupiter:junit-jupiter:5.10.2")
    testRuntimeOnly("org.junit.platform:junit-platform-launcher")
}

application {
    mainClass = "io.kahshe.Kahshe"
    // the distribution keeps its name: bin/kahshe, and the Dockerfiles copy app/build/install/kahshe
    applicationName = "kahshe"
}

// DocTablesTest reads markdown from outside this module, so Gradle cannot see that a doc changed
// and would report the last run's result — a check that never re-runs is worse than none, because
// it reads as passing. Naming the files as inputs makes an edited document re-run the tests.
tasks.test {
    inputs.files(
        fileTree(rootDir) {
            include("*.md", "docs/*.md", "benchmark/*.md", "dev/trino-patch/*.md",
                    "helm/kahshe/*.md", "sigma/*.md", "*/README.md")
        }
    ).withPropertyName("repositoryMarkdown").withPathSensitivity(PathSensitivity.RELATIVE)
}
