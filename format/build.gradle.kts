// kahshe-format: the bytes docs/FORMAT.md specifies and the code that answers from them -- each
// tier's reader and writer together, the shared vocabulary, the lease, the pruner. Depends on
// common and on Iceberg's reading and writing libraries; never on the indexer, the proxy or the
// watch, and never on a storage client (the app registers one through IndexPaths.externalIo).
plugins {
    java
}

dependencies {
    // The SHADED Hadoop client, not hadoop-common: parquet-hadoop wants a Configuration and its
    // input-format classes at runtime and nothing here imports either, and every catalog measured
    // that carries Hadoop on a server runtime (Polaris, Unity Catalog) takes exactly this pair.
    // It relocates its own third-party dependencies inside itself, so it leaks no Guava 27 and no
    // commons-collections/jsch/reload4j into an embedder's classpath — which is the real cost,
    // not the megabytes.
    runtimeOnly("org.apache.hadoop:hadoop-client-api:3.4.1")
    runtimeOnly("org.apache.hadoop:hadoop-client-runtime:3.4.1")
    implementation(project(":common"))
    implementation(project(":analysis"))
    implementation("org.apache.iceberg:iceberg-api:1.11.0")
    implementation("org.apache.iceberg:iceberg-core:1.11.0")
    implementation("org.apache.iceberg:iceberg-data:1.11.0")
    implementation("org.apache.iceberg:iceberg-parquet:1.11.0")
    implementation("org.apache.parquet:parquet-column:1.17.1")
    implementation("org.roaringbitmap:RoaringBitmap:1.3.0")
    implementation("com.fasterxml.jackson.core:jackson-databind:2.21.3")
    implementation("org.slf4j:slf4j-api:2.0.13")
    runtimeOnly("com.github.luben:zstd-jni:1.5.6-4")
    // Test-only, and the one permitted backward edge: the round-trip tests build a real index
    // through the indexer module.
    testImplementation(project(":indexer"))
    testImplementation(testFixtures(project(":indexer")))
    testImplementation(testFixtures(project(":common")))
    testImplementation("org.junit.jupiter:junit-jupiter:5.10.2")
    testRuntimeOnly("org.junit.platform:junit-platform-launcher")
    testRuntimeOnly("org.slf4j:slf4j-simple:2.0.13")
}
