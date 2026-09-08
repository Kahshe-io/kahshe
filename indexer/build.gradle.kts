// kahshe-indexer: which tables, which columns, when, under what budget and lease -- the read pass
// over data files and the orchestration around the format's writers. Reaches its catalog only
// through TableSource; depends on format and common, never on the proxy or the watch.
plugins {
    java
    `java-test-fixtures`
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
    implementation(project(":format"))
    implementation("org.apache.iceberg:iceberg-api:1.11.0")
    implementation("org.apache.iceberg:iceberg-core:1.11.0")
    implementation("org.apache.iceberg:iceberg-data:1.11.0")
    implementation("org.apache.iceberg:iceberg-parquet:1.11.0")
    implementation("org.apache.parquet:parquet-column:1.17.1")
    implementation("org.roaringbitmap:RoaringBitmap:1.3.0")
    implementation("com.fasterxml.jackson.core:jackson-databind:2.21.3")
    implementation("org.slf4j:slf4j-api:2.0.13")
    runtimeOnly("com.github.luben:zstd-jni:1.5.6-4")
    // LocalTableFixture writes Parquet tables the way a table writer would, on a local Hadoop
    // filesystem; RecordingListener records the build hook. Both are lent to every module's tests.
    testFixturesImplementation(project(":common"))
    testFixturesImplementation(project(":analysis"))
    testFixturesImplementation(project(":format"))
    testFixturesImplementation(testFixtures(project(":common")))
    testFixturesImplementation("org.apache.iceberg:iceberg-api:1.11.0")
    testFixturesImplementation("org.apache.iceberg:iceberg-core:1.11.0")
    testFixturesImplementation("org.apache.iceberg:iceberg-data:1.11.0")
    testFixturesImplementation("org.apache.iceberg:iceberg-parquet:1.11.0")
    testFixturesImplementation("org.apache.parquet:parquet-column:1.17.1")
    // Test only: writing a Parquet file with NO field ids, which is what add_files and a Hive
    // migrate leave behind and what the refusal exists for. Iceberg's own writers always id
    // their columns, so such a file cannot be produced through them. hadoop-client-api is here
    // only so javac can resolve ExampleParquetWriter.builder's overloads; the test calls the
    // OutputFile one. Both are already on the test runtime classpath, so neither adds a jar.
    testImplementation("org.apache.parquet:parquet-hadoop:1.17.1")
    testImplementation("org.apache.hadoop:hadoop-client-api:3.4.1")
    testFixturesImplementation("org.apache.hadoop:hadoop-common:3.4.1") {
        exclude(group = "org.apache.zookeeper")
        exclude(group = "org.apache.curator")
        exclude(group = "org.apache.kerby")
        exclude(group = "com.sun.jersey")
        exclude(group = "org.eclipse.jetty")
        exclude(group = "io.netty")
    }
    testImplementation(testFixtures(project(":common")))
    testImplementation("org.junit.jupiter:junit-jupiter:5.10.2")
    testImplementation("org.mockito:mockito-core:5.20.0")
    testRuntimeOnly("org.junit.platform:junit-platform-launcher")
    testRuntimeOnly("org.slf4j:slf4j-simple:2.0.13")
}
