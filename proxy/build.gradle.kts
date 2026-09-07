// kahshe-proxy: the zero-engine-change deployment -- the Iceberg REST passthrough, the planning
// endpoints, the count route, the catalog clients. Depends on indexer (traffic drives
// maintenance), format (the pruner and the term reader) and common; never on the watch.
plugins {
    java
    `java-test-fixtures`
}

dependencies {
    // The SHADED Hadoop client, not hadoop-common: this module hands Iceberg a Configuration, so it
    // compiles against the API jar while the runtime jar carries the relocated dependencies, which
    // keeps Hadoop's transitive tree out of the classpath.
    implementation("org.apache.hadoop:hadoop-client-api:3.4.1")
    runtimeOnly("org.apache.hadoop:hadoop-client-runtime:3.4.1")
    implementation(project(":common"))
    implementation(project(":analysis"))
    implementation(project(":format"))
    implementation(project(":indexer"))
    implementation("org.apache.iceberg:iceberg-api:1.11.0")
    implementation("org.apache.iceberg:iceberg-core:1.11.0")
    implementation("com.fasterxml.jackson.core:jackson-databind:2.21.3")
    implementation("org.roaringbitmap:RoaringBitmap:1.3.0")
    implementation("org.slf4j:slf4j-api:2.0.13")
    // TestConfigs wires the real dispatcher over the indexer's table fixture
    testFixturesImplementation(project(":common"))
    testFixturesImplementation(project(":format"))
    testFixturesImplementation(project(":indexer"))
    testFixturesImplementation(testFixtures(project(":common")))
    testFixturesImplementation(testFixtures(project(":indexer")))
    testImplementation(testFixtures(project(":common")))
    testImplementation(testFixtures(project(":indexer")))
    testImplementation("org.junit.jupiter:junit-jupiter:5.10.2")
    testImplementation("org.mockito:mockito-core:5.20.0")
    testRuntimeOnly("org.junit.platform:junit-platform-launcher")
    testRuntimeOnly("org.slf4j:slf4j-simple:2.0.13")
}
