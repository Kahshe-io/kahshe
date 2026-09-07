// kahshe-watch: alerting -- rules, discovery, webhook delivery. Rides the indexer's build hook
// and reads the format's vocabulary; depends on indexer, format and common; never on the proxy.
plugins {
    java
}

dependencies {
    implementation(project(":common"))
    implementation(project(":analysis"))
    implementation(project(":format"))
    implementation(project(":indexer"))
    implementation("org.apache.iceberg:iceberg-api:1.11.0")
    implementation("org.apache.iceberg:iceberg-core:1.11.0")
    // RuleScanner reads the columns a rule names out of each new data file: the generic record
    // reader and the Parquet entry point, the same two the indexer's read pass uses.
    implementation("org.apache.iceberg:iceberg-data:1.11.0")
    implementation("org.apache.iceberg:iceberg-parquet:1.11.0")
    implementation("org.apache.parquet:parquet-column:1.17.1")
    implementation("com.fasterxml.jackson.core:jackson-databind:2.21.3")
    implementation("com.fasterxml.jackson.dataformat:jackson-dataformat-yaml:2.21.3")
    implementation("org.slf4j:slf4j-api:2.0.13")
    testImplementation(testFixtures(project(":common")))
    testImplementation(testFixtures(project(":indexer")))
    testImplementation("org.junit.jupiter:junit-jupiter:5.10.2")
    testImplementation("org.mockito:mockito-core:5.20.0")
    testRuntimeOnly("org.junit.platform:junit-platform-launcher")
    testRuntimeOnly("org.slf4j:slf4j-simple:2.0.13")
}

// SigmaGeneratedRulesTest loads the rules files the pySigma backend writes, which live outside
// this module. Without declaring them Gradle sees no changed input, skips the task and reports
// the previous run — the same way a doc check that never re-runs reads as passing. Found by a
// red check that did not bite.
tasks.test {
    inputs.files(fileTree(rootDir) { include("sigma/tests/generated/*.yaml") })
        .withPropertyName("sigmaGeneratedRules")
        .withPathSensitivity(PathSensitivity.RELATIVE)
}
