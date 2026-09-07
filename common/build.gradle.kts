// kahshe-common: what every role shares and nothing Iceberg-shaped -- the metrics registry, the
// two caches, single-flight loading and the digest. No dependency on any other kahshe module,
// by construction.
plugins {
    java
    `java-test-fixtures`
}

dependencies {
    implementation("com.github.ben-manes.caffeine:caffeine:2.9.3")
    implementation("org.slf4j:slf4j-api:2.0.13")
    testImplementation("org.junit.jupiter:junit-jupiter:5.10.2")
    testRuntimeOnly("org.junit.platform:junit-platform-launcher")
}
