// ANALYSIS, as Lucene and Elasticsearch use the word: what a canonical value is, and what a token
// is. The one thing every component and every foreign implementation must agree on, kept separate
// from the artifact that stores it.
//
// It depends on nothing but the JDK and a logging facade: the type system a canonical form is
// taken against is ValueKind, this module's own small vocabulary, and the binding from a table
// format's types to it is one function per format, outside here. That is the point — a
// row-scan-only watcher, or a second implementation in another language reading this as a
// specification, needs these rules and not the index.
dependencies {
    implementation("org.slf4j:slf4j-api:2.0.13")
    testImplementation("org.junit.jupiter:junit-jupiter:5.10.2")
    testRuntimeOnly("org.junit.platform:junit-platform-launcher")
    testRuntimeOnly("org.slf4j:slf4j-simple:2.0.13")
}
