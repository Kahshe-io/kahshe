rootProject.name = "kahshe"

// One subproject per role or layer. Each lands as its step
// does; the app is the `app` subproject; the root holds the shared rules and the Trino overlay.
include("common")
// what a value MEANS -- canonical form and tokens -- below the artifact that stores it
// (section 0 item 23). "analysis" as Lucene and Elasticsearch use it, because "vocabulary"
// already means the term dictionary's contents in this project.
include("analysis")
include("format")
include("indexer")
include("proxy")
include("watch")
include("app")
