// The root builds nothing of its own: the modules are the subprojects, the app is `app`, and
// this file holds what every module shares plus the
// Trino overlay's type-check and tests, which are not a module of kahshe.
plugins {
    java
}

allprojects {
    group = "io.kahshe"
    version = "0.1.0-SNAPSHOT"
    repositories {
        mavenCentral()
    }
}

// The rules every module builds under: the 17 toolchain, JUnit 5, javadoc as part of check with
// the same doclint the root has always run.
subprojects {
    apply(plugin = "java")
    // the module is the artifact: kahshe-common, kahshe-format, kahshe-indexer, kahshe-proxy,
    // kahshe-watch, kahshe-app (none published; the name is what they would publish under)
    extensions.configure<BasePluginExtension> {
        archivesName.set("kahshe-${project.name}")
    }
    extensions.configure<JavaPluginExtension> {
        toolchain {
            languageVersion = JavaLanguageVersion.of(17)
        }
    }
    tasks.withType<Test>().configureEach {
        useJUnitPlatform()
        // the conformance fixture is regenerated on request only (format module)
        systemProperty("kahshe.conformance.regenerate", System.getProperty("kahshe.conformance.regenerate", "false"))
    }
    tasks.withType<Javadoc>().configureEach {
        (options as StandardJavadocDocletOptions).apply {
            memberLevel = JavadocMemberLevel.PACKAGE
            addStringOption("Xdoclint:all,-missing", "-quiet")
        }
    }
    tasks.named("check") {
        dependsOn("javadoc")
    }
}

java {
    toolchain {
        languageVersion = JavaLanguageVersion.of(17)
    }
}

val trinoPatch: Configuration by configurations.creating

dependencies {
    trinoPatch("io.trino:trino-iceberg:483")
}

// Trino 483's own class files are built for a JDK far newer than kahshe targets, so this one
// task needs its own compiler. kahshe itself stays on 17.
private val trinoJdk = 25

val compileTrinoPatch by tasks.registering(JavaCompile::class) {
    description = "Type-checks the overlaid Trino classes against Trino 483."
    group = "verification"
    source = fileTree("dev/trino-patch") { include("*.java") }
    classpath = trinoPatch
    destinationDirectory = layout.buildDirectory.dir("classes/dev/trino-patch")
    javaCompiler = javaToolchains.compilerFor {
        languageVersion = JavaLanguageVersion.of(trinoJdk)
    }
    options.compilerArgs.addAll(listOf("-nowarn", "-proc:none"))
}

// The overlay's own tests. `dev/trino-patch` had NO harness, which is why the most dangerous fix in
// this tree carries the note "No test" in §3.1: the recogniser accepting a bare column as well as
// lower(col) was a false negative -- the analyzer lower-cases before it tokenises, so the two are
// not equivalent -- and nothing anywhere asserted that the bare form stays unrecognised. These
// tests run against the real Trino 483 artifacts already resolved above, no Docker and no cluster.
val trinoPatchTest: Configuration by configurations.creating {
    extendsFrom(trinoPatch)
}

dependencies {
    trinoPatchTest("org.junit.jupiter:junit-jupiter:5.11.3")
    trinoPatchTest("org.junit.platform:junit-platform-launcher:1.11.3")
}

val compileTrinoPatchTest by tasks.registering(JavaCompile::class) {
    description = "Compiles the overlay's tests against Trino 483."
    source = fileTree("dev/trino-patch/test") { include("*.java") }
    // Overlay classes FIRST. trinoPatchTest extends trinoPatch, which carries the stock
    // trino-iceberg jar, and its IcebergSplitSource would otherwise shadow the overlaid one --
    // the test would compile against the class this patch replaces and silently test nothing.
    classpath = files(compileTrinoPatch.map { it.destinationDirectory }) + trinoPatchTest
    destinationDirectory = layout.buildDirectory.dir("classes/dev/trino-patch-test")
    javaCompiler = javaToolchains.compilerFor {
        languageVersion = JavaLanguageVersion.of(trinoJdk)
    }
    options.compilerArgs.addAll(listOf("-nowarn", "-proc:none"))
}

val trinoPatchTestTask by tasks.registering(Test::class) {
    description = "Runs the overlaid Trino classes' own tests."
    group = "verification"
    testClassesDirs = files(compileTrinoPatchTest.map { it.destinationDirectory })
    classpath = files(compileTrinoPatch.map { it.destinationDirectory }) +
        files(compileTrinoPatchTest.map { it.destinationDirectory }) +
        trinoPatchTest
    useJUnitPlatform()
    javaLauncher = javaToolchains.launcherFor {
        languageVersion = JavaLanguageVersion.of(trinoJdk)
    }
    testLogging { exceptionFormat = org.gradle.api.tasks.testing.logging.TestExceptionFormat.FULL }
}

tasks.check {
    dependsOn(compileTrinoPatch)
    dependsOn(trinoPatchTestTask)
}
