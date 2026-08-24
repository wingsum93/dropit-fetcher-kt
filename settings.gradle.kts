plugins {
    id("org.gradle.toolchains.foojay-resolver-convention") version "0.8.0"
}
rootProject.name = "dropit-fetcher-kt"
include("scraper-core")
include("server")
include("scraper-worker")
