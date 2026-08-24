plugins {
    kotlin("jvm") version "2.1.10" apply false
    kotlin("plugin.serialization") version "2.1.10" apply false
    id("com.github.johnrengelman.shadow") version "8.1.1" apply false
}

allprojects {
    group = "org.ericho.dropit-fetcher"
    version = "1.0-SNAPSHOT"

    repositories {
        mavenCentral()
    }
}
