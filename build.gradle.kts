plugins {
    kotlin("jvm") version "2.1.10"
    kotlin("plugin.serialization") version "2.1.10"
    application
    id("com.github.johnrengelman.shadow") version "8.1.1"
}

group = "org.ericho.dropit-fetcher"
version = "1.0-SNAPSHOT"

repositories {
    mavenCentral()
}

application {
    mainClass.set("com.ericho.dropit.MainKt")
}

tasks {
    // Build a runnable fat jar for Docker.
    jar {
        enabled = false
    }
    shadowJar {
        archiveClassifier.set("")
        manifest {
            attributes("Main-Class" to application.mainClass.get())
        }
    }
    build {
        dependsOn(shadowJar)
    }
    // Declare explicit dependencies on shadowJar
    distZip {
        dependsOn(shadowJar)
    }
    distTar {
        dependsOn(shadowJar)
    }
    startScripts {
        dependsOn(shadowJar)
    }
}

dependencies {
    val jooqVersion = "3.19.15"

    // Ktor client (CIO engine)
    implementation("io.ktor:ktor-client-core:2.3.12")
    implementation("io.ktor:ktor-client-cio:2.3.12")

    // JSON serialization
    implementation("io.ktor:ktor-client-content-negotiation:2.3.12")
    implementation("io.ktor:ktor-serialization-kotlinx-json:2.3.12")
    implementation("io.ktor:ktor-client-logging:2.3.12")
    implementation("org.jetbrains.kotlinx:kotlinx-serialization-json:1.7.3")

    // Logging (simple, CLI-friendly)
    implementation("ch.qos.logback:logback-classic:1.5.16")

    // Parse env / .env
    implementation("io.github.cdimascio:dotenv-kotlin:6.4.2")
    implementation("org.postgresql:postgresql:42.7.3")
    implementation("org.xerial:sqlite-jdbc:3.46.1.0")
    implementation("com.zaxxer:HikariCP:5.1.0")
    implementation("org.jooq:jooq:$jooqVersion")
    implementation("org.jooq:jooq-kotlin:$jooqVersion")
    testImplementation(kotlin("test"))
}

tasks.test {
    useJUnitPlatform()
}
