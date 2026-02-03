plugins {
    kotlin("jvm") version "2.1.10"
    kotlin("plugin.serialization") version "2.1.10"
    application
}

group = "org.ericho.dropit-fetcher"
version = "1.0-SNAPSHOT"

repositories {
    mavenCentral()
}

application {
    mainClass.set("com.ericho.dropit.MainKt")
}

dependencies {
    // Ktor client (CIO engine)
    implementation("io.ktor:ktor-client-core:2.3.12")
    implementation("io.ktor:ktor-client-cio:2.3.12")

    // JSON serialization
    implementation("io.ktor:ktor-client-content-negotiation:2.3.12")
    implementation("io.ktor:ktor-serialization-kotlinx-json:2.3.12")
    implementation("org.jetbrains.kotlinx:kotlinx-serialization-json:1.7.3")

    // Logging (simple, CLI-friendly)
    implementation("ch.qos.logback:logback-classic:1.5.16")

    // Optional: parse env / .env (if you want)
    // implementation("io.github.cdimascio:dotenv-kotlin:6.4.2")
    testImplementation(kotlin("test"))
}

tasks.test {
    useJUnitPlatform()
}
