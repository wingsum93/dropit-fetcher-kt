plugins {
    kotlin("jvm")
    kotlin("plugin.serialization")
    application
    id("com.github.johnrengelman.shadow")
}

application {
    mainClass.set("com.ericho.dropit.worker.WorkerMainKt")
}

dependencies {
    val ktorVersion = "2.3.12"

    implementation(project(":scraper-core"))
    implementation("org.jetbrains.kotlinx:kotlinx-coroutines-core:1.8.1")
    implementation("io.ktor:ktor-server-core:$ktorVersion")
    implementation("io.ktor:ktor-server-cio:$ktorVersion")
    implementation("io.github.cdimascio:dotenv-kotlin:6.4.2")
    implementation("ch.qos.logback:logback-classic:1.5.16")
    testImplementation(kotlin("test"))
    testImplementation("org.jetbrains.kotlinx:kotlinx-coroutines-test:1.8.1")
    testImplementation("io.ktor:ktor-server-test-host:$ktorVersion")
}

tasks {
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
    distZip {
        dependsOn(shadowJar)
    }
    distTar {
        dependsOn(shadowJar)
    }
    startScripts {
        dependsOn(shadowJar)
    }
    test {
        useJUnitPlatform()
    }
}
