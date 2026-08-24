package com.ericho.dropit.server

import com.ericho.dropit.model.DatabaseConfig
import com.ericho.dropit.model.adapter.PostgresqlStorage
import com.ericho.dropit.scraper.ProductScraper
import io.github.cdimascio.dotenv.dotenv
import io.ktor.server.cio.CIO
import io.ktor.server.application.ApplicationStopping
import io.ktor.server.engine.embeddedServer

fun main() {
    val dotenv = dotenv {
        ignoreIfMissing = true
    }
    val storage = PostgresqlStorage(
        DatabaseConfig.fromEnv(dotenv)
            ?: error("Postgres configuration is required")
    )
    val host = dotenv["SERVER_HOST"] ?: System.getenv("SERVER_HOST") ?: "0.0.0.0"
    val port = (dotenv["SERVER_PORT"] ?: System.getenv("SERVER_PORT"))?.toIntOrNull() ?: 8080

    embeddedServer(CIO, host = host, port = port) {
        configureScrapeApi(
            storage = storage,
            scraper = ProductScraper(storage = storage)
        )
        environment.monitor.subscribe(ApplicationStopping) {
            storage.close()
        }
    }.start(wait = true)
}
