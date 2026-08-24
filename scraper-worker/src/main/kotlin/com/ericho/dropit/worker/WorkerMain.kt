package com.ericho.dropit.worker

import com.ericho.dropit.model.DatabaseConfig
import com.ericho.dropit.model.adapter.PostgresqlStorage
import com.ericho.dropit.scraper.ProductScraper
import io.github.cdimascio.dotenv.dotenv
import kotlinx.coroutines.runBlocking
import java.util.concurrent.atomic.AtomicBoolean

fun main() = runBlocking {
    val dotenv = dotenv {
        ignoreIfMissing = true
    }
    val storage = PostgresqlStorage(
        DatabaseConfig.fromEnv(dotenv)
            ?: error("Postgres configuration is required")
    )
    val running = AtomicBoolean(true)
    Runtime.getRuntime().addShutdownHook(
        Thread {
            running.set(false)
            storage.close()
        }
    )

    val pollIntervalMs = (dotenv["WORKER_POLL_INTERVAL_MS"] ?: System.getenv("WORKER_POLL_INTERVAL_MS"))
        ?.toLongOrNull()
        ?: 2_000L
    val batchSize = (dotenv["WORKER_BATCH_SIZE"] ?: System.getenv("WORKER_BATCH_SIZE"))
        ?.toIntOrNull()
        ?: 1

    val worker = ScraperWorker(
        storage = storage,
        scraper = ProductScraper(storage = storage),
        pollIntervalMs = pollIntervalMs,
        batchSize = batchSize
    )

    try {
        worker.runUntilStopped(running)
    } finally {
        storage.close()
    }
}
