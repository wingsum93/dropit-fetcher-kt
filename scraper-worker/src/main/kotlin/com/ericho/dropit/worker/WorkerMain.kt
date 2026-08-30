package com.ericho.dropit.worker

import com.ericho.dropit.GroceryRepository
import com.ericho.dropit.model.DatabaseConfig
import com.ericho.dropit.model.adapter.PostgresqlStorage
import com.ericho.dropit.scraper.ProductScraper
import com.ericho.dropit.session.ApiFreshopTokenProvider
import com.ericho.dropit.session.FreshopSessionClient
import com.ericho.dropit.session.FreshopSessionConfig
import io.github.cdimascio.dotenv.dotenv
import kotlinx.coroutines.delay
import kotlinx.coroutines.runBlocking
import java.util.concurrent.atomic.AtomicBoolean

fun main() = runBlocking {
    val dotenv = dotenv {
        ignoreIfMissing = true
    }
    val readiness = WorkerReadiness()
    val healthHost = dotenv["WORKER_HEALTH_HOST"] ?: System.getenv("WORKER_HEALTH_HOST") ?: "0.0.0.0"
    val healthPort = (dotenv["WORKER_HEALTH_PORT"] ?: System.getenv("WORKER_HEALTH_PORT"))
        ?.toIntOrNull()
        ?: 8081
    val healthServer = startWorkerHealthServer(readiness, healthHost, healthPort)

    val storage = PostgresqlStorage(
        DatabaseConfig.fromEnv(dotenv)
            ?: error("Postgres configuration is required")
    )
    val running = AtomicBoolean(true)
    Runtime.getRuntime().addShutdownHook(
        Thread {
            running.set(false)
            readiness.markNotReady()
            storage.close()
        }
    )

    val pollIntervalMs = (dotenv["WORKER_POLL_INTERVAL_MS"] ?: System.getenv("WORKER_POLL_INTERVAL_MS"))
        ?.toLongOrNull()
        ?: 2_000L
    val batchSize = (dotenv["WORKER_BATCH_SIZE"] ?: System.getenv("WORKER_BATCH_SIZE"))
        ?.toIntOrNull()
        ?: 1
    val initMaxAttempts = (dotenv["WORKER_INIT_MAX_ATTEMPTS"] ?: System.getenv("WORKER_INIT_MAX_ATTEMPTS"))
        ?.toIntOrNull()
        ?: 5
    val initRetryDelayMs = (dotenv["WORKER_INIT_RETRY_DELAY_MS"] ?: System.getenv("WORKER_INIT_RETRY_DELAY_MS"))
        ?.toLongOrNull()
        ?: 3_000L

    try {
        val tokenProvider = ApiFreshopTokenProvider(
            client = FreshopSessionClient(),
            config = FreshopSessionConfig.fromEnv(dotenv)
        )
        try {
            initializeFreshopTokenWithRetry(
                tokenProvider = tokenProvider,
                maxAttempts = initMaxAttempts,
                retryDelayMs = initRetryDelayMs
            )
            val worker = ScraperWorker(
                storage = storage,
                scraper = ProductScraper(
                    repo = GroceryRepository(
                        tokenProvider = tokenProvider
                    ),
                    storage = storage
                ),
                pollIntervalMs = pollIntervalMs,
                batchSize = batchSize
            )
            readiness.markReady()
            worker.runUntilStopped(running)
        } finally {
            tokenProvider.close()
        }
    } finally {
        readiness.markNotReady()
        healthServer.stop(gracePeriodMillis = 1_000, timeoutMillis = 2_000)
        storage.close()
    }
}

internal suspend fun initializeFreshopTokenWithRetry(
    tokenProvider: ApiFreshopTokenProvider,
    maxAttempts: Int,
    retryDelayMs: Long
): String {
    require(maxAttempts > 0) {
        "WORKER_INIT_MAX_ATTEMPTS must be greater than 0"
    }
    require(retryDelayMs >= 0) {
        "WORKER_INIT_RETRY_DELAY_MS must be greater than or equal to 0"
    }

    var lastFailure: Throwable? = null
    repeat(maxAttempts) { attemptIndex ->
        val attempt = attemptIndex + 1
        try {
            val token = tokenProvider.initialize()
            println("event=worker_init_session_success attempt=$attempt")
            return token
        } catch (exception: Throwable) {
            lastFailure = exception
            System.err.println(
                "event=worker_init_session_error attempt=$attempt maxAttempts=$maxAttempts " +
                    "error=${exception::class.simpleName} message=${exception.message}"
            )
            if (attempt < maxAttempts) {
                delay(retryDelayMs)
            }
        }
    }

    throw IllegalStateException(
        "Failed to initialize Freshop session after $maxAttempts attempts",
        lastFailure
    )
}
