package com.ericho.dropit.scraper

import com.ericho.dropit.model.adapter.Storage
import kotlinx.coroutines.delay
import java.time.Instant

interface RateLimiter {
    suspend fun awaitPermit(name: String = DEFAULT_LIMITER_NAME)

    companion object {
        const val DEFAULT_LIMITER_NAME = "freshop-api"
    }
}

class StorageRateLimiter(
    private val storage: Storage,
    private val spacingMs: Long = 5_000L
) : RateLimiter {
    override suspend fun awaitPermit(name: String) {
        val reservedAt = storage.reserveRateLimitSlot(name, spacingMs)
        val waitMs = reservedAt.toEpochMilli() - Instant.now().toEpochMilli()
        if (waitMs > 0) {
            delay(waitMs)
        }
    }
}
