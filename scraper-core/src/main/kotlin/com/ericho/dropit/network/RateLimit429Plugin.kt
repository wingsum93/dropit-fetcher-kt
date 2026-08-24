package com.ericho.dropit.network

import io.ktor.client.*
import io.ktor.client.call.*
import io.ktor.client.plugins.*
import io.ktor.client.request.*
import io.ktor.client.statement.*
import io.ktor.http.*
import io.ktor.util.*
import io.ktor.utils.io.cancel
import kotlinx.coroutines.delay
import kotlin.math.min
import kotlin.random.Random
import kotlin.time.Duration
import kotlin.time.Duration.Companion.milliseconds
import kotlin.time.Duration.Companion.seconds
import java.time.ZonedDateTime
import java.time.format.DateTimeFormatter
import java.time.format.DateTimeParseException

/**
 * Interceptor-style plugin:
 * - If response is 429, wait then retry
 * - Uses Retry-After header if present, else backoff
 */
class RateLimit429Plugin private constructor(
    private val maxRetries: Int,
    private val baseDelay: Duration,
    private val maxDelay: Duration,
    private val jitterRatio: Double,
    private val respectRetryAfter: Boolean,
    private val rateLimitStatusCodes: Set<Int>,
    private val backoffMultiplier: Double
) {

    class Config {
        var maxRetries: Int = 8
        var baseDelay: Duration = 500.milliseconds
        var maxDelay: Duration = 30.seconds
        var jitterRatio: Double = 0.2
        var respectRetryAfter: Boolean = true
        /**
         * Next-delay growth factor after each retry.
         * - 2.0 (default): exponential backoff
         * - 1.0: fixed delay (useful for known fixed rate limits)
         */
        var backoffMultiplier: Double = 2.0
        // Include 400 since this API signals rate limit with Bad Request
        var rateLimitStatusCodes: Set<Int> = setOf(
            HttpStatusCode.TooManyRequests.value,
            HttpStatusCode.BadRequest.value
        )
    }

    companion object : HttpClientPlugin<Config, RateLimit429Plugin> {
        override val key: AttributeKey<RateLimit429Plugin> = AttributeKey("RateLimit429Plugin")

        override fun prepare(block: Config.() -> Unit): RateLimit429Plugin {
            val cfg = Config().apply(block)
            require(cfg.maxRetries >= 0) { "maxRetries must be >= 0" }
            require(cfg.jitterRatio in 0.0..1.0) { "jitterRatio must be within 0..1" }
            require(cfg.backoffMultiplier >= 1.0) { "backoffMultiplier must be >= 1.0" }
            require(cfg.rateLimitStatusCodes.isNotEmpty()) { "rateLimitStatusCodes must not be empty" }
            return RateLimit429Plugin(
                maxRetries = cfg.maxRetries,
                baseDelay = cfg.baseDelay,
                maxDelay = cfg.maxDelay,
                jitterRatio = cfg.jitterRatio,
                respectRetryAfter = cfg.respectRetryAfter,
                rateLimitStatusCodes = cfg.rateLimitStatusCodes,
                backoffMultiplier = cfg.backoffMultiplier
            )
        }

        override fun install(plugin: RateLimit429Plugin, scope: HttpClient) {
            // Intercept sending of requests
            scope.sendPipeline.intercept(HttpSendPipeline.Monitoring) { request ->
                var attempt = 0
                var currentDelay = plugin.baseDelay

                while (true) {
                    val result = proceedWith(request) // send request
                    val response = when (result) {
                        is HttpClientCall -> result.response
                        is HttpResponse -> result
                        else -> return@intercept
                    }

                    if (response.status.value !in plugin.rateLimitStatusCodes) {
                        return@intercept // success or other status, let it pass
                    }

                    logRateLimitHeaders(response, attempt)

                    // 429 handling
                    if (attempt >= plugin.maxRetries) {
                        return@intercept // give up, caller handles 429
                    }

                    // Make sure we release response resources (important!)
                    response.bodyAsChannel().cancel()

                    val waitMs = plugin.computeWaitMs(response, currentDelay)
                    delay(waitMs)

                    // exponential backoff for next round (even if Retry-After exists, still keep a fallback growth)
                    currentDelay = minOf(currentDelay * plugin.backoffMultiplier, plugin.maxDelay)
                    attempt++
                    // loop retries
                }
            }
        }

        private fun logRateLimitHeaders(response: HttpResponse, attempt: Int) {
            val headers = response.headers.entries()
                .joinToString(separator = ", ") { (key, values) -> "$key=${values.joinToString("|")}" }
            System.err.println("HTTP ${response.status.value} received (attempt=${attempt + 1}); headers=[$headers]")
        }
    }



    private fun computeWaitMs(response: HttpResponse, fallbackDelay: Duration): Long {
        val retryAfterMs = if (respectRetryAfter) parseRetryAfterMs(response) else null
        val base = retryAfterMs ?: fallbackDelay.inWholeMilliseconds

        // jitter: random in [base*(1-j), base*(1+j)]
        val j = jitterRatio
        val low = (base * (1.0 - j)).toLong().coerceAtLeast(0)
        val high = (base * (1.0 + j)).toLong().coerceAtLeast(low + 1)
        return Random.nextLong(low, high)
    }

    private fun parseRetryAfterMs(response: HttpResponse): Long? {
        val raw = response.headers[HttpHeaders.RetryAfter]?.trim()?.takeIf { it.isNotEmpty() } ?: return null

        // Case 1: integer seconds
        raw.toLongOrNull()?.let { seconds ->
            return (seconds * 1000L).coerceAtLeast(0)
        }

        // Case 2: HTTP-date (RFC 7231 / IMF-fixdate)
        return try {
            val retryAt = ZonedDateTime.parse(raw, DateTimeFormatter.RFC_1123_DATE_TIME)
            val retryAtMs = retryAt.toInstant().toEpochMilli()
            val nowMs = System.currentTimeMillis()
            (retryAtMs - nowMs).coerceAtLeast(0)
        } catch (_: DateTimeParseException) {
            null
        } catch (_: Throwable) {
            null
        }
    }
}
