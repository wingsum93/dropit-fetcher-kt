package com.ericho.dropit.session

import com.ericho.dropit.AppSetting
import io.github.cdimascio.dotenv.Dotenv
import io.ktor.client.HttpClient
import io.ktor.client.call.body
import io.ktor.client.engine.cio.CIO
import io.ktor.client.plugins.HttpTimeout
import io.ktor.client.plugins.contentnegotiation.ContentNegotiation
import io.ktor.client.plugins.defaultRequest
import io.ktor.client.request.accept
import io.ktor.client.request.forms.submitForm
import io.ktor.http.ContentType
import io.ktor.http.parameters
import io.ktor.serialization.kotlinx.json.json
import kotlinx.serialization.SerialName
import kotlinx.serialization.Serializable
import kotlinx.serialization.json.Json
import java.io.Closeable

@Serializable
data class FreshopSessionResponse(
    val token: String,
    @SerialName("store_id")
    val storeId: String,
    @SerialName("created_at")
    val createdAt: String,
    val locale: String
)

data class FreshopSessionConfig(
    val appKey: String = AppSetting.appKey,
    val locale: String = DEFAULT_LOCALE,
    val referrer: String = DEFAULT_REFERRER
) {
    companion object {
        const val DEFAULT_LOCALE = "false"
        const val DEFAULT_REFERRER = "https://www.dropit.bm/"

        fun fromEnv(dotenv: Dotenv): FreshopSessionConfig {
            return FreshopSessionConfig(
                appKey = readEnv(dotenv, "FRESHOP_APP_KEY") ?: AppSetting.appKey,
                locale = readEnv(dotenv, "FRESHOP_LOCALE") ?: DEFAULT_LOCALE,
                referrer = readEnv(dotenv, "FRESHOP_REFERRER") ?: DEFAULT_REFERRER
            )
        }

        private fun readEnv(dotenv: Dotenv, key: String): String? {
            return (dotenv[key] ?: System.getenv(key))?.takeIf { it.isNotBlank() }
        }
    }
}

fun interface FreshopTokenProvider {
    suspend fun token(): String
}

class StaticFreshopTokenProvider(
    private val token: String
) : FreshopTokenProvider {
    override suspend fun token(): String = token
}

class FreshopSessionClient private constructor(
    private val httpClient: HttpClient,
    private val endpoint: String,
    private val clockMillis: () -> Long
) : Closeable {
    constructor() : this(
        httpClient = defaultHttpClient(),
        endpoint = DEFAULT_ENDPOINT,
        clockMillis = { System.currentTimeMillis() }
    )

    internal constructor(
        httpClient: HttpClient,
        endpoint: String,
        clockMillis: () -> Long,
        @Suppress("UNUSED_PARAMETER")
        testConstructor: Unit
    ) : this(
        httpClient = httpClient,
        endpoint = endpoint,
        clockMillis = clockMillis
    )

    suspend fun createSession(config: FreshopSessionConfig): FreshopSessionResponse {
        val response = httpClient.submitForm(
            url = endpoint,
            formParameters = createSessionFormParameters(config, clockMillis())
        ).body<FreshopSessionResponse>()

        require(response.token.isNotBlank()) {
            "Freshop session token is blank"
        }
        return response
    }

    override fun close() {
        httpClient.close()
    }

    companion object {
        const val DEFAULT_ENDPOINT = "https://api.freshop.ncrcloud.com/2/sessions/create"

        internal fun createSessionFormParameters(config: FreshopSessionConfig, utcMillis: Long) = parameters {
            append("app_key", config.appKey)
            append("locale", config.locale)
            append("referrer", config.referrer)
            append("utc", utcMillis.toString())
        }

        private fun defaultHttpClient(): HttpClient {
            return HttpClient(CIO) {
                install(ContentNegotiation) {
                    json(
                        Json {
                            ignoreUnknownKeys = true
                            isLenient = true
                        }
                    )
                }
                install(HttpTimeout) {
                    requestTimeoutMillis = 30_000
                    connectTimeoutMillis = 10_000
                    socketTimeoutMillis = 20_000
                }
                expectSuccess = true
                defaultRequest {
                    accept(ContentType.Application.Json)
                }
            }
        }
    }
}
