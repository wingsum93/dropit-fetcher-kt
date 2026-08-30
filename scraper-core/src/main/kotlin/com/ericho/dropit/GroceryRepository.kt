package com.ericho.dropit

import com.ericho.dropit.model.FetchOptions
import com.ericho.dropit.model.SingleProductPayload
import com.ericho.dropit.model.api.DepartmentDto
import com.ericho.dropit.model.api.DepartmentPayload
import com.ericho.dropit.model.api.ProductDto
import com.ericho.dropit.model.api.ProductPayload
import com.ericho.dropit.network.RateLimit429Plugin
import com.ericho.dropit.session.FreshopTokenProvider
import com.ericho.dropit.session.StaticFreshopTokenProvider
import io.ktor.client.*
import io.ktor.client.call.*
import io.ktor.client.engine.cio.*
import io.ktor.client.plugins.*
import io.ktor.client.plugins.contentnegotiation.*
import io.ktor.client.request.*
import io.ktor.client.statement.*
import io.ktor.http.*
import io.ktor.serialization.kotlinx.json.*
import kotlinx.serialization.SerialName
import kotlinx.serialization.Serializable
import kotlinx.serialization.json.Json
import kotlin.time.Duration.Companion.milliseconds
import kotlin.time.Duration.Companion.seconds

class GroceryRepository private constructor(
    private val tokenProvider: FreshopTokenProvider,
    private val httpClient: HttpClient
) : GroceryDataSource {
    constructor(
        tokenProvider: FreshopTokenProvider = StaticFreshopTokenProvider(AppSetting.sampleToken)
    ) : this(tokenProvider, defaultHttpClient())

    internal constructor(
        tokenProvider: FreshopTokenProvider,
        httpClient: HttpClient,
        @Suppress("UNUSED_PARAMETER")
        testConstructor: Unit
    ) : this(tokenProvider, httpClient)

    suspend fun fetchUrlAsJson(url: String): String {
        return requestFreshopText {
            httpClient.get(url)
        }
    }

    override suspend fun getAllDepartments(
        storeId: Int,
    ): List<DepartmentDto> {
        val token = tokenProvider.token()
        return requestFreshop<DepartmentPayload> {
            httpClient.get(URL_PRODUCT) {
                url {
                    parameters.append("app_key", AppSetting.appKey)
                    parameters.append("store_id", storeId.toString())

                    parameters.append("include_departments", true.toString())
                    parameters.append("token", token)
                    parameters.append("render_id", "1769356302366")
                }
            }
        }.departments
    }

    // limit to 96 items
    suspend fun getProductsFromDepartment(
        departmentId: Int,
        pageNo: Int = 0,
        storeId: Int = AppSetting.storeId7442,
    ): ProductPayload {
        val token = tokenProvider.token()

        val fields = listOf(
            "id",
            "store_id",
            "department_id",
            "status",
            "product_name",
            "price",
            "unit_price",
            "popularity",
            "upc",
            "size",
            "cover_image",
            "path",
            "count",
            "parent_id",
            "canonical_url"
        ).joinToString(",")

        return requestFreshop {
            httpClient.get(URL_PRODUCT) {
                url {
                    parameters.append("app_key", AppSetting.appKey)
                    parameters.append("store_id", storeId.toString())
                    parameters.append("department_id", departmentId.toString())
                    parameters.append("include_departments", true.toString())
                    parameters.append("token", token)
                    parameters.append("render_id", "1769356302366")
                    // can add skip param
                    parameters.append("popularity_sort", "asc")
                    parameters.append("limit", (96).toString())
                    parameters.append("department_id_cascade", true.toString())
                    parameter("fields", fields)

                    if (pageNo > 0) {
                        parameters.append("skip", (pageNo * PAGE_SIZE).toString())
                    }
                }
            }
        }
    }

    override suspend fun getAllItemsInDepartment(departmentId: Int, fetchOptions: FetchOptions): List<ProductDto> {
        val tempPool = mutableListOf<ProductDto>()
        var pageNo = 0
        do {
            val payload = getProductsFromDepartment(departmentId, pageNo = pageNo)
            val items = payload.items
            tempPool.addAll(items)
            pageNo++
        } while (items.size == PAGE_SIZE)
        return tempPool
    }

    override suspend fun getItemDetail(
        itemId: Long
    ): SingleProductPayload {
        val token = tokenProvider.token()
        return requestFreshop {
            httpClient.get {
                url {
                    takeFrom(URL_PRODUCT_DETAIL)
                    appendPathSegments(itemId.toString())
                    parameters.append("app_key", AppSetting.appKey)
                    parameters.append("token", token)
                }
            }
        }
    }

    private suspend inline fun <reified T> requestFreshop(crossinline request: suspend () -> HttpResponse): T {
        val response = request()
        handleFreshopError(response)
        return response.body()
    }

    private suspend fun requestFreshopText(request: suspend () -> HttpResponse): String {
        val response = request()
        handleFreshopError(response)
        return response.bodyAsText()
    }

    private suspend fun handleFreshopError(response: HttpResponse) {
        when (response.status) {
            HttpStatusCode.BadRequest -> throw mapFreshopBadRequest(response.bodyAsText())
            HttpStatusCode.TooManyRequests -> throw RateLimitException("Freshop API rate limit exceeded")
        }
    }

    private fun mapFreshopBadRequest(body: String): AppException {
        val error = runCatching {
            json.decodeFromString<FreshopErrorPayload>(body)
        }.getOrNull()
        val message = error?.errorMessage?.takeIf { it.isNotBlank() }
            ?: body.takeIf { it.isNotBlank() }
            ?: "Freshop API returned HTTP 400"

        return when {
            error?.errorCode == "sign_out_required" -> TokenExpiredException(message)
            message.contains("429") -> RateLimitException(message)
            else -> DataInvalidateException(message)
        }
    }

    @Serializable
    private data class FreshopErrorPayload(
        @SerialName("error_code")
        val errorCode: String? = null,
        @SerialName("error_message")
        val errorMessage: String? = null
    )

    companion object {
        private const val URL_PRODUCT = "https://api.freshop.ncrcloud.com/1/products"
        private const val URL_PRODUCT_DETAIL = "https://api.freshop.ncrcloud.com/1/products/"
        private const val PAGE_SIZE = 96

        private val json = Json {
            ignoreUnknownKeys = true
            isLenient = true
        }

        private fun defaultHttpClient(): HttpClient {
            return HttpClient(CIO) {
                install(ContentNegotiation) {
                    json(json)
                }
                install(HttpTimeout) {
                    requestTimeoutMillis = 80_000
                    connectTimeoutMillis = 20_000
                    socketTimeoutMillis = 40_000
                }
                install(RateLimit429Plugin) {
                    maxRetries = 6
                    // API rate limit: ~1 request per 3 seconds. Keep retries aligned to that window.
                    baseDelay = 3100.milliseconds
                    maxDelay = 120.seconds
                    jitterRatio = 0.0
                    respectRetryAfter = true
                    backoffMultiplier = 1.0
                    rateLimitStatusCodes = setOf(HttpStatusCode.TooManyRequests.value)
                }
                install(HttpRequestRetry) {
                    maxRetries = 3
                    retryIf { _, response -> response.status.value in setOf(500, 502, 503, 504) }
                    exponentialDelay()
                }
                defaultRequest {
                    url("apiBaseUrl")
                    accept(ContentType.Application.Json)
                }
            }
        }
    }
}
