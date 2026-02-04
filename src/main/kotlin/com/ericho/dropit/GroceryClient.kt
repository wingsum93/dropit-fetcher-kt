package com.ericho.dropit

import io.ktor.client.*
import io.ktor.client.call.*
import io.ktor.client.engine.cio.*
import io.ktor.client.plugins.*
import io.ktor.client.plugins.contentnegotiation.*
import io.ktor.client.plugins.logging.LogLevel
import io.ktor.client.plugins.logging.Logging
import io.ktor.client.request.*
import io.ktor.client.statement.*
import io.ktor.http.*
import io.ktor.serialization.kotlinx.json.*
import kotlinx.serialization.json.Json

class GroceryClient {
    private val URL_PRODUCT = "https://api.freshop.ncrcloud.com/1/products"
    private val URL_PRODUCT_DETAIL = "https://api.freshop.ncrcloud.com/1/products/"
    private val http = HttpClient(CIO) {
        install(ContentNegotiation) {
            json(
                Json {
                    ignoreUnknownKeys = true // ✅ API schema change tolerant
                    isLenient = true
                }
            )
        }
        install(HttpTimeout) {
            requestTimeoutMillis = 20_000
            connectTimeoutMillis = 10_000
            socketTimeoutMillis = 20_000
        }
        install(HttpRequestRetry) {
            maxRetries = 3
            retryIf { _, response -> response.status.value in setOf(429, 500, 502, 503, 504) }
            exponentialDelay()
        }
        install(Logging){
            level = LogLevel.ALL
        }
        defaultRequest {
            url("apiBaseUrl")
            header(HttpHeaders.Authorization, "Bearer abc")
            accept(ContentType.Application.Json)
        }
    }

    suspend fun fetchSnapshots(): List<ProductSnapshot> {
        // TODO: change path + response model to your real API
        // Example endpoint: GET /v1/products/prices
        return http.get("/v1/products/prices")
            .body()
    }

    suspend fun fetchUrlAsJson(url: String): String {
        return http.get(url).bodyAsText()
    }

    suspend fun fetchProductsFromDepartment(
        departmentId: Int,
        storeId: Int = AppSetting.storeId7442,
    ): String {

        val fields = listOf("id","store_id","department_id","status", "product_name","path","count","parent_id","canonical_url").joinToString(",")

        return http.get(URL_PRODUCT) {
            url {
                parameters.append("app_key", AppSetting.appKey)
                parameters.append("store_id", storeId.toString())
                parameters.append("department_id", departmentId.toString())
                parameters.append("include_departments", true.toString())
                parameters.append("token", AppSetting.sampleToken)
                parameters.append("render_id", "1769356302366")
                // can add skip param
                parameters.append("popularity_sort", "asc")
                parameters.append("limit", (96).toString())
                parameters.append("department_id_cascade", true.toString())
                parameter("fields",fields)
            }
        }.bodyAsText()
    }

    suspend fun fetchProductDetailAsJson(
        productId: Long
    ): String {
        return http.get {
            url {
                takeFrom(URL_PRODUCT_DETAIL)
                appendPathSegments(productId.toString())
                parameters.append("app_key", AppSetting.appKey)
            }
        }.bodyAsText()
    }
}