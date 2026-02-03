package com.ericho.dropit

import io.ktor.client.*
import io.ktor.client.call.*
import io.ktor.client.engine.cio.*
import io.ktor.client.plugins.*
import io.ktor.client.plugins.contentnegotiation.*
import io.ktor.client.request.*
import io.ktor.client.statement.*
import io.ktor.http.*
import io.ktor.serialization.kotlinx.json.*
import kotlinx.serialization.json.Json

class GroceryClient {
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

    suspend fun fetchUrlAsJson(url:String):String{
        return http.get(url).bodyAsText()
    }
}