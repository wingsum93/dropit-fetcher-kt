package com.ericho.dropit

import com.ericho.dropit.model.FetchOptions
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
import com.ericho.dropit.model.api.DepartmentPayload
import com.ericho.dropit.model.api.ProductPayload
import com.ericho.dropit.model.SingleProductPayload
import com.ericho.dropit.model.api.DepartmentDto
import com.ericho.dropit.model.api.ProductDto

class GroceryRepository {
    private val URL_PRODUCT = "https://api.freshop.ncrcloud.com/1/products"
    private val URL_PRODUCT_DETAIL = "https://api.freshop.ncrcloud.com/1/products/"
    private val PAGE_SIZE = 96
    private val httpClient = HttpClient(CIO) {
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
        install(Logging) {
            level = LogLevel.ALL
        }
        defaultRequest {
            url("apiBaseUrl")
            header(HttpHeaders.Authorization, "Bearer abc")
            accept(ContentType.Application.Json)
        }
    }

    suspend fun fetchUrlAsJson(url: String): String {
        return httpClient.get(url).bodyAsText()
    }

    suspend fun getAllDepartments(
        storeId: Int = AppSetting.storeId7442,
    ): List<DepartmentDto> {
        return httpClient.get(URL_PRODUCT) {
            url {
                parameters.append("app_key", AppSetting.appKey)
                parameters.append("store_id", storeId.toString())

                parameters.append("include_departments", true.toString())
                parameters.append("token", AppSetting.sampleToken)
                parameters.append("render_id", "1769356302366")
            }
        }.body<DepartmentPayload>().departments
    }

    // limit to 96 items
    suspend fun getProductsFromDepartment(
        departmentId: Int,
        pageNo: Int = 0,
        storeId: Int = AppSetting.storeId7442,
    ): ProductPayload {

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

        return httpClient.get(URL_PRODUCT) {
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
                parameter("fields", fields)

                if (pageNo > 0) {
                    parameters.append("skip", (pageNo * PAGE_SIZE).toString())
                }
            }
        }.body()
    }

    suspend fun getAllItemsInDepartment(departmentId: Int, fetchOptions: FetchOptions): List<ProductDto> {
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

    suspend fun getItemDetail(
        itemId: Long
    ): SingleProductPayload {
        return httpClient.get {
            url {
                takeFrom(URL_PRODUCT_DETAIL)
                appendPathSegments(itemId.toString())
                parameters.append("app_key", AppSetting.appKey)
            }
        }.body()
    }
}
