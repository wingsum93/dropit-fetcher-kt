package com.ericho.dropit

import com.ericho.dropit.session.FreshopTokenProvider
import io.ktor.client.HttpClient
import io.ktor.client.engine.mock.MockEngine
import io.ktor.client.engine.mock.respond
import io.ktor.client.plugins.contentnegotiation.ContentNegotiation
import io.ktor.client.request.accept
import io.ktor.client.plugins.defaultRequest
import io.ktor.http.ContentType
import io.ktor.http.HttpHeaders
import io.ktor.http.HttpStatusCode
import io.ktor.http.headersOf
import io.ktor.serialization.kotlinx.json.json
import kotlinx.coroutines.runBlocking
import kotlinx.serialization.json.Json
import kotlin.test.AfterTest
import kotlin.test.Test
import kotlin.test.assertEquals
import kotlin.test.assertFailsWith

class GroceryRepositoryErrorMappingTest {
    private val clients = mutableListOf<HttpClient>()

    @AfterTest
    fun tearDown() {
        clients.forEach { it.close() }
        clients.clear()
    }

    @Test
    fun `getAllDepartments maps sign out required response to token expired exception`() = runBlocking {
        val repo = repositoryReturning(
            """
            {
              "error_code": "sign_out_required",
              "error_message": "Please log in again."
            }
            """.trimIndent()
        )

        val exception = assertFailsWith<TokenExpiredException> {
            repo.getAllDepartments()
        }

        assertEquals("Please log in again.", exception.message)
    }

    @Test
    fun `getProductsFromDepartment maps 429 message response to rate limit exception`() = runBlocking {
        val repo = repositoryReturning(
            """
            {
              "error_code": "bad_request",
              "error_message": "Upstream returned 429 too many requests."
            }
            """.trimIndent()
        )

        val exception = assertFailsWith<RateLimitException> {
            repo.getProductsFromDepartment(departmentId = 22886614)
        }

        assertEquals("Upstream returned 429 too many requests.", exception.message)
    }

    @Test
    fun `getItemDetail maps other bad request response to data invalidate exception`() = runBlocking {
        val repo = repositoryReturning(
            """
            {
              "error_code": "invalid_department",
              "error_message": "Department data is invalid."
            }
            """.trimIndent()
        )

        val exception = assertFailsWith<DataInvalidateException> {
            repo.getItemDetail(1564405684712095895L)
        }

        assertEquals("Department data is invalid.", exception.message)
    }

    @Test
    fun `fetchUrlAsJson maps malformed bad request response to data invalidate exception`() = runBlocking {
        val repo = repositoryReturning("not-json")

        val exception = assertFailsWith<DataInvalidateException> {
            repo.fetchUrlAsJson("https://example.com/products")
        }

        assertEquals("not-json", exception.message)
    }

    private fun repositoryReturning(
        responseBody: String,
        status: HttpStatusCode = HttpStatusCode.BadRequest
    ): GroceryRepository {
        val client = HttpClient(
            MockEngine {
                respond(
                    content = responseBody,
                    status = status,
                    headers = headersOf(HttpHeaders.ContentType, ContentType.Application.Json.toString())
                )
            }
        ) {
            install(ContentNegotiation) {
                json(
                    Json {
                        ignoreUnknownKeys = true
                        isLenient = true
                    }
                )
            }
            defaultRequest {
                accept(ContentType.Application.Json)
            }
        }
        clients += client
        return GroceryRepository(
            tokenProvider = FreshopTokenProvider { "test-token" },
            httpClient = client,
            testConstructor = Unit
        )
    }
}
