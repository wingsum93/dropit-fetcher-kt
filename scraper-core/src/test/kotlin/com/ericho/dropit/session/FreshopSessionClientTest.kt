package com.ericho.dropit.session

import io.ktor.client.request.forms.MultiPartFormDataContent
import io.ktor.client.HttpClient
import io.ktor.client.engine.mock.MockEngine
import io.ktor.client.engine.mock.respond
import io.ktor.client.plugins.contentnegotiation.ContentNegotiation
import io.ktor.http.ContentType
import io.ktor.http.HttpHeaders
import io.ktor.http.HttpMethod
import io.ktor.http.HttpStatusCode
import io.ktor.http.headersOf
import io.ktor.http.content.PartData
import io.ktor.serialization.kotlinx.json.json
import kotlinx.coroutines.async
import kotlinx.coroutines.awaitAll
import kotlinx.coroutines.runBlocking
import kotlinx.serialization.json.Json
import java.util.concurrent.atomic.AtomicInteger
import kotlin.test.Test
import kotlin.test.assertEquals
import kotlin.test.assertFailsWith
import kotlin.test.assertIs
import kotlin.test.assertTrue

class FreshopSessionClientTest {
    @Test
    fun `createSession posts multipart form fields and parses response`() = runBlocking {
        val formFields = FreshopSessionClient.createSessionFormParameters(FreshopSessionConfig())
            .filterIsInstance<PartData.FormItem>()
            .associate { requireNotNull(it.name) to it.value }

        assertEquals("lindos", formFields["app_keys"])
        assertEquals("false", formFields["locale"])
        assertEquals("https://www.dropit.bm/", formFields["referrer"])

        val engine = MockEngine { request ->
            assertEquals(HttpMethod.Post, request.method)
            assertEquals("https", request.url.protocol.name)
            assertEquals("api.freshop.ncrcloud.com", request.url.host)
            assertEquals("/2/sessions/create", request.url.encodedPath)
            assertEquals("https://www.dropit.bm/", request.url.parameters["referrer"])
            assertEquals("false", request.url.parameters["locale"])
            assertEquals("lindos", request.url.parameters["app_key"])

            val body = assertIs<MultiPartFormDataContent>(request.body)
            assertTrue(body.contentType.match(ContentType.MultiPart.FormData))

            respond(
                content = """
                    {
                        "token": "session-token",
                        "store_id": "7442",
                        "created_at": "2026-08-25T20:39:24.514+00:00",
                        "locale": "false"
                    }
                """.trimIndent(),
                status = HttpStatusCode.OK,
                headers = headersOf(HttpHeaders.ContentType, "application/json")
            )
        }
        val client = FreshopSessionClient(
            httpClient = HttpClient(engine) {
                install(ContentNegotiation) {
                    json(
                        Json {
                            ignoreUnknownKeys = true
                            isLenient = true
                        }
                    )
                }
            },
            endpoint = FreshopSessionClient.DEFAULT_ENDPOINT,
            clockMillis = { 1_787_661_173_769L },
            testConstructor = Unit
        )

        val session = client.createSession(FreshopSessionConfig())

        assertEquals("session-token", session.token)
        assertEquals("7442", session.storeId)
        assertEquals("2026-08-25T20:39:24.514+00:00", session.createdAt)
        assertEquals("false", session.locale)
    }

    @Test
    fun `ApiFreshopTokenProvider initializes and caches token`() = runBlocking {
        val requestCount = AtomicInteger(0)
        val client = testSessionClient(
            engine = MockEngine {
                requestCount.incrementAndGet()
                respond(
                    content = """
                        {
                            "token": "cached-token",
                            "store_id": "7442",
                            "created_at": "2026-08-25T20:39:24.514+00:00",
                            "locale": "false"
                        }
                    """.trimIndent(),
                    status = HttpStatusCode.OK,
                    headers = headersOf(HttpHeaders.ContentType, "application/json")
                )
            }
        )
        val provider = ApiFreshopTokenProvider(client = client, config = FreshopSessionConfig())

        assertEquals("cached-token", provider.initialize())
        assertEquals("cached-token", provider.token())
        assertEquals("cached-token", provider.token())
        assertEquals(1, requestCount.get())
    }

    @Test
    fun `ApiFreshopTokenProvider shares a concurrent initialization request`() = runBlocking {
        val requestCount = AtomicInteger(0)
        val client = testSessionClient(
            engine = MockEngine {
                requestCount.incrementAndGet()
                respond(
                    content = """
                        {
                            "token": "concurrent-token",
                            "store_id": "7442",
                            "created_at": "2026-08-25T20:39:24.514+00:00",
                            "locale": "false"
                        }
                    """.trimIndent(),
                    status = HttpStatusCode.OK,
                    headers = headersOf(HttpHeaders.ContentType, "application/json")
                )
            }
        )
        val provider = ApiFreshopTokenProvider(client = client, config = FreshopSessionConfig())

        val tokens = List(10) {
            async { provider.token() }
        }.awaitAll()

        assertEquals(List(10) { "concurrent-token" }, tokens)
        assertEquals(1, requestCount.get())
    }

    @Test
    fun `ApiFreshopTokenProvider rejects blank session token`() = runBlocking {
        val client = testSessionClient(
            engine = MockEngine {
                respond(
                    content = """
                        {
                            "token": "",
                            "store_id": "7442",
                            "created_at": "2026-08-25T20:39:24.514+00:00",
                            "locale": "false"
                        }
                    """.trimIndent(),
                    status = HttpStatusCode.OK,
                    headers = headersOf(HttpHeaders.ContentType, "application/json")
                )
            }
        )
        val provider = ApiFreshopTokenProvider(client = client, config = FreshopSessionConfig())

        assertFailsWith<IllegalArgumentException> {
            provider.initialize()
        }
    }

    private fun testSessionClient(engine: MockEngine): FreshopSessionClient {
        return FreshopSessionClient(
            httpClient = HttpClient(engine) {
                install(ContentNegotiation) {
                    json(
                        Json {
                            ignoreUnknownKeys = true
                            isLenient = true
                        }
                    )
                }
            },
            endpoint = FreshopSessionClient.DEFAULT_ENDPOINT,
            clockMillis = { 1_787_661_173_769L },
            testConstructor = Unit
        )
    }
}
