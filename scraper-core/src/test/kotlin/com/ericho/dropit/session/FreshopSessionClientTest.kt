package com.ericho.dropit.session

import io.ktor.client.HttpClient
import io.ktor.client.engine.mock.MockEngine
import io.ktor.client.engine.mock.respond
import io.ktor.client.plugins.contentnegotiation.ContentNegotiation
import io.ktor.client.request.forms.FormDataContent
import io.ktor.http.HttpHeaders
import io.ktor.http.HttpMethod
import io.ktor.http.HttpStatusCode
import io.ktor.http.headersOf
import io.ktor.serialization.kotlinx.json.json
import kotlinx.coroutines.runBlocking
import kotlinx.serialization.json.Json
import kotlin.test.Test
import kotlin.test.assertEquals

class FreshopSessionClientTest {
    @Test
    fun `createSession posts form fields and parses response`() = runBlocking {
        val engine = MockEngine { request ->
            assertEquals(HttpMethod.Post, request.method)
            assertEquals(FreshopSessionClient.DEFAULT_ENDPOINT, request.url.toString())

            val body = request.body as FormDataContent
            assertEquals("lindos", body.formData["app_key"])
            assertEquals("false", body.formData["locale"])
            assertEquals("https://www.dropit.bm/", body.formData["referrer"])
            assertEquals("1787661173769", body.formData["utc"])

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
}
