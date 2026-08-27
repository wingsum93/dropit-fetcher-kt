package com.ericho.dropit.worker

import io.ktor.client.request.get
import io.ktor.client.statement.bodyAsText
import io.ktor.http.HttpStatusCode
import io.ktor.server.testing.testApplication
import kotlin.test.Test
import kotlin.test.assertEquals

class WorkerHealthTest {
    @Test
    fun `health returns service unavailable before worker is ready`() = testApplication {
        val readiness = WorkerReadiness()
        application {
            configureWorkerHealth(readiness)
        }

        val response = client.get("/health")

        assertEquals(HttpStatusCode.ServiceUnavailable, response.status)
        assertEquals("initializing", response.bodyAsText())
    }

    @Test
    fun `health returns ok after worker is ready`() = testApplication {
        val readiness = WorkerReadiness()
        readiness.markReady()
        application {
            configureWorkerHealth(readiness)
        }

        val response = client.get("/health")

        assertEquals(HttpStatusCode.OK, response.status)
        assertEquals("ready", response.bodyAsText())
    }
}
