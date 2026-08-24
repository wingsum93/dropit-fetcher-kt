package com.ericho.dropit.server

import com.ericho.dropit.model.adapter.FakeStorage
import com.ericho.dropit.model.entity.DepartmentEntity
import com.ericho.dropit.model.entity.ProductEntity
import com.ericho.dropit.scraper.ProductScraper
import io.ktor.client.request.get
import io.ktor.client.request.post
import io.ktor.client.statement.bodyAsText
import io.ktor.http.HttpStatusCode
import io.ktor.server.testing.testApplication
import java.time.Instant
import kotlin.test.Test
import kotlin.test.assertEquals
import kotlin.test.assertTrue

class ScrapeApiTest {
    @Test
    fun `health returns ok`() = testApplication {
        val storage = FakeStorage()
        application {
            configureScrapeApi(storage = storage, scraper = ProductScraper(storage = storage))
        }

        val response = client.get("/health")

        assertEquals(HttpStatusCode.OK, response.status)
        assertTrue(response.bodyAsText().contains("\"ok\""))
    }

    @Test
    fun `post scrapes creates a sync and seed job`() = testApplication {
        val storage = FakeStorage()
        application {
            configureScrapeApi(storage = storage, scraper = ProductScraper(storage = storage))
        }

        val response = client.post("/scrapes")

        assertEquals(HttpStatusCode.Accepted, response.status)
        assertTrue(response.bodyAsText().contains("\"status\":\"RUNNING\""))
        assertEquals(1, storage.findJobsBySync(1).size)
    }

    @Test
    fun `read routes return stored entities`() = testApplication {
        val storage = FakeStorage()
        storage.createProductIfNotExist(listOf(1001))
        storage.updateProduct(
            1001,
            ProductEntity(
                id = 1001,
                name = "Milk",
                remoteLastUpdateAt = Instant.parse("2026-01-01T00:00:00Z")
            )
        )
        storage.insertDepartmentEntity(
            listOf(
                DepartmentEntity(
                    id = 10,
                    parentDepartmentId = null,
                    name = "Dairy",
                    path = "/dairy",
                    storeId = 7442,
                    count = 1,
                    canonicalUrl = "https://example.com/dairy"
                )
            )
        )
        application {
            configureScrapeApi(storage = storage, scraper = ProductScraper(storage = storage))
        }

        assertTrue(client.get("/products?limit=10").bodyAsText().contains("\"name\":\"Milk\""))
        assertTrue(client.get("/departments").bodyAsText().contains("\"name\":\"Dairy\""))
    }
}
