package com.ericho.dropit.worker

import com.ericho.dropit.GroceryDataSource
import com.ericho.dropit.model.FetchOptions
import com.ericho.dropit.model.SingleProductPayload
import com.ericho.dropit.model.adapter.FakeStorage
import com.ericho.dropit.model.api.DepartmentDto
import com.ericho.dropit.model.api.ProductDto
import com.ericho.dropit.model.entity.JobStatus
import com.ericho.dropit.model.entity.JobType
import com.ericho.dropit.scraper.ProductScraper
import com.ericho.dropit.scraper.RateLimiter
import kotlinx.coroutines.runBlocking
import kotlin.test.Test
import kotlin.test.assertEquals

class ScraperWorkerTest {
    @Test
    fun `runOnce claims and executes a pending job`() = runBlocking {
        val storage = FakeStorage()
        val scraper = ProductScraper(repo = DepartmentOnlyDataSource, storage = storage, rateLimiter = NoopRateLimiter)
        val syncId = scraper.submitScrape().id ?: error("missing sync id")
        val worker = ScraperWorker(storage = storage, scraper = scraper, batchSize = 1)

        val processed = worker.runOnce()

        assertEquals(1, processed)
        assertEquals(1, storage.findJobsBySync(syncId, JobStatus.SUCCESS, JobType.FETCH_DEPARTMENTS).size)
        assertEquals(1, storage.findJobsBySync(syncId, JobStatus.PENDING, JobType.FETCH_DEPARTMENT_PRODUCTS).size)
    }

    private object NoopRateLimiter : RateLimiter {
        override suspend fun awaitPermit(name: String) = Unit
    }

    private object DepartmentOnlyDataSource : GroceryDataSource {
        override suspend fun getAllDepartments(storeId: Int): List<DepartmentDto> {
            return listOf(
                DepartmentDto(
                    id = "10",
                    count = 1,
                    sequence = 10,
                    name = "Department 10",
                    parentId = null,
                    identifier = "dept-10",
                    internalSequence = 10,
                    storeId = storeId.toString(),
                    storeDepth = 1,
                    typeId = "department",
                    path = "/department/10",
                    lineage = listOf("Shop"),
                    canonicalUrl = "https://example.com/department/10"
                )
            )
        }

        override suspend fun getAllItemsInDepartment(departmentId: Int, fetchOptions: FetchOptions): List<ProductDto> {
            return emptyList()
        }

        override suspend fun getItemDetail(itemId: Long): SingleProductPayload {
            error("not used")
        }
    }
}
