package com.ericho.dropit.scraper

import com.ericho.dropit.GroceryDataSource
import com.ericho.dropit.model.FetchOptions
import com.ericho.dropit.model.SingleProductPayload
import com.ericho.dropit.model.adapter.FakeStorage
import com.ericho.dropit.model.api.DepartmentDto
import com.ericho.dropit.model.api.ProductDto
import com.ericho.dropit.model.entity.JobStatus
import com.ericho.dropit.model.entity.JobType
import com.ericho.dropit.model.entity.SyncStatus
import kotlinx.coroutines.runBlocking
import kotlinx.serialization.json.Json
import kotlin.test.Test
import kotlin.test.assertEquals
import kotlin.test.assertNotNull

class ProductScraperTest {
    @Test
    fun `submit creates running sync and seed department job`() {
        val storage = FakeStorage()
        val scraper = ProductScraper(repo = FakeDataSource(), storage = storage, rateLimiter = NoopRateLimiter)

        val sync = scraper.submitScrape()
        val syncId = sync.id ?: error("missing sync id")

        assertEquals(SyncStatus.RUNNING, sync.status)
        assertEquals(1, sync.attempts)
        val jobs = storage.findJobsBySync(syncId)
        assertEquals(1, jobs.size)
        assertEquals(JobType.FETCH_DEPARTMENTS, jobs.single().jobType)
        assertEquals(JobStatus.PENDING, jobs.single().status)
    }

    @Test
    fun `claimed jobs expand departments and complete sync`() = runBlocking {
        val storage = FakeStorage()
        val scraper = ProductScraper(repo = FakeDataSource(), storage = storage, rateLimiter = NoopRateLimiter)
        val syncId = scraper.submitScrape().id ?: error("missing sync id")

        scraper.executeClaimedJob(storage.claimPendingJobs(1).single())

        assertEquals(2, storage.findAllDepartments().size)
        assertEquals(2, storage.findJobsBySync(syncId, JobStatus.PENDING, JobType.FETCH_DEPARTMENT_PRODUCTS).size)
        assertEquals(SyncStatus.RUNNING, storage.findSyncById(syncId)?.status)

        scraper.executeClaimedJob(storage.claimPendingJobs(1).single())
        scraper.executeClaimedJob(storage.claimPendingJobs(1).single())

        assertEquals(SyncStatus.DONE, storage.findSyncById(syncId)?.status)
        assertNotNull(storage.findSyncById(syncId)?.finishedAt)
        assertEquals(2, storage.findProducts(10).size)
        assertEquals("Item 1001", storage.findProductById(1001)?.name)
    }

    private object NoopRateLimiter : RateLimiter {
        override suspend fun awaitPermit(name: String) = Unit
    }

    private class FakeDataSource : GroceryDataSource {
        private val json = Json {
            ignoreUnknownKeys = true
            isLenient = true
        }
        private val baseDetail: SingleProductPayload by lazy {
            val rawJson = checkNotNull(this::class.java.getResource("/single_product_1564405684712095895.json")).readText()
            json.decodeFromString(rawJson)
        }

        override suspend fun getAllDepartments(storeId: Int): List<DepartmentDto> {
            return listOf(10, 20).map { departmentId ->
                DepartmentDto(
                    id = departmentId.toString(),
                    count = 1,
                    sequence = departmentId,
                    name = "Department $departmentId",
                    parentId = null,
                    identifier = "dept-$departmentId",
                    internalSequence = departmentId,
                    storeId = storeId.toString(),
                    storeDepth = 1,
                    typeId = "department",
                    path = "/department/$departmentId",
                    lineage = listOf("Shop"),
                    canonicalUrl = "https://example.com/department/$departmentId"
                )
            }
        }

        override suspend fun getAllItemsInDepartment(departmentId: Int, fetchOptions: FetchOptions): List<ProductDto> {
            return listOf(ProductDto(id = "${departmentId}01"))
        }

        override suspend fun getItemDetail(itemId: Long): SingleProductPayload {
            return baseDetail.copy(
                id = itemId.toString(),
                name = "Item $itemId"
            )
        }
    }
}
