package com.ericho.dropit

import com.ericho.dropit.model.FetchOptions
import com.ericho.dropit.model.SingleProductPayload
import com.ericho.dropit.model.adapter.FakeStorage
import com.ericho.dropit.model.adapter.Storage
import com.ericho.dropit.model.api.DepartmentDto
import com.ericho.dropit.model.api.ProductDto
import com.ericho.dropit.model.entity.JobEntity
import com.ericho.dropit.model.entity.JobStatus
import com.ericho.dropit.model.entity.JobType
import com.ericho.dropit.model.entity.SyncEntity
import com.ericho.dropit.model.entity.SyncStatus
import kotlinx.coroutines.runBlocking
import kotlinx.serialization.json.Json
import org.junit.jupiter.api.Test
import kotlin.test.assertEquals
import kotlin.test.assertFailsWith
import kotlin.test.assertNotNull
import kotlin.test.assertNull

class DropitFetchServiceSyncFlowTest {
    private val json = Json {
        ignoreUnknownKeys = true
        isLenient = true
    }

    @Test
    fun `run creates sync when none running and finalizes done`() = runBlocking {
        val storage = TrackingStorage()
        val repo = RecordingDataSource(basePayload = baseDetail(), departments = listOf(10, 20))
        val service = DropitFetchService(repo = repo, storage = storage)

        val report = service.run(defaultOptions())

        val sync = storage.latestSync()
        assertNotNull(sync)
        assertEquals(SyncStatus.DONE, sync.status)
        assertEquals(1, sync.attempts)
        assertNotNull(sync.finishedAt)
        assertEquals(2, report.departments)
        assertEquals(1, repo.allDepartmentsCalls)
    }

    @Test
    fun `run reuses existing running sync`() = runBlocking {
        val storage = TrackingStorage()
        val repo = RecordingDataSource(basePayload = baseDetail(), departments = listOf(30))
        val existing = storage.createSyncEntity()
        storage.updateSyncEntity(existing.copy(status = SyncStatus.RUNNING, attempts = 4))
        val createCallsBeforeRun = storage.createSyncCalls

        val service = DropitFetchService(repo = repo, storage = storage)
        val report = service.run(defaultOptions())

        assertEquals(createCallsBeforeRun, storage.createSyncCalls)
        val sync = storage.getSync(existing.id ?: error("missing id"))
        assertNotNull(sync)
        assertEquals(SyncStatus.DONE, sync.status)
        assertEquals(5, sync.attempts)
        assertEquals(1, report.departments)
    }

    @Test
    fun `run skips all-departments API when marker exists`() = runBlocking {
        val storage = TrackingStorage()
        val repo = RecordingDataSource(basePayload = baseDetail(), departments = listOf(40, 50))
        val running = storage.createSyncEntity()
        val runningSync = storage.updateSyncEntity(running.copy(status = SyncStatus.RUNNING))
        val syncId = runningSync.id ?: error("missing id")
        storage.insertJobsIfNotExist(
            listOf(
                JobEntity(
                    syncId = syncId,
                    jobType = JobType.FETCH_DEPARTMENTS,
                    status = JobStatus.PENDING,
                    dedupeKey = "all_departments"
                ),
                JobEntity(
                    syncId = syncId,
                    jobType = JobType.FETCH_DEPARTMENT_PRODUCTS,
                    status = JobStatus.PENDING,
                    dedupeKey = "dept:40"
                ),
                JobEntity(
                    syncId = syncId,
                    jobType = JobType.FETCH_DEPARTMENT_PRODUCTS,
                    status = JobStatus.PENDING,
                    dedupeKey = "dept:50"
                )
            )
        )

        val service = DropitFetchService(repo = repo, storage = storage)
        val report = service.run(defaultOptions())

        assertEquals(0, repo.allDepartmentsCalls)
        assertEquals(2, report.departments)
        assertEquals(2, report.items)
        assertEquals(2, report.details)
    }

    @Test
    fun `run marks sync retry on fatal failure`() = runBlocking {
        val storage = TrackingStorage()
        val repo = RecordingDataSource(
            basePayload = baseDetail(),
            departments = listOf(60),
            failAllDepartments = true
        )
        val service = DropitFetchService(repo = repo, storage = storage)

        assertFailsWith<IllegalStateException> {
            service.run(defaultOptions())
        }

        val sync = storage.latestSync()
        assertNotNull(sync)
        assertEquals(SyncStatus.RETRY, sync.status)
        assertEquals(1, sync.attempts)
        assertNull(sync.finishedAt)
    }

    private fun defaultOptions(): FetchOptions {
        return FetchOptions(
            deptConcurrency = 2,
            detailConcurrency = 4,
            resume = false,
            since = null,
            dryRun = false
        )
    }

    private fun baseDetail(): SingleProductPayload {
        val rawJson = readResourceText("/single_product_1564405684712095895.json")
        return json.decodeFromString(rawJson)
    }

    private fun readResourceText(resourceName: String): String {
        val url = checkNotNull(this::class.java.getResource(resourceName)) {
            "Missing test resource: $resourceName"
        }
        return url.readText()
    }

    private class RecordingDataSource(
        private val basePayload: SingleProductPayload,
        departments: List<Int>,
        private val failAllDepartments: Boolean = false
    ) : GroceryDataSource {
        private val departmentsPayload = departments.map { id ->
            DepartmentDto(
                id = id.toString(),
                count = 1,
                sequence = id,
                name = "Department $id",
                parentId = null,
                identifier = "dept-$id",
                internalSequence = id,
                storeId = "7442",
                storeDepth = 1,
                typeId = "type-$id",
                path = "Department/$id",
                lineage = listOf("Department"),
                canonicalUrl = "/department/$id",
                masterTaxonomy = null,
                isRedDepartment = false
            )
        }
        var allDepartmentsCalls: Int = 0
            private set

        override suspend fun getAllDepartments(storeId: Int): List<DepartmentDto> {
            allDepartmentsCalls += 1
            if (failAllDepartments) {
                throw IllegalStateException("all departments failed")
            }
            return departmentsPayload
        }

        override suspend fun getAllItemsInDepartment(departmentId: Int, fetchOptions: FetchOptions): List<ProductDto> {
            return listOf(ProductDto(id = "${departmentId}001"))
        }

        override suspend fun getItemDetail(itemId: Long): SingleProductPayload {
            return basePayload.copy(
                id = itemId.toString(),
                name = "Item $itemId"
            )
        }
    }

    private class TrackingStorage(private val delegate: FakeStorage = FakeStorage()) : Storage by delegate {
        private val syncsById: MutableMap<Int, SyncEntity> = mutableMapOf()
        var createSyncCalls: Int = 0
            private set

        override fun createSyncEntity(): SyncEntity {
            createSyncCalls += 1
            val created = delegate.createSyncEntity()
            val id = created.id ?: error("missing sync id")
            syncsById[id] = created
            return created
        }

        override fun updateSyncEntity(entity: SyncEntity): SyncEntity {
            val updated = delegate.updateSyncEntity(entity)
            val id = updated.id ?: error("missing sync id")
            syncsById[id] = updated
            return updated
        }

        fun latestSync(): SyncEntity? {
            return syncsById.values.maxByOrNull { it.id ?: Int.MIN_VALUE }
        }

        fun getSync(id: Int): SyncEntity? {
            return syncsById[id]
        }
    }
}
