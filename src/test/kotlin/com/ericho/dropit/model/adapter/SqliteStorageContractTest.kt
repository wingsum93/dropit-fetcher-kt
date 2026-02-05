package com.ericho.dropit.model.adapter

import com.ericho.dropit.model.entity.DepartmentEntity
import com.ericho.dropit.model.entity.JobEntity
import com.ericho.dropit.model.entity.JobStatus
import com.ericho.dropit.model.entity.JobType
import com.ericho.dropit.model.entity.ProductEntity
import com.ericho.dropit.model.entity.SyncStatus
import org.junit.jupiter.api.Test
import java.nio.file.Files
import java.sql.DriverManager
import java.time.Instant
import kotlin.test.assertEquals
import kotlin.test.assertFailsWith
import kotlin.test.assertNull
import kotlin.test.assertTrue

class SqliteStorageContractTest {
    @Test
    fun `schema exists after startup`() {
        val dbPath = Files.createTempFile("dropit-storage-contract-", ".sqlite")
        val storage = SqliteStorage(dbPath.toString())

        try {
            DriverManager.getConnection("jdbc:sqlite:${dbPath}").use { connection ->
                connection.prepareStatement(
                    "SELECT name FROM sqlite_master WHERE type = 'table' AND name = 'products'"
                ).use { stmt ->
                    stmt.executeQuery().use { rs ->
                        assertTrue(rs.next(), "Expected products table to exist")
                    }
                }

                connection.prepareStatement(
                    "SELECT name FROM sqlite_master WHERE type = 'table' AND name = 'departments'"
                ).use { stmt ->
                    stmt.executeQuery().use { rs ->
                        assertTrue(rs.next(), "Expected departments table to exist")
                    }
                }

                connection.prepareStatement(
                    "SELECT name FROM sqlite_master WHERE type = 'table' AND name = 'jobs'"
                ).use { stmt ->
                    stmt.executeQuery().use { rs ->
                        assertTrue(rs.next(), "Expected jobs table to exist")
                    }
                }

                connection.prepareStatement(
                    "SELECT name FROM sqlite_master WHERE type = 'table' AND name = 'syncs'"
                ).use { stmt ->
                    stmt.executeQuery().use { rs ->
                        assertTrue(rs.next(), "Expected syncs table to exist")
                    }
                }
            }
        } finally {
            storage.close()
            Files.deleteIfExists(dbPath)
        }
    }

    @Test
    fun `findProductById returns null for missing row`() {
        val dbPath = Files.createTempFile("dropit-storage-find-by-id-", ".sqlite")
        val storage = SqliteStorage(dbPath.toString())

        try {
            assertNull(storage.findProductById(9999))
        } finally {
            storage.close()
            Files.deleteIfExists(dbPath)
        }
    }

    @Test
    fun `createProductIfNotExist inserts only missing ids`() {
        val dbPath = Files.createTempFile("dropit-storage-create-missing-", ".sqlite")
        val storage = SqliteStorage(dbPath.toString())

        try {
            storage.createProductIfNotExist(listOf(101, 102, 102, 103))
            storage.createProductIfNotExist(listOf(102, 103, 104))

            DriverManager.getConnection("jdbc:sqlite:${dbPath}").use { connection ->
                val rowCount = connection.prepareStatement("SELECT COUNT(*) FROM products").use { stmt ->
                    stmt.executeQuery().use { rs ->
                        rs.next()
                        rs.getInt(1)
                    }
                }
                assertEquals(4, rowCount)
            }
        } finally {
            storage.close()
            Files.deleteIfExists(dbPath)
        }
    }

    @Test
    fun `findProductsNameIsEmpty matches null and blank and respects order limit`() {
        val dbPath = Files.createTempFile("dropit-storage-name-empty-", ".sqlite")
        val storage = SqliteStorage(dbPath.toString())
        val now = Instant.parse("2026-01-01T00:00:00Z")

        try {
            storage.createProductIfNotExist(listOf(1, 2, 3, 4))
            storage.updateProduct(1, sampleProduct(1, name = null, remoteLastUpdateAt = now))
            storage.updateProduct(2, sampleProduct(2, name = "", remoteLastUpdateAt = now))
            storage.updateProduct(3, sampleProduct(3, name = "   ", remoteLastUpdateAt = now))
            storage.updateProduct(4, sampleProduct(4, name = "Apple", remoteLastUpdateAt = now))

            val rows = storage.findProductsNameIsEmpty(limit = 2)
            assertEquals(listOf(1L, 2L), rows.map { it.id })
        } finally {
            storage.close()
            Files.deleteIfExists(dbPath)
        }
    }

    @Test
    fun `findProductsSince filters with inclusive boundary and newest first`() {
        val dbPath = Files.createTempFile("dropit-storage-since-", ".sqlite")
        val storage = SqliteStorage(dbPath.toString())

        val t1 = Instant.parse("2026-01-01T10:00:00Z")
        val t2 = Instant.parse("2026-01-02T10:00:00Z")
        val t3 = Instant.parse("2026-01-03T10:00:00Z")

        try {
            storage.createProductIfNotExist(listOf(10, 11, 12))
            storage.updateProduct(10, sampleProduct(10, name = "A", remoteLastUpdateAt = t1))
            storage.updateProduct(11, sampleProduct(11, name = "B", remoteLastUpdateAt = t2))
            storage.updateProduct(12, sampleProduct(12, name = "C", remoteLastUpdateAt = t3))

            val rows = storage.findProductsSince(t2, limit = 10)
            assertEquals(listOf(12L, 11L), rows.map { it.id })
        } finally {
            storage.close()
            Files.deleteIfExists(dbPath)
        }
    }

    @Test
    fun `updateProduct updates existing row`() {
        val dbPath = Files.createTempFile("dropit-storage-update-existing-", ".sqlite")
        val storage = SqliteStorage(dbPath.toString())

        try {
            storage.createProductIfNotExist(listOf(777))
            storage.updateProduct(
                productId = 777,
                product = sampleProduct(
                    productId = 777,
                    name = "Updated Name",
                    remoteLastUpdateAt = Instant.parse("2026-02-01T00:00:00Z")
                )
            )

            val row = storage.findProductById(777)
            assertEquals("Updated Name", row?.name)
            assertEquals(Instant.parse("2026-02-01T00:00:00Z"), row?.remoteLastUpdateAt)
        } finally {
            storage.close()
            Files.deleteIfExists(dbPath)
        }
    }

    @Test
    fun `updateProduct throws when row missing`() {
        val dbPath = Files.createTempFile("dropit-storage-update-missing-", ".sqlite")
        val storage = SqliteStorage(dbPath.toString())

        try {
            val exception = assertFailsWith<StorageWriteException> {
                storage.updateProduct(
                    productId = 888,
                    product = sampleProduct(
                        productId = 888,
                        name = "Missing",
                        remoteLastUpdateAt = Instant.parse("2026-02-02T00:00:00Z")
                    )
                )
            }
            assertTrue(exception.message?.contains("updateProduct") == true)
        } finally {
            storage.close()
            Files.deleteIfExists(dbPath)
        }
    }

    @Test
    fun `updateProduct throws when productId mismatches`() {
        val dbPath = Files.createTempFile("dropit-storage-update-mismatch-", ".sqlite")
        val storage = SqliteStorage(dbPath.toString())

        try {
            storage.createProductIfNotExist(listOf(1000))

            assertFailsWith<IllegalArgumentException> {
                storage.updateProduct(
                    productId = 1000,
                    product = sampleProduct(
                        productId = 2000,
                        name = "Mismatch",
                        remoteLastUpdateAt = Instant.parse("2026-02-03T00:00:00Z")
                    )
                )
            }
        } finally {
            storage.close()
            Files.deleteIfExists(dbPath)
        }
    }

    @Test
    fun `resets all managed tables when schema is stale`() {
        val dbPath = Files.createTempFile("dropit-storage-stale-", ".sqlite")

        DriverManager.getConnection("jdbc:sqlite:${dbPath}").use { connection ->
            connection.createStatement().use { statement ->
                statement.execute(
                    """
                    CREATE TABLE IF NOT EXISTS products (
                        id INTEGER PRIMARY KEY AUTOINCREMENT,
                        product_id INTEGER NOT NULL UNIQUE,
                        created_at TIMESTAMP NOT NULL
                    )
                    """.trimIndent()
                )

                statement.execute(
                    """
                    CREATE TABLE IF NOT EXISTS departments (
                        id INTEGER NOT NULL PRIMARY KEY,
                        parent_department_id INTEGER NULL,
                        name TEXT NOT NULL,
                        path TEXT NOT NULL,
                        store_id INTEGER NOT NULL,
                        count INTEGER NOT NULL,
                        canonical_url TEXT NOT NULL,
                        created_at TIMESTAMP NOT NULL
                    )
                    """.trimIndent()
                )

                statement.execute(
                    """
                    CREATE TABLE IF NOT EXISTS jobs (
                        id INTEGER PRIMARY KEY AUTOINCREMENT,
                        sync_id INTEGER NOT NULL,
                        job_type TEXT NOT NULL,
                        status TEXT NOT NULL,
                        created_at TIMESTAMP NOT NULL,
                        updated_at TIMESTAMP NOT NULL,
                        dedupe_key TEXT NOT NULL
                    )
                    """.trimIndent()
                )

                statement.execute(
                    """
                    INSERT INTO departments (
                        id, parent_department_id, name, path, store_id, count, canonical_url, created_at
                    ) VALUES (1, NULL, 'D', '/d', 7442, 1, '/department/1', CURRENT_TIMESTAMP)
                    """.trimIndent()
                )

                statement.execute(
                    """
                    INSERT INTO jobs (
                        sync_id, job_type, status, created_at, updated_at, dedupe_key
                    ) VALUES (1, 'FETCH_PRODUCT', 'PENDING', CURRENT_TIMESTAMP, CURRENT_TIMESTAMP, 'dept:1')
                    """.trimIndent()
                )
            }
        }

        val storage = SqliteStorage(dbPath.toString())
        try {
            DriverManager.getConnection("jdbc:sqlite:${dbPath}").use { connection ->
                val columns = connection.prepareStatement("PRAGMA table_info(products)").use { stmt ->
                    stmt.executeQuery().use { rs ->
                        buildSet {
                            while (rs.next()) {
                                add(rs.getString("name"))
                            }
                        }
                    }
                }

                assertEquals(
                    setOf(
                        "id",
                        "store_id",
                        "category",
                        "department_id",
                        "unit_price",
                        "popularity",
                        "upc",
                        "name",
                        "canonical_url",
                        "remote_last_update_at",
                        "created_at"
                    ),
                    columns
                )

                val departmentCount = connection.prepareStatement("SELECT COUNT(*) FROM departments").use { stmt ->
                    stmt.executeQuery().use { rs ->
                        rs.next()
                        rs.getInt(1)
                    }
                }
                assertEquals(0, departmentCount)

                val jobCount = connection.prepareStatement("SELECT COUNT(*) FROM jobs").use { stmt ->
                    stmt.executeQuery().use { rs ->
                        rs.next()
                        rs.getInt(1)
                    }
                }
                assertEquals(0, jobCount)
            }
        } finally {
            storage.close()
            Files.deleteIfExists(dbPath)
        }
    }

    @Test
    fun `insertDepartmentEntity inserts missing ids and skips conflicts`() {
        val dbPath = Files.createTempFile("dropit-storage-department-insert-", ".sqlite")
        val storage = SqliteStorage(dbPath.toString())

        try {
            val now = Instant.parse("2026-03-02T00:00:00Z")
            val d1 = DepartmentEntity(
                id = 11,
                parentDepartmentId = null,
                name = "Fruit",
                path = "a/b",
                storeId = 7442,
                count = 10,
                canonicalUrl = "/department/11",
                createdAt = now
            )
            val d1Conflict = d1.copy(name = "Should Skip")
            val d2 = d1.copy(id = 12, name = "Vegetable")

            storage.insertDepartmentEntity(listOf(d1, d1Conflict, d2))

            val all = storage.findAllDepartments()
            assertEquals(listOf(11, 12), all.map { it.id })
            assertEquals("Fruit", storage.findDepartmentById(11)?.name)
        } finally {
            storage.close()
            Files.deleteIfExists(dbPath)
        }
    }

    @Test
    fun `jobs APIs respect dedupe and ordering semantics`() {
        val dbPath = Files.createTempFile("dropit-storage-jobs-", ".sqlite")
        val storage = SqliteStorage(dbPath.toString())

        try {
            val now = Instant.parse("2026-03-03T00:00:00Z")
            storage.insertJobsIfNotExist(
                listOf(
                    JobEntity(
                        syncId = 1,
                        jobType = JobType.FETCH_PRODUCT,
                        status = JobStatus.PENDING,
                        dedupeKey = "product:100",
                        createdAt = now,
                        updatedAt = now
                    ),
                    JobEntity(
                        syncId = 1,
                        jobType = JobType.FETCH_PRODUCT,
                        status = JobStatus.PENDING,
                        dedupeKey = "dept:33",
                        createdAt = now,
                        updatedAt = now
                    )
                )
            )
            storage.insertJobsIfNotExist(
                listOf(
                    JobEntity(
                        syncId = 1,
                        jobType = JobType.FETCH_PRODUCT,
                        status = JobStatus.PENDING,
                        dedupeKey = "product:100",
                        createdAt = now,
                        updatedAt = now
                    )
                )
            )

            val pending = storage.findJobsByType(JobType.FETCH_PRODUCT, JobStatus.PENDING)
            assertEquals(2, pending.size)
            assertTrue((pending[0].id ?: 0) < (pending[1].id ?: 0))

            val deptJob = storage.findDepartmentJobsById(1, 33)
            assertEquals(1, deptJob.size)
            assertEquals("dept:33", deptJob[0].dedupeKey)

            val latestByKey = storage.findByDedupeKey(1, "product:100")
            assertEquals("product:100", latestByKey?.dedupeKey)

            val firstId = pending.first().id ?: error("missing id")
            storage.updateJobStatusById(firstId, JobStatus.IN_PROGRESS)
            assertEquals(
                JobStatus.IN_PROGRESS,
                storage.findJobsByType(1, JobType.FETCH_PRODUCT, JobStatus.IN_PROGRESS).first().status
            )

            val secondId = pending.last().id ?: error("missing id")
            storage.updateJobStatusByIds(listOf(firstId, secondId), JobStatus.SUCCESS)
            val done = storage.findJobsByType(1, JobType.FETCH_PRODUCT, JobStatus.SUCCESS)
            assertEquals(2, done.size)
        } finally {
            storage.close()
            Files.deleteIfExists(dbPath)
        }
    }

    @Test
    fun `createSyncEntity returns defaults with generated id`() {
        val dbPath = Files.createTempFile("dropit-storage-sync-create-", ".sqlite")
        val storage = SqliteStorage(dbPath.toString())

        try {
            val created = storage.createSyncEntity()
            assertTrue((created.id ?: 0) > 0)
            assertEquals(0, created.attempts)
            assertEquals(SyncStatus.PENDING, created.status)
            assertNull(created.finishedAt)
        } finally {
            storage.close()
            Files.deleteIfExists(dbPath)
        }
    }

    @Test
    fun `findSyncEntityLeft returns latest running by id`() {
        val dbPath = Files.createTempFile("dropit-storage-sync-find-left-", ".sqlite")
        val storage = SqliteStorage(dbPath.toString())

        try {
            val first = storage.createSyncEntity()
            val second = storage.createSyncEntity()
            val third = storage.createSyncEntity()

            storage.updateSyncEntity(first.copy(status = SyncStatus.RUNNING, attempts = 1))
            storage.updateSyncEntity(second.copy(status = SyncStatus.DONE, attempts = 1))
            val updatedThird = storage.updateSyncEntity(third.copy(status = SyncStatus.RUNNING, attempts = 2))

            val left = storage.findSyncEntityLeft()
            assertEquals(updatedThird.id, left?.id)
            assertEquals(SyncStatus.RUNNING, left?.status)
        } finally {
            storage.close()
            Files.deleteIfExists(dbPath)
        }
    }

    @Test
    fun `updateSyncEntity updates status attempts and finishedAt`() {
        val dbPath = Files.createTempFile("dropit-storage-sync-update-", ".sqlite")
        val storage = SqliteStorage(dbPath.toString())

        try {
            val created = storage.createSyncEntity()
            val finishedAt = Instant.parse("2026-04-01T00:00:00Z")
            val updated = storage.updateSyncEntity(
                created.copy(
                    status = SyncStatus.DONE,
                    attempts = 3,
                    finishedAt = finishedAt
                )
            )

            assertEquals(SyncStatus.DONE, updated.status)
            assertEquals(3, updated.attempts)
            assertEquals(finishedAt, updated.finishedAt)
        } finally {
            storage.close()
            Files.deleteIfExists(dbPath)
        }
    }

    private fun sampleProduct(
        productId: Long,
        name: String?,
        remoteLastUpdateAt: Instant
    ): ProductEntity {
        return ProductEntity(
            id = productId,
            storeId = 7442,
            category = 100,
            departmentId = 200,
            unitPrice = 9.99f,
            popularity = 10,
            upc = "UPC-$productId",
            name = name,
            canonicalUrl = "/products/$productId",
            remoteLastUpdateAt = remoteLastUpdateAt
        )
    }
}
