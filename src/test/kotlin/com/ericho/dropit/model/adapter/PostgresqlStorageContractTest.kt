package com.ericho.dropit.model.adapter

import com.ericho.dropit.model.DatabaseConfig
import com.ericho.dropit.model.entity.DepartmentEntity
import com.ericho.dropit.model.entity.JobEntity
import com.ericho.dropit.model.entity.JobStatus
import com.ericho.dropit.model.entity.JobType
import com.ericho.dropit.model.entity.ProductEntity
import com.ericho.dropit.model.entity.SyncStatus
import org.junit.jupiter.api.Assumptions.assumeTrue
import org.junit.jupiter.api.Test
import java.sql.DriverManager
import java.time.Instant
import java.util.Properties
import java.util.UUID
import kotlin.test.assertEquals
import kotlin.test.assertFailsWith
import kotlin.test.assertNull
import kotlin.test.assertTrue

class PostgresqlStorageContractTest {
    @Test
    fun `schema exists after startup`() {
        val config = postgresConfigOrNull()
        assumeTrue(config != null, "Postgres test config is not available")

        val storage = PostgresqlStorage(config!!)
        try {
            withPostgresConnection(config) { connection ->
                connection.prepareStatement(
                    """
                    SELECT EXISTS (
                        SELECT 1
                        FROM information_schema.tables
                        WHERE table_schema = 'public'
                          AND table_name = 'products'
                    )
                    """.trimIndent()
                ).use { stmt ->
                    stmt.executeQuery().use { rs ->
                        rs.next()
                        assertTrue(rs.getBoolean(1))
                    }
                }

                connection.prepareStatement(
                    """
                    SELECT EXISTS (
                        SELECT 1
                        FROM information_schema.tables
                        WHERE table_schema = 'public'
                          AND table_name = 'syncs'
                    )
                    """.trimIndent()
                ).use { stmt ->
                    stmt.executeQuery().use { rs ->
                        rs.next()
                        assertTrue(rs.getBoolean(1))
                    }
                }
            }
        } finally {
            storage.close()
        }
    }

    @Test
    fun `findProductById returns null for missing row`() {
        val config = postgresConfigOrNull()
        assumeTrue(config != null, "Postgres test config is not available")

        val storage = PostgresqlStorage(config!!)
        try {
            assertNull(storage.findProductById(Long.MAX_VALUE))
        } finally {
            storage.close()
        }
    }

    @Test
    fun `createProductIfNotExist inserts only missing ids`() {
        val config = postgresConfigOrNull()
        assumeTrue(config != null, "Postgres test config is not available")

        val ids = listOf(uniqueProductId(), uniqueProductId(), uniqueProductId())
        val storage = PostgresqlStorage(config!!)

        try {
            storage.createProductIfNotExist(listOf(ids[0], ids[1], ids[1], ids[2]))
            storage.createProductIfNotExist(listOf(ids[0], ids[2]))

            withPostgresConnection(config) { connection ->
                val count = connection.prepareStatement(
                    "SELECT COUNT(*) FROM products WHERE id = ANY (?)"
                ).use { stmt ->
                    stmt.setArray(1, connection.createArrayOf("bigint", ids.toTypedArray()))
                    stmt.executeQuery().use { rs ->
                        rs.next()
                        rs.getInt(1)
                    }
                }
                assertEquals(3, count)
            }
        } finally {
            cleanupProducts(config, ids)
            storage.close()
        }
    }

    @Test
    fun `findProductsNameIsEmpty matches null and blank and respects order limit`() {
        val config = postgresConfigOrNull()
        assumeTrue(config != null, "Postgres test config is not available")

        val ids = listOf(uniqueProductId(), uniqueProductId(), uniqueProductId(), uniqueProductId())
        val t = Instant.parse("2026-01-01T00:00:00Z")
        val storage = PostgresqlStorage(config!!)

        try {
            storage.createProductIfNotExist(ids)
            storage.updateProduct(ids[0], sampleProduct(ids[0], name = null, remoteLastUpdateAt = t))
            storage.updateProduct(ids[1], sampleProduct(ids[1], name = "", remoteLastUpdateAt = t))
            storage.updateProduct(ids[2], sampleProduct(ids[2], name = "   ", remoteLastUpdateAt = t))
            storage.updateProduct(ids[3], sampleProduct(ids[3], name = "Orange", remoteLastUpdateAt = t))

            val rows = storage.findProductsNameIsEmpty(limit = 2)
            assertEquals(listOf(ids[0], ids[1]), rows.map { it.id })
        } finally {
            cleanupProducts(config, ids)
            storage.close()
        }
    }

    @Test
    fun `findProductsSince filters with inclusive boundary and newest first`() {
        val config = postgresConfigOrNull()
        assumeTrue(config != null, "Postgres test config is not available")

        val ids = listOf(uniqueProductId(), uniqueProductId(), uniqueProductId())
        val t1 = Instant.parse("2026-01-01T10:00:00Z")
        val t2 = Instant.parse("2026-01-02T10:00:00Z")
        val t3 = Instant.parse("2026-01-03T10:00:00Z")
        val storage = PostgresqlStorage(config!!)

        try {
            storage.createProductIfNotExist(ids)
            storage.updateProduct(ids[0], sampleProduct(ids[0], name = "A", remoteLastUpdateAt = t1))
            storage.updateProduct(ids[1], sampleProduct(ids[1], name = "B", remoteLastUpdateAt = t2))
            storage.updateProduct(ids[2], sampleProduct(ids[2], name = "C", remoteLastUpdateAt = t3))

            val rows = storage.findProductsSince(t2, limit = 10)
            assertEquals(listOf(ids[2], ids[1]), rows.map { it.id })
        } finally {
            cleanupProducts(config, ids)
            storage.close()
        }
    }

    @Test
    fun `updateProduct updates existing row`() {
        val config = postgresConfigOrNull()
        assumeTrue(config != null, "Postgres test config is not available")

        val productId = uniqueProductId()
        val storage = PostgresqlStorage(config!!)

        try {
            storage.createProductIfNotExist(listOf(productId))
            storage.updateProduct(
                productId,
                sampleProduct(
                    productId = productId,
                    name = "Updated Name",
                    remoteLastUpdateAt = Instant.parse("2026-02-01T00:00:00Z")
                )
            )

            val row = storage.findProductById(productId)
            assertEquals("Updated Name", row?.name)
            assertEquals(Instant.parse("2026-02-01T00:00:00Z"), row?.remoteLastUpdateAt)
        } finally {
            cleanupProducts(config, listOf(productId))
            storage.close()
        }
    }

    @Test
    fun `updateProduct throws when row missing`() {
        val config = postgresConfigOrNull()
        assumeTrue(config != null, "Postgres test config is not available")

        val missingProductId = uniqueProductId()
        val storage = PostgresqlStorage(config!!)

        try {
            val exception = assertFailsWith<StorageWriteException> {
                storage.updateProduct(
                    missingProductId,
                    sampleProduct(
                        productId = missingProductId,
                        name = "Missing",
                        remoteLastUpdateAt = Instant.parse("2026-02-02T00:00:00Z")
                    )
                )
            }
            assertTrue(exception.message?.contains("updateProduct") == true)
        } finally {
            cleanupProducts(config, listOf(missingProductId))
            storage.close()
        }
    }

    @Test
    fun `updateProduct throws when productId mismatches`() {
        val config = postgresConfigOrNull()
        assumeTrue(config != null, "Postgres test config is not available")

        val existingId = uniqueProductId()
        val storage = PostgresqlStorage(config!!)

        try {
            storage.createProductIfNotExist(listOf(existingId))
            assertFailsWith<IllegalArgumentException> {
                storage.updateProduct(
                    existingId,
                    sampleProduct(
                        productId = uniqueProductId(),
                        name = "Mismatch",
                        remoteLastUpdateAt = Instant.parse("2026-02-03T00:00:00Z")
                    )
                )
            }
        } finally {
            cleanupProducts(config, listOf(existingId))
            storage.close()
        }
    }

    @Test
    fun `department APIs insert and read by id`() {
        val config = postgresConfigOrNull()
        assumeTrue(config != null, "Postgres test config is not available")

        val baseId = (uniqueProductId() % 100000).toInt()
        val storage = PostgresqlStorage(config!!)
        try {
            val now = Instant.parse("2026-03-02T00:00:00Z")
            val d1 = DepartmentEntity(
                id = baseId,
                parentDepartmentId = null,
                name = "Dept A",
                path = "x/y",
                storeId = 7442,
                count = 4,
                canonicalUrl = "/department/$baseId",
                createdAt = now
            )
            val d2 = d1.copy(id = baseId + 1, name = "Dept B")
            storage.insertDepartmentEntity(listOf(d1, d1.copy(name = "Skip"), d2))

            assertEquals("Dept A", storage.findDepartmentById(baseId)?.name)
            assertTrue(storage.findAllDepartments().any { it.id == baseId })
        } finally {
            withPostgresConnection(config) { connection ->
                connection.prepareStatement("DELETE FROM departments WHERE id = ANY (?)").use { stmt ->
                    stmt.setArray(1, connection.createArrayOf("integer", arrayOf(baseId, baseId + 1)))
                    stmt.executeUpdate()
                }
            }
            storage.close()
        }
    }

    @Test
    fun `jobs APIs support dedupe and status updates`() {
        val config = postgresConfigOrNull()
        assumeTrue(config != null, "Postgres test config is not available")

        val syncId = (uniqueProductId() % 100000).toInt()
        val storage = PostgresqlStorage(config!!)
        try {
            val now = Instant.parse("2026-03-03T00:00:00Z")
            storage.insertJobsIfNotExist(
                listOf(
                    JobEntity(
                        syncId = syncId,
                        jobType = JobType.FETCH_PRODUCT,
                        status = JobStatus.PENDING,
                        dedupeKey = "product:100",
                        createdAt = now,
                        updatedAt = now
                    ),
                    JobEntity(
                        syncId = syncId,
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
                        syncId = syncId,
                        jobType = JobType.FETCH_PRODUCT,
                        status = JobStatus.PENDING,
                        dedupeKey = "product:100",
                        createdAt = now,
                        updatedAt = now
                    )
                )
            )

            val pending = storage.findJobsByType(syncId, JobType.FETCH_PRODUCT, JobStatus.PENDING)
            assertEquals(2, pending.size)
            assertTrue((pending[0].id ?: 0) < (pending[1].id ?: 0))

            val firstId = pending.first().id ?: error("missing id")
            storage.updateJobStatusById(firstId, JobStatus.IN_PROGRESS)
            assertEquals(1, storage.findJobsByType(syncId, JobType.FETCH_PRODUCT, JobStatus.IN_PROGRESS).size)

            val secondId = pending.last().id ?: error("missing id")
            storage.updateJobStatusByIds(listOf(firstId, secondId), JobStatus.SUCCESS)
            assertEquals(2, storage.findJobsByType(syncId, JobType.FETCH_PRODUCT, JobStatus.SUCCESS).size)
            assertEquals("dept:33", storage.findDepartmentJobsById(syncId, 33).firstOrNull()?.dedupeKey)
        } finally {
            withPostgresConnection(config) { connection ->
                connection.prepareStatement("DELETE FROM jobs WHERE sync_id = ?").use { stmt ->
                    stmt.setInt(1, syncId)
                    stmt.executeUpdate()
                }
            }
            storage.close()
        }
    }

    @Test
    fun `createSyncEntity returns defaults with generated id`() {
        val config = postgresConfigOrNull()
        assumeTrue(config != null, "Postgres test config is not available")

        val storage = PostgresqlStorage(config!!)
        val createdId = mutableListOf<Int>()
        try {
            val created = storage.createSyncEntity()
            val id = created.id ?: error("missing sync id")
            createdId.add(id)
            assertTrue(id > 0)
            assertEquals(0, created.attempts)
            assertEquals(SyncStatus.PENDING, created.status)
            assertNull(created.finishedAt)
        } finally {
            cleanupSyncs(config, createdId)
            storage.close()
        }
    }

    @Test
    fun `findSyncEntityLeft returns latest running by id`() {
        val config = postgresConfigOrNull()
        assumeTrue(config != null, "Postgres test config is not available")

        val storage = PostgresqlStorage(config!!)
        val createdIds = mutableListOf<Int>()
        try {
            val first = storage.createSyncEntity()
            val second = storage.createSyncEntity()
            val third = storage.createSyncEntity()
            createdIds.addAll(listOf(first.id!!, second.id!!, third.id!!))

            storage.updateSyncEntity(first.copy(status = SyncStatus.RUNNING, attempts = 1))
            storage.updateSyncEntity(second.copy(status = SyncStatus.DONE, attempts = 1))
            val updatedThird = storage.updateSyncEntity(third.copy(status = SyncStatus.RUNNING, attempts = 2))

            val left = storage.findSyncEntityLeft()
            assertEquals(updatedThird.id, left?.id)
            assertEquals(SyncStatus.RUNNING, left?.status)
        } finally {
            cleanupSyncs(config, createdIds)
            storage.close()
        }
    }

    @Test
    fun `updateSyncEntity updates status attempts and finishedAt`() {
        val config = postgresConfigOrNull()
        assumeTrue(config != null, "Postgres test config is not available")

        val storage = PostgresqlStorage(config!!)
        val createdIds = mutableListOf<Int>()
        try {
            val created = storage.createSyncEntity()
            createdIds.add(created.id ?: error("missing sync id"))
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
            cleanupSyncs(config, createdIds)
            storage.close()
        }
    }

    private fun withPostgresConnection(config: DatabaseConfig, block: (java.sql.Connection) -> Unit) {
        val jdbcUrl = "jdbc:postgresql://${config.host}:${config.port}/${config.database}"
        val props = Properties().apply {
            setProperty("user", config.user)
            setProperty("password", config.password)
        }
        DriverManager.getConnection(jdbcUrl, props).use(block)
    }

    private fun cleanupProducts(config: DatabaseConfig, productIds: List<Long>) {
        if (productIds.isEmpty()) return
        withPostgresConnection(config) { connection ->
            connection.prepareStatement(
                "DELETE FROM products WHERE id = ANY (?)"
            ).use { stmt ->
                stmt.setArray(1, connection.createArrayOf("bigint", productIds.toTypedArray()))
                stmt.executeUpdate()
            }
        }
    }

    private fun cleanupSyncs(config: DatabaseConfig, syncIds: List<Int>) {
        if (syncIds.isEmpty()) return
        withPostgresConnection(config) { connection ->
            connection.prepareStatement("DELETE FROM syncs WHERE id = ANY (?)").use { stmt ->
                stmt.setArray(1, connection.createArrayOf("integer", syncIds.toTypedArray()))
                stmt.executeUpdate()
            }
        }
    }

    private fun uniqueProductId(): Long {
        return java.lang.Math.abs(UUID.randomUUID().mostSignificantBits)
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

    private fun postgresConfigOrNull(): DatabaseConfig? {
        val host = envFirst("POSTGRES_TEST_HOST", "POSTGRES_HOST") ?: return null
        val port = envFirst("POSTGRES_TEST_PORT", "POSTGRES_PORT")?.toIntOrNull() ?: return null
        val database = envFirst("POSTGRES_TEST_DB", "POSTGRES_DB") ?: return null
        val user = envFirst("POSTGRES_TEST_USER", "POSTGRES_USER") ?: return null
        val password = envFirst("POSTGRES_TEST_PASSWORD", "POSTGRES_PASSWORD") ?: return null

        return DatabaseConfig(
            host = host,
            port = port,
            database = database,
            user = user,
            password = password
        )
    }

    private fun envFirst(vararg keys: String): String? {
        return keys
            .asSequence()
            .mapNotNull { System.getenv(it) }
            .firstOrNull { it.isNotBlank() }
    }
}
