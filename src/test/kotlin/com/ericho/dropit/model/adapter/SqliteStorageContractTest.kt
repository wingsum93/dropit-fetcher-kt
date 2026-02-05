package com.ericho.dropit.model.adapter

import com.ericho.dropit.model.SingleProductPayload
import com.ericho.dropit.model.entity.ProductEntity
import kotlinx.serialization.json.Json
import org.junit.jupiter.api.Test
import java.nio.file.Files
import java.sql.DriverManager
import java.time.Instant
import kotlin.test.assertEquals
import kotlin.test.assertFailsWith
import kotlin.test.assertNull
import kotlin.test.assertTrue

class SqliteStorageContractTest {
    private val json = Json {
        ignoreUnknownKeys = true
        isLenient = true
    }

    @Test
    fun `schema exists after startup`() {
        val dbPath = Files.createTempFile("dropit-storage-contract-", ".sqlite")
        val storage = SqliteStorage(dbPath.toString())

        try {
            DriverManager.getConnection("jdbc:sqlite:${dbPath}").use { connection ->
                connection.prepareStatement(
                    "SELECT name FROM sqlite_master WHERE type = 'table' AND name = 'product_snapshots'"
                ).use { stmt ->
                    stmt.executeQuery().use { rs ->
                        assertTrue(rs.next(), "Expected product_snapshots table to exist")
                    }
                }

                connection.prepareStatement(
                    "SELECT name FROM sqlite_master WHERE type = 'table' AND name = 'products'"
                ).use { stmt ->
                    stmt.executeQuery().use { rs ->
                        assertTrue(rs.next(), "Expected products table to exist")
                    }
                }
            }
        } finally {
            storage.close()
            Files.deleteIfExists(dbPath)
        }
    }

    @Test
    fun `upsertSnapshot is deterministic for repeated key`() {
        val dbPath = Files.createTempFile("dropit-storage-upsert-", ".sqlite")
        val storage = SqliteStorage(dbPath.toString())
        val snapshotKey = "item-1001"

        try {
            storage.upsertSnapshot(sampleDetail(id = snapshotKey, name = "Version 1"))
            storage.upsertSnapshot(sampleDetail(id = snapshotKey, name = "Version 2"))

            DriverManager.getConnection("jdbc:sqlite:${dbPath}").use { connection ->
                val rowCount = connection.prepareStatement(
                    "SELECT COUNT(*) FROM product_snapshots WHERE snapshot_key = ?"
                ).use { stmt ->
                    stmt.setString(1, snapshotKey)
                    stmt.executeQuery().use { rs ->
                        rs.next()
                        rs.getInt(1)
                    }
                }
                assertEquals(1, rowCount)

                val storedPayload = connection.prepareStatement(
                    "SELECT payload FROM product_snapshots WHERE snapshot_key = ?"
                ).use { stmt ->
                    stmt.setString(1, snapshotKey)
                    stmt.executeQuery().use { rs ->
                        rs.next()
                        rs.getString(1)
                    }
                }
                assertTrue(storedPayload.contains("Version 2"))
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
            assertEquals(listOf(1L, 2L), rows.map { it.productId })
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
            assertEquals(listOf(12L, 11L), rows.map { it.productId })
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
    fun `reconciles legacy products table on startup`() {
        val dbPath = Files.createTempFile("dropit-storage-legacy-", ".sqlite")

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
            }
        }

        val storage = SqliteStorage(dbPath.toString())
        try {
            storage.createProductIfNotExist(listOf(5000))
            storage.updateProduct(
                productId = 5000,
                product = sampleProduct(
                    productId = 5000,
                    name = "Legacy Migrated",
                    remoteLastUpdateAt = Instant.parse("2026-03-01T00:00:00Z")
                )
            )

            DriverManager.getConnection("jdbc:sqlite:${dbPath}").use { connection ->
                val columns = connection.prepareStatement(
                    "PRAGMA table_info(products)"
                ).use { stmt ->
                    stmt.executeQuery().use { rs ->
                        buildList {
                            while (rs.next()) {
                                add(rs.getString("name"))
                            }
                        }
                    }
                }

                assertTrue(columns.contains("name"))
                assertTrue(columns.contains("remote_last_update_at"))
                assertTrue(columns.contains("store_id"))
            }
        } finally {
            storage.close()
            Files.deleteIfExists(dbPath)
        }
    }

    private fun sampleDetail(id: String, name: String): SingleProductPayload {
        return baseDetail().copy(
            id = id,
            name = name
        )
    }

    private fun sampleProduct(
        productId: Long,
        name: String?,
        remoteLastUpdateAt: Instant
    ): ProductEntity {
        return ProductEntity(
            productId = productId,
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
}
