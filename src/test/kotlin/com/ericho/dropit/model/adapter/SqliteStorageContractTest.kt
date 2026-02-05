package com.ericho.dropit.model.adapter

import com.ericho.dropit.model.SingleProductPayload
import kotlinx.serialization.json.Json
import org.junit.jupiter.api.Test
import java.nio.file.Files
import java.sql.DriverManager
import kotlin.test.assertEquals
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

    private fun sampleDetail(id: String, name: String): SingleProductPayload {
        return baseDetail().copy(
            id = id,
            name = name
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
