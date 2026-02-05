package com.ericho.dropit.model.adapter

import com.ericho.dropit.model.DatabaseConfig
import com.ericho.dropit.model.SingleProductPayload
import kotlinx.serialization.json.Json
import org.junit.jupiter.api.Assumptions.assumeTrue
import org.junit.jupiter.api.Test
import java.sql.DriverManager
import java.util.Properties
import java.util.UUID
import kotlin.test.assertEquals
import kotlin.test.assertTrue

class PostgresqlStorageContractTest {
    private val json = Json {
        ignoreUnknownKeys = true
        isLenient = true
    }

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
                          AND table_name = 'product_snapshots'
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
    fun `upsertSnapshot is deterministic for repeated key`() {
        val config = postgresConfigOrNull()
        assumeTrue(config != null, "Postgres test config is not available")

        val storage = PostgresqlStorage(config!!)
        val snapshotKey = "item-${UUID.randomUUID()}"

        try {
            storage.upsertSnapshot(sampleDetail(id = snapshotKey, name = "Version 1"))
            storage.upsertSnapshot(sampleDetail(id = snapshotKey, name = "Version 2"))

            withPostgresConnection(config) { connection ->
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

                val payload = connection.prepareStatement(
                    "SELECT payload::text FROM product_snapshots WHERE snapshot_key = ?"
                ).use { stmt ->
                    stmt.setString(1, snapshotKey)
                    stmt.executeQuery().use { rs ->
                        rs.next()
                        rs.getString(1)
                    }
                }
                assertTrue(payload.contains("Version 2"))
            }
        } finally {
            withPostgresConnection(config) { connection ->
                connection.prepareStatement(
                    "DELETE FROM product_snapshots WHERE snapshot_key = ?"
                ).use { stmt ->
                    stmt.setString(1, snapshotKey)
                    stmt.executeUpdate()
                }
            }
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
