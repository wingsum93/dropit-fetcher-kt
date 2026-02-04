package com.ericho.dropit.model.adapter

import com.ericho.dropit.model.DatabaseConfig
import com.ericho.dropit.model.SingleProductPayload
import com.ericho.dropit.model.api.SnapshotPayload
import kotlinx.serialization.encodeToString
import kotlinx.serialization.json.Json
import java.sql.DriverManager
import java.sql.Timestamp
import java.time.Instant
import java.util.Properties

class PostgresqlStorage(private val config: DatabaseConfig) : Storage {
    private val json = Json { encodeDefaults = true }

    override fun upsert(detail: SingleProductPayload) {
        val snapshot = SnapshotPayload(
            key = detail.id,
            json = json.encodeToString(detail)
        )
        writeSnapshots(listOf(snapshot))
    }

    private fun writeSnapshots(snapshots: List<SnapshotPayload>) {
        if (snapshots.isEmpty()) {
            return
        }

        val jdbcUrl = "jdbc:postgresql://${config.host}:${config.port}/${config.database}"
        val props = Properties().apply {
            setProperty("user", config.user)
            setProperty("password", config.password)
        }

        try {
            DriverManager.getConnection(jdbcUrl, props).use { connection ->
                connection.createStatement().use { statement ->
                    statement.execute(
                        """
                        CREATE TABLE IF NOT EXISTS product_snapshots (
                            id BIGSERIAL PRIMARY KEY,
                            snapshot_key TEXT NOT NULL,
                            payload JSONB NOT NULL,
                            created_at TIMESTAMPTZ NOT NULL DEFAULT NOW()
                        )
                        """.trimIndent()
                    )
                }

                val insertSql = """
                    INSERT INTO product_snapshots (snapshot_key, payload, created_at)
                    VALUES (?, ?::jsonb, ?)
                """.trimIndent()

                connection.prepareStatement(insertSql).use { statement ->
                    val now = Timestamp.from(Instant.now())
                    snapshots.forEach { snapshot ->
                        statement.setString(1, snapshot.key)
                        statement.setString(2, snapshot.json)
                        statement.setTimestamp(3, now)
                        statement.addBatch()
                    }
                    statement.executeBatch()
                }
            }
        } catch (exception: Exception) {
            println("Postgres unavailable; skipping database persistence. (${exception::class.simpleName})")
        }
    }
}
