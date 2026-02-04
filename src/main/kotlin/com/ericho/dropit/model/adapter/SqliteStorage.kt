package com.ericho.dropit.model.adapter

import com.ericho.dropit.model.SingleProductPayload
import com.ericho.dropit.model.api.SnapshotPayload
import kotlinx.serialization.encodeToString
import kotlinx.serialization.json.Json
import java.sql.DriverManager
import java.sql.Timestamp
import java.time.Instant

class SqliteStorage(private val dbPath: String) : Storage {
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

        val jdbcUrl = "jdbc:sqlite:$dbPath"

        try {
            DriverManager.getConnection(jdbcUrl).use { connection ->
                connection.createStatement().use { statement ->
                    statement.execute(
                        """
                        CREATE TABLE IF NOT EXISTS product_snapshots (
                            id INTEGER PRIMARY KEY AUTOINCREMENT,
                            snapshot_key TEXT NOT NULL,
                            payload TEXT NOT NULL,
                            created_at TIMESTAMP NOT NULL
                        )
                        """.trimIndent()
                    )
                }

                val insertSql = """
                    INSERT INTO product_snapshots (snapshot_key, payload, created_at)
                    VALUES (?, ?, ?)
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
            println("SQLite unavailable; skipping database persistence. (${exception::class.simpleName})")
        }
    }
}
