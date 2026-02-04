package com.ericho.dropit.model.adapter

import com.ericho.dropit.model.SingleProductPayload
import com.ericho.dropit.model.api.SnapshotPayload
import com.ericho.dropit.model.db.ProductSnapshotDao
import com.ericho.dropit.model.db.SqliteSnapshotDialect
import kotlinx.serialization.encodeToString
import kotlinx.serialization.json.Json
import java.sql.DriverManager

class SqliteStorage(private val dbPath: String) : Storage {
    private val json = Json { encodeDefaults = true }
    private val dao = ProductSnapshotDao(SqliteSnapshotDialect)

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
                dao.insertSnapshots(connection, snapshots)
            }
        } catch (exception: Exception) {
            println("SQLite unavailable; skipping database persistence. (${exception::class.simpleName})")
        }
    }
}
