package com.ericho.dropit.model.adapter

import com.ericho.dropit.model.DatabaseConfig
import com.ericho.dropit.model.SingleProductPayload
import com.ericho.dropit.model.api.SnapshotPayload
import com.ericho.dropit.model.db.PostgresSnapshotDialect
import com.ericho.dropit.model.db.ProductSnapshotDao
import com.ericho.dropit.model.entity.ProductEntity
import kotlinx.serialization.encodeToString
import kotlinx.serialization.json.Json
import java.sql.DriverManager
import java.util.Properties

class PostgresqlStorage(private val config: DatabaseConfig) : Storage {
    private val json = Json { encodeDefaults = true }
    private val dao = ProductSnapshotDao(PostgresSnapshotDialect)

    override fun upsert(detail: SingleProductPayload) {
        val snapshot = SnapshotPayload(
            key = detail.id,
            json = json.encodeToString(detail)
        )
        writeSnapshots(listOf(snapshot))
    }

    override fun upsert(product: ProductEntity) {
        // no-op for now
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
                dao.insertSnapshots(connection, snapshots)
            }
        } catch (exception: Exception) {
            println("Postgres unavailable; skipping database persistence. (${exception::class.simpleName})")
        }
    }
}
