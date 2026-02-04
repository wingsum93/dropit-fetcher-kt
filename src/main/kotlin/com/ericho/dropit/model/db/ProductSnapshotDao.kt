package com.ericho.dropit.model.db

import com.ericho.dropit.model.api.SnapshotPayload
import java.sql.Connection
import java.sql.Timestamp
import java.time.Instant

class ProductSnapshotDao(private val dialect: SnapshotDialect) {
    fun insertSnapshots(connection: Connection, snapshots: List<SnapshotPayload>) {
        if (snapshots.isEmpty()) return

        connection.createStatement().use { statement ->
            statement.execute(dialect.createTableSql)
        }

        connection.prepareStatement(dialect.insertSql).use { statement ->
            val now = Timestamp.from(Instant.now())
            snapshots.forEach { snapshot ->
                statement.setString(1, snapshot.key)
                dialect.bindPayload(statement, 2, snapshot.json)
                statement.setTimestamp(3, now)
                statement.addBatch()
            }
            statement.executeBatch()
        }
    }
}

