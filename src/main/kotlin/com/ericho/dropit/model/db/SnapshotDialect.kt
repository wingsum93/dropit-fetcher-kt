package com.ericho.dropit.model.db

import java.sql.PreparedStatement

interface SnapshotDialect {
    val createTableSql: String
    val insertSql: String

    fun bindPayload(statement: PreparedStatement, index: Int, json: String)
}

object PostgresSnapshotDialect : SnapshotDialect {
    override val createTableSql: String =
        """
        CREATE TABLE IF NOT EXISTS product_snapshots (
            id BIGSERIAL PRIMARY KEY,
            snapshot_key TEXT NOT NULL,
            payload JSONB NOT NULL,
            created_at TIMESTAMPTZ NOT NULL DEFAULT NOW()
        )
        """.trimIndent()

    override val insertSql: String =
        """
        INSERT INTO product_snapshots (snapshot_key, payload, created_at)
        VALUES (?, ?::jsonb, ?)
        """.trimIndent()

    override fun bindPayload(statement: PreparedStatement, index: Int, json: String) {
        statement.setString(index, json)
    }
}

object SqliteSnapshotDialect : SnapshotDialect {
    override val createTableSql: String =
        """
        CREATE TABLE IF NOT EXISTS product_snapshots (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            snapshot_key TEXT NOT NULL,
            payload TEXT NOT NULL,
            created_at TIMESTAMP NOT NULL
        )
        """.trimIndent()

    override val insertSql: String =
        """
        INSERT INTO product_snapshots (snapshot_key, payload, created_at)
        VALUES (?, ?, ?)
        """.trimIndent()

    override fun bindPayload(statement: PreparedStatement, index: Int, json: String) {
        statement.setString(index, json)
    }
}

