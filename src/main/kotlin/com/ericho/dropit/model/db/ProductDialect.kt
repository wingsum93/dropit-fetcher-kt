package com.ericho.dropit.model.db

import java.sql.PreparedStatement
import java.sql.Timestamp
import java.time.Instant

interface ProductDialect {
    val createTableSql: String
    val upsertSql: String

    fun bindProductId(statement: PreparedStatement, index: Int, productId: Long)
    fun bindCreatedAt(statement: PreparedStatement, index: Int, createdAt: Instant)
}

object PostgresProductDialect : ProductDialect {
    override val createTableSql: String =
        """
        CREATE TABLE IF NOT EXISTS products (
            id BIGSERIAL PRIMARY KEY,
            product_id BIGINT NOT NULL UNIQUE,
            created_at TIMESTAMPTZ NOT NULL
        )
        """.trimIndent()

    override val upsertSql: String =
        """
        INSERT INTO products (product_id, created_at)
        VALUES (?, ?)
        ON CONFLICT (product_id) DO UPDATE
        SET created_at = EXCLUDED.created_at
        """.trimIndent()

    override fun bindProductId(statement: PreparedStatement, index: Int, productId: Long) {
        statement.setLong(index, productId)
    }

    override fun bindCreatedAt(statement: PreparedStatement, index: Int, createdAt: Instant) {
        statement.setTimestamp(index, Timestamp.from(createdAt))
    }
}

object SqliteProductDialect : ProductDialect {
    override val createTableSql: String =
        """
        CREATE TABLE IF NOT EXISTS products (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            product_id INTEGER NOT NULL UNIQUE,
            created_at TIMESTAMP NOT NULL
        )
        """.trimIndent()

    override val upsertSql: String =
        """
        INSERT INTO products (product_id, created_at)
        VALUES (?, ?)
        ON CONFLICT(product_id) DO UPDATE
        SET created_at = excluded.created_at
        """.trimIndent()

    override fun bindProductId(statement: PreparedStatement, index: Int, productId: Long) {
        statement.setLong(index, productId)
    }

    override fun bindCreatedAt(statement: PreparedStatement, index: Int, createdAt: Instant) {
        statement.setTimestamp(index, Timestamp.from(createdAt))
    }
}
