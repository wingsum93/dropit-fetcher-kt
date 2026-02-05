package com.ericho.dropit.model.db

import java.sql.PreparedStatement
import java.sql.Timestamp
import java.time.Instant

interface DepartmentDialect {
    val createTableSql: String
    val upsertSql: String

    fun bindDepartmentId(statement: PreparedStatement, index: Int, departmentId: Int)
    fun bindCreatedAt(statement: PreparedStatement, index: Int, createdAt: Instant)
}

object PostgresDepartmentDialect : DepartmentDialect {
    override val createTableSql: String =
        """
        CREATE TABLE IF NOT EXISTS departments (
            id BIGSERIAL PRIMARY KEY,
            department_id INT NOT NULL UNIQUE,
            created_at TIMESTAMPTZ NOT NULL
        )
        """.trimIndent()

    override val upsertSql: String =
        """
        INSERT INTO departments (department_id, created_at)
        VALUES (?, ?)
        ON CONFLICT (department_id) DO UPDATE
        SET created_at = EXCLUDED.created_at
        """.trimIndent()

    override fun bindDepartmentId(statement: PreparedStatement, index: Int, departmentId: Int) {
        statement.setInt(index, departmentId)
    }

    override fun bindCreatedAt(statement: PreparedStatement, index: Int, createdAt: Instant) {
        statement.setTimestamp(index, Timestamp.from(createdAt))
    }
}

object SqliteDepartmentDialect : DepartmentDialect {
    override val createTableSql: String =
        """
        CREATE TABLE IF NOT EXISTS departments (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            department_id INTEGER NOT NULL UNIQUE,
            created_at TIMESTAMP NOT NULL
        )
        """.trimIndent()

    override val upsertSql: String =
        """
        INSERT INTO departments (department_id, created_at)
        VALUES (?, ?)
        ON CONFLICT(department_id) DO UPDATE
        SET created_at = excluded.created_at
        """.trimIndent()

    override fun bindDepartmentId(statement: PreparedStatement, index: Int, departmentId: Int) {
        statement.setInt(index, departmentId)
    }

    override fun bindCreatedAt(statement: PreparedStatement, index: Int, createdAt: Instant) {
        statement.setTimestamp(index, Timestamp.from(createdAt))
    }
}
