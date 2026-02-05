package com.ericho.dropit.model.db

import com.ericho.dropit.model.entity.DepartmentEntity
import org.junit.jupiter.api.Assertions.assertEquals
import org.junit.jupiter.api.Assertions.assertTrue
import org.junit.jupiter.api.Test
import java.sql.DriverManager
import java.time.Instant

class DepartmentDaoTest {
    @Test
    fun `upsertDepartments inserts and updates by departmentId`() {
        DriverManager.getConnection("jdbc:sqlite::memory:").use { connection ->
            val dao = DepartmentDao(SqliteDepartmentDialect)

            val first = DepartmentEntity(departmentId = 101, createdAt = Instant.parse("2025-01-01T00:00:00Z"))
            dao.upsertDepartments(connection, listOf(first))

            val countAfterInsert = connection.prepareStatement(
                "SELECT COUNT(*) FROM departments"
            ).use { stmt ->
                stmt.executeQuery().use { rs ->
                    rs.next()
                    rs.getInt(1)
                }
            }
            assertEquals(1, countAfterInsert)

            val second = DepartmentEntity(departmentId = 101, createdAt = Instant.parse("2025-02-01T00:00:00Z"))
            dao.upsertDepartments(connection, listOf(second))

            val countAfterUpsert = connection.prepareStatement(
                "SELECT COUNT(*) FROM departments"
            ).use { stmt ->
                stmt.executeQuery().use { rs ->
                    rs.next()
                    rs.getInt(1)
                }
            }
            assertEquals(1, countAfterUpsert)

            val createdAt = connection.prepareStatement(
                "SELECT created_at FROM departments WHERE department_id = ?"
            ).use { stmt ->
                stmt.setInt(1, 101)
                stmt.executeQuery().use { rs ->
                    assertTrue(rs.next())
                    rs.getTimestamp(1).toInstant()
                }
            }
            assertEquals(Instant.parse("2025-02-01T00:00:00Z"), createdAt)
        }
    }
}
