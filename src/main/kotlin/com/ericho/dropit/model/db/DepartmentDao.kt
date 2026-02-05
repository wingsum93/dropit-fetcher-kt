package com.ericho.dropit.model.db

import com.ericho.dropit.model.entity.DepartmentEntity
import java.sql.Connection

class DepartmentDao(private val dialect: DepartmentDialect) {
    fun upsertDepartments(connection: Connection, departments: List<DepartmentEntity>) {
        if (departments.isEmpty()) return

        connection.createStatement().use { statement ->
            statement.execute(dialect.createTableSql)
        }

        connection.prepareStatement(dialect.upsertSql).use { statement ->
            departments.forEach { department ->
                dialect.bindDepartmentId(statement, 1, department.departmentId)
                dialect.bindCreatedAt(statement, 2, department.createdAt)
                statement.addBatch()
            }
            statement.executeBatch()
        }
    }
}
