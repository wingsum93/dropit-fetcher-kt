package com.ericho.dropit.model.db

import com.ericho.dropit.model.entity.ProductEntity
import org.junit.jupiter.api.Assertions.assertEquals
import org.junit.jupiter.api.Assertions.assertTrue
import org.junit.jupiter.api.Test
import java.sql.DriverManager
import java.time.Instant

class ProductDaoTest {
    @Test
    fun `upsertProducts inserts and updates by productId`() {
        DriverManager.getConnection("jdbc:sqlite::memory:").use { connection ->
            val dao = ProductDao(SqliteProductDialect)

            val first = ProductEntity(productId = 123, createdAt = Instant.parse("2025-01-01T00:00:00Z"))
            dao.upsertProducts(connection, listOf(first))

            val countAfterInsert = connection.prepareStatement(
                "SELECT COUNT(*) FROM products"
            ).use { stmt ->
                stmt.executeQuery().use { rs ->
                    rs.next()
                    rs.getInt(1)
                }
            }
            assertEquals(1, countAfterInsert)

            val second = ProductEntity(productId = 123, createdAt = Instant.parse("2025-02-01T00:00:00Z"))
            dao.upsertProducts(connection, listOf(second))

            val countAfterUpsert = connection.prepareStatement(
                "SELECT COUNT(*) FROM products"
            ).use { stmt ->
                stmt.executeQuery().use { rs ->
                    rs.next()
                    rs.getInt(1)
                }
            }
            assertEquals(1, countAfterUpsert)

            val createdAt = connection.prepareStatement(
                "SELECT created_at FROM products WHERE product_id = ?"
            ).use { stmt ->
                stmt.setLong(1, 123)
                stmt.executeQuery().use { rs ->
                    assertTrue(rs.next())
                    rs.getTimestamp(1).toInstant()
                }
            }
            assertEquals(Instant.parse("2025-02-01T00:00:00Z"), createdAt)
        }
    }
}
