package com.ericho.dropit.model.db

import com.ericho.dropit.model.entity.ProductEntity
import java.sql.Connection

class ProductDao(private val dialect: ProductDialect) {
    fun upsertProducts(connection: Connection, products: List<ProductEntity>) {
        if (products.isEmpty()) return

        connection.createStatement().use { statement ->
            statement.execute(dialect.createTableSql)
        }

        connection.prepareStatement(dialect.upsertSql).use { statement ->
            products.forEach { product ->
                dialect.bindProductId(statement, 1, product.productId)
                dialect.bindCreatedAt(statement, 2, product.createdAt)
                statement.addBatch()
            }
            statement.executeBatch()
        }
    }
}
