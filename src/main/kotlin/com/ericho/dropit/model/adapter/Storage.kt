package com.ericho.dropit.model.adapter
import com.ericho.dropit.model.SingleProductPayload
import com.ericho.dropit.model.entity.ProductEntity
import java.time.Instant

interface Storage : AutoCloseable {
    fun upsertSnapshot(detail: SingleProductPayload)
    fun findProductById(productId: Long): ProductEntity?
    fun findProductsNameIsEmpty(limit: Int): List<ProductEntity>
    fun findProductsSince(instant: Instant, limit: Int = 10): List<ProductEntity>
    fun updateProduct(productId: Long, product: ProductEntity)
    fun createProductIfNotExist(list: List<Long>)

    override fun close() {
        // default no-op
    }
}
