package com.ericho.dropit.model.adapter

import com.ericho.dropit.model.SingleProductPayload
import com.ericho.dropit.model.entity.ProductEntity
import java.time.Instant

class FakeStorage : Storage {
    private val productsById: MutableMap<Long, ProductEntity> = mutableMapOf()

    override fun upsertSnapshot(detail: SingleProductPayload) {
        //
    }

    override fun findProductById(productId: Long): ProductEntity? {
        return productsById[productId]
    }

    override fun findProductsNameIsEmpty(limit: Int): List<ProductEntity> {
        require(limit > 0) { "limit must be > 0" }
        return productsById.values
            .asSequence()
            .filter { it.name.isNullOrBlank() }
            .sortedBy { it.productId }
            .take(limit)
            .toList()
    }

    override fun findProductsSince(instant: Instant, limit: Int): List<ProductEntity> {
        require(limit > 0) { "limit must be > 0" }
        return productsById.values
            .asSequence()
            .filter { it.remoteLastUpdateAt != null && !it.remoteLastUpdateAt.isBefore(instant) }
            .sortedWith(compareByDescending<ProductEntity> { it.remoteLastUpdateAt }.thenByDescending { it.productId })
            .take(limit)
            .toList()
    }

    override fun updateProduct(productId: Long, product: ProductEntity) {
        require(product.productId == productId) {
            "productId mismatch: arg=$productId payload=${product.productId}"
        }
        if (!productsById.containsKey(productId)) {
            throw StorageWriteException(
                backend = "fake",
                operation = "updateProduct",
                key = productId.toString(),
                cause = IllegalStateException("Product not found: $productId")
            )
        }
        productsById[productId] = product
    }

    override fun createProductIfNotExist(list: List<Long>) {
        if (list.isEmpty()) return
        list.distinct().forEach { productId ->
            productsById.putIfAbsent(
                productId,
                ProductEntity(productId = productId)
            )
        }
    }
}
