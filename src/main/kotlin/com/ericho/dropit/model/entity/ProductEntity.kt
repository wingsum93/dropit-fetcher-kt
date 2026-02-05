package com.ericho.dropit.model.entity

import java.time.Instant

// store in my db
data class ProductEntity(
    val id: Long,
    val storeId: Int? = null,
    val category: Int? = null,
    val departmentId: Int? = null,
    val unitPrice: Float? = null,
    val popularity: Int? = null,
    val upc: String? = null,
    val name: String? = null,
    val canonicalUrl: String? = null,
    val remoteLastUpdateAt: Instant? = null,
    val createdAt: Instant = Instant.now()
)
