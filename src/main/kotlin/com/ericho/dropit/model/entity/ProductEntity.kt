package com.ericho.dropit.model.entity

import java.time.Instant

data class ProductEntity(
    val id: Long? = null,
    val productId: Long,
    val createdAt: Instant = Instant.now()
)
