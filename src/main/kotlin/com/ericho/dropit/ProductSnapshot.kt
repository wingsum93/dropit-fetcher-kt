package com.ericho.dropit

import kotlinx.serialization.Serializable

@Serializable
class ProductSnapshot(
    val productId: String,
    val name: String,
    val price: Double,
    val currency: String,
    val capturedAt: String // keep string first; later parse to Instant if you want
)
