package com.ericho.dropit.model

import kotlinx.serialization.Serializable

@Serializable
data class ProductPayload(
    val total: Int,
    val items: List<ProductDto>,
    val departments: List<DepartmentDto>
)

@Serializable
data class ProductDto(
    val id: String,
    val count: Int,

)
