package com.ericho.dropit.model.api

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


)
