package com.ericho.dropit.model

import kotlinx.serialization.SerialName
import kotlinx.serialization.Serializable

@Serializable
data class DepartmentPayload(
    val total: Int,
    val departments: List<DepartmentDto>
)

@Serializable
data class DepartmentDto(
    val id: String,
    val count: Int,
    val sequence: Int,
    val name: String,
    @SerialName("parent_id")
    val parentId: String? = null,
    val identifier: String,
    @SerialName("_sequence")
    val internalSequence: Int? = null,
    @SerialName("store_id")
    val storeId: String,
    @SerialName("store_depth")
    val storeDepth: Int,
    @SerialName("type_id")
    val typeId: String,
    val path: String,
    val lineage: List<String>,
    @SerialName("canonical_url")
    val canonicalUrl: String,
    @SerialName("master_taxonomy")
    val masterTaxonomy: String? = null,
    @SerialName("is_red_department")
    val isRedDepartment: Boolean? = null
)
