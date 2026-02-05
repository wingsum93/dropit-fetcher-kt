package com.ericho.dropit.model.entity

import java.time.Instant

data class DepartmentEntity(
    val id: Int,
    val parentDepartmentId : Int?,
    val name:String,
    val path:String,
    val storeId: Int,
    val count:Int,
    val canonicalUrl: String,
    val createdAt: Instant = Instant.now()
)
