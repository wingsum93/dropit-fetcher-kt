package com.ericho.dropit.model.entity

import java.time.Instant

data class DepartmentEntity(
    val id: Long? = null,
    val departmentId: Int,
    val parentDepartmentId : Int?,
    val name:String,
    val createdAt: Instant = Instant.now()
)
