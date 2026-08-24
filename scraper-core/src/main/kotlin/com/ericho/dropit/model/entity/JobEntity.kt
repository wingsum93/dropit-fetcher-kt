package com.ericho.dropit.model.entity

import java.time.Instant

data class JobEntity(
    val id: Int? = null,
    val syncId: Int,
    val jobType: JobType,
    val status: JobStatus,
    val dedupeKey: String,
    val createdAt: Instant = Instant.now(),
    val updatedAt: Instant = Instant.now()
)

enum class JobStatus {
    PENDING,
    IN_PROGRESS,
    SUCCESS,
    ERROR
}

enum class JobType {
    FETCH_DEPARTMENTS,
    FETCH_DEPARTMENT_PRODUCTS,
    FETCH_PRODUCT
}
